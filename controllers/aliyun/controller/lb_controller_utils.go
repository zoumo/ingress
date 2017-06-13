package controller

import (
	"fmt"
	"strconv"
	"time"

	"github.com/golang/glog"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/pkg/api"
	"k8s.io/client-go/pkg/api/v1"
	"k8s.io/client-go/pkg/apis/extensions/v1beta1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

const (
	// allowHTTPKey tells the Ingress controller to allow/block HTTP access.
	// If either unset or set to true, the controller will create a
	// forwarding-rule for port 80, and any additional rules based on the TLS
	// section of the Ingress. If set to false, the controller will only create
	// rules for port 443 based on the TLS section.
	allowHTTPKey = "kubernetes.io/ingress.allow-http"

	// staticIPNameKey tells the Ingress controller to use a specific Aliyun
	// static ip for its forwarding rules. If specified, the Ingress controller
	// assigns the static ip by this name to the forwarding rules of the given
	// Ingress. The controller *does not* manage this ip, it is the users
	// responsibility to create/delete it.
	staticIPNameKey = "kubernetes.io/ingress.global-static-ip-name"

	// ingressClassKey picks a specific "class" for the Ingress. The controller
	// only processes Ingresses with this annotation either unset, or set
	// to either aliyunIngessClass or the empty string.
	ingressClassKey    = "kubernetes.io/ingress.class"
	aliyunIngressClass = "aliyun"

	// Label key to denote which Aliyun zone a Kubernetes node is in.
	zoneKey     = "failure-domain.beta.kubernetes.io/zone"
	defaultZone = ""
)

const (
	// ingressLoadbalancerAnno = "ingress.alpha.k8s.io/loadbalancer-name"
	ingressLoadbalancerAnno = "kubernetes.io/ingress.class"
)

var (
	keyFunc = cache.DeletionHandlingMetaNamespaceKeyFunc

	// DefaultClusterUID is the uid to use for clusters resources created by an
	// L7 controller created without specifying the --cluster-uid flag.
	DefaultClusterUID = ""

	// Frequency to poll on local stores to sync.
	storeSyncPollPeriod = 5 * time.Second
)

const (
	NamespaceIndex string = "namespace"
)

type StoreToIngressLister struct {
	cache.Store
}

// AppendFunc is used to add a matching item to whatever list the caller is using
type AppendFunc func(interface{})

func ListAll(store cache.Store, selector labels.Selector, appendFn AppendFunc) error {
	for _, m := range store.List() {
		metadata, err := meta.Accessor(m)
		if err != nil {
			return err
		}
		if selector.Matches(labels.Set(metadata.GetLabels())) {
			appendFn(m)
		}
	}
	return nil
}

func ListAllByNamespace(indexer cache.Indexer, namespace string, selector labels.Selector, appendFn AppendFunc) error {
	if namespace == v1.NamespaceAll {
		for _, m := range indexer.List() {
			metadata, err := meta.Accessor(m)
			if err != nil {
				return err
			}
			if selector.Matches(labels.Set(metadata.GetLabels())) {
				appendFn(m)
			}
		}
		return nil
	}

	items, err := indexer.Index(NamespaceIndex, &v1.ObjectMeta{Namespace: namespace})
	if err != nil {
		// Ignore error; do slow search without index.
		glog.Warningf("can not retrieve list of objects using index : %v", err)
		for _, m := range indexer.List() {
			metadata, err := meta.Accessor(m)
			if err != nil {
				return err
			}
			if metadata.GetNamespace() == namespace && selector.Matches(labels.Set(metadata.GetLabels())) {
				appendFn(m)
			}

		}
		return nil
	}
	for _, m := range items {
		metadata, err := meta.Accessor(m)
		if err != nil {
			return err
		}
		if selector.Matches(labels.Set(metadata.GetLabels())) {
			appendFn(m)
		}
	}

	return nil
}

// List lists all Ingress' in the store.
func (s *StoreToIngressLister) List() (ing v1beta1.IngressList, err error) {
	for _, m := range s.Store.List() {
		newIng := m.(*v1beta1.Ingress)
		if isAliyunIngress(newIng) {
			ing.Items = append(ing.Items, *newIng)
		}
	}
	return ing, nil
}

// GetServiceIngress gets all the Ingress' that have rules pointing to a service.
// Note that this ignores services without the right nodePorts.
func (s *StoreToIngressLister) GetServiceIngress(svc *v1.Service) (ings []v1beta1.Ingress, err error) {
	for _, m := range s.Store.List() {
		ing := *m.(*v1beta1.Ingress)
		if ing.Namespace != svc.Namespace {
			continue
		}
		for _, rules := range ing.Spec.Rules {
			if rules.IngressRuleValue.HTTP == nil {
				continue
			}
			for _, p := range rules.IngressRuleValue.HTTP.Paths {
				if p.Backend.ServiceName == svc.Name {
					ings = append(ings, ing)
				}
			}
		}
	}
	if len(ings) == 0 {
		err = fmt.Errorf("No ingress for service %v", svc.Name)
	}
	return
}

// NodeConditionPredicate is a function that indicates whether the given node's conditions meet
// some set of criteria defined by the function.
type NodeConditionPredicate func(node *v1.Node) bool

// StoreToNodeLister makes a Store have the List method of the client.NodeInterface
// The Store must contain (only) Nodes.
type StoreToNodeLister struct {
	cache.Store
}

func (s *StoreToNodeLister) List() (machines v1.NodeList, err error) {
	for _, m := range s.Store.List() {
		machines.Items = append(machines.Items, *(m.(*v1.Node)))
	}
	return machines, nil
}

// NodeCondition returns a storeToNodeConditionLister
func (s *StoreToNodeLister) NodeCondition(predicate NodeConditionPredicate) storeToNodeConditionLister {
	// TODO: Move this filtering server side. Currently our selectors don't facilitate searching through a list so we
	// have the reflector filter out the Unschedulable field and sift through node conditions in the lister.
	return storeToNodeConditionLister{s.Store, predicate}
}

// storeToNodeConditionLister filters and returns nodes matching the given type and status from the store.
type storeToNodeConditionLister struct {
	store     cache.Store
	predicate NodeConditionPredicate
}

// List returns a list of nodes that match the conditions defined by the predicate functions in the storeToNodeConditionLister.
func (s storeToNodeConditionLister) List() (nodes []*v1.Node, err error) {
	for _, m := range s.store.List() {
		node := m.(*v1.Node)
		if s.predicate(node) {
			nodes = append(nodes, node)
		} else {
			glog.V(5).Infof("Node %s matches none of the conditions", node.Name)
		}
	}
	return
}

// StoreToServiceLister helps list services
type StoreToServiceLister struct {
	Indexer cache.Indexer
}

func (s *StoreToServiceLister) List(selector labels.Selector) (ret []*v1.Service, err error) {
	err = ListAll(s.Indexer, selector, func(m interface{}) {
		ret = append(ret, m.(*v1.Service))
	})
	return ret, err
}

func (s *StoreToServiceLister) Services(namespace string) storeServicesNamespacer {
	return storeServicesNamespacer{s.Indexer, namespace}
}

type storeServicesNamespacer struct {
	indexer   cache.Indexer
	namespace string
}

func (s storeServicesNamespacer) List(selector labels.Selector) (ret []*v1.Service, err error) {
	err = ListAllByNamespace(s.indexer, s.namespace, selector, func(m interface{}) {
		ret = append(ret, m.(*v1.Service))
	})
	return ret, err
}

func (s storeServicesNamespacer) Get(name string) (*v1.Service, error) {
	obj, exists, err := s.indexer.GetByKey(s.namespace + "/" + name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.NewNotFound(api.Resource("service"), name)
	}
	return obj.(*v1.Service), nil
}

// TODO: Move this back to scheduler as a helper function that takes a Store,
// rather than a method of StoreToServiceLister.
func (s *StoreToServiceLister) GetPodServices(pod *v1.Pod) (services []*v1.Service, err error) {
	allServices, err := s.Services(pod.Namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}

	for i := range allServices {
		service := allServices[i]
		if service.Spec.Selector == nil {
			// services with nil selectors match nothing, not everything.
			continue
		}
		selector := labels.Set(service.Spec.Selector).AsSelectorPreValidated()
		if selector.Matches(labels.Set(pod.Labels)) {
			services = append(services, service)
		}
	}

	return services, nil
}

// StoreToPodLister helps list pods
type StoreToPodLister struct {
	Indexer cache.Indexer
}

func (s *StoreToPodLister) List(selector labels.Selector) (ret []*v1.Pod, err error) {
	err = ListAll(s.Indexer, selector, func(m interface{}) {
		ret = append(ret, m.(*v1.Pod))
	})
	return ret, err
}

func (s *StoreToPodLister) Pods(namespace string) storePodsNamespacer {
	return storePodsNamespacer{Indexer: s.Indexer, namespace: namespace}
}

type storePodsNamespacer struct {
	Indexer   cache.Indexer
	namespace string
}

func (s storePodsNamespacer) List(selector labels.Selector) (ret []*v1.Pod, err error) {
	err = ListAllByNamespace(s.Indexer, s.namespace, selector, func(m interface{}) {
		ret = append(ret, m.(*v1.Pod))
	})
	return ret, err
}

func (s storePodsNamespacer) Get(name string) (*v1.Pod, error) {
	obj, exists, err := s.Indexer.GetByKey(s.namespace + "/" + name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.NewNotFound(api.Resource("pod"), name)
	}
	return obj.(*v1.Pod), nil
}

// ingAnnotations represents Ingress annotations.
type ingAnnotations map[string]string

// allowHTTP returns the allowHTTP flag. True by default.
func (ing ingAnnotations) allowHTTP() bool {
	val, ok := ing[allowHTTPKey]
	if !ok {
		return true
	}
	v, err := strconv.ParseBool(val)
	if err != nil {
		return true
	}
	return v
}

func (ing ingAnnotations) staticIPName() string {
	val, ok := ing[staticIPNameKey]
	if !ok {
		return ""
	}
	return val
}

func (ing ingAnnotations) ingressClass() string {
	val, ok := ing[ingressClassKey]
	if !ok {
		return ""
	}
	return val
}

// isAliyunIngress returns true if the given Ingress either doesn't specify the
// ingress.class annotation, or it's set to "aliyun".
func isAliyunIngress(ing *v1beta1.Ingress) bool {
	return true
	// class := ingAnnotations(ing.ObjectMeta.Annotations).ingressClass()
	// return class == "" || class == aliyunIngressClass
}

func isAssignedToSelf(ing *v1beta1.Ingress, loadBalancerName string) bool {
	if ing.Annotations == nil {
		return false
	}
	return ing.Annotations[ingressLoadbalancerAnno] == loadBalancerName
}

// taskQueue manages a work queue through an independent worker that
// invokes the given sync function for every work item inserted.
type taskQueue struct {
	// queue is the work queue the worker polls
	queue workqueue.RateLimitingInterface
	// sync is called for each item in the queue
	sync func(string) error
	// workerDone is closed when the worker exits
	workerDone chan struct{}
}

func (t *taskQueue) run(period time.Duration, stopCh <-chan struct{}) {
	wait.Until(t.worker, period, stopCh)
}

// enqueue enqueues ns/name of the given api object in the task queue.
func (t *taskQueue) enqueue(obj interface{}) {
	key, err := keyFunc(obj)
	if err != nil {
		glog.Infof("Couldn't get key for object %+v: %v", obj, err)
		return
	}
	t.queue.Add(key)
}

// worker processes work in the queue through sync.
func (t *taskQueue) worker() {
	for {
		key, quit := t.queue.Get()
		if quit {
			close(t.workerDone)
			return
		}
		glog.V(3).Infof("Syncing %v", key)
		if err := t.sync(key.(string)); err != nil {
			glog.Errorf("Requeuing %v, err %v", key, err)
			t.queue.AddRateLimited(key)
		} else {
			t.queue.Forget(key)
		}
		t.queue.Done(key)
	}
}

// shutdown shuts down the work queue and waits for the worker to ACK
func (t *taskQueue) shutdown() {
	t.queue.ShutDown()
	<-t.workerDone
}

// NewTaskQueue creates a new task queue with the given sync function.
// The sync function is called for every element inserted into the queue.
func NewTaskQueue(syncFn func(string) error) *taskQueue {
	return &taskQueue{
		queue:      workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()),
		sync:       syncFn,
		workerDone: make(chan struct{}),
	}
}
