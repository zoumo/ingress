package controller

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/denverdino/aliyungo/common"
	"github.com/denverdino/aliyungo/slb"
	"github.com/golang/glog"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/pkg/api"
	"k8s.io/client-go/pkg/api/v1"
	"k8s.io/client-go/pkg/apis/extensions/v1beta1"
	"k8s.io/client-go/tools/cache"

	"k8s.io/ingress/controllers/aliyun/client"
)

const (
	// port 0 is used as a signal for port not found/no such port etc.
	invalidPort                = 0
	defaultBackendServerWeight = 100
)

const (
	updateRetryNum      = 5
	updateRetryInterval = 500 * time.Millisecond
)

type LoadBalancerController struct {
	aliyunClient *client.AliyunClient
	clientset    *kubernetes.Clientset

	loadBalancerName string
	loadBalancerID   string
	loadBalancerIP   string

	ingressLister StoreToIngressLister
	nodeLister    StoreToNodeLister
	svcLister     StoreToServiceLister
	podLister     StoreToPodLister

	ingressController cache.Controller
	nodeController    cache.Controller
	svcController     cache.Controller
	//podController     cache.Controller

	ingressQueue *taskQueue
	nodeQueue    *taskQueue
}

func NewLoadBalancerController(aliyunClient *client.AliyunClient, clientset *kubernetes.Clientset, loadBalancerName, loadBalancerID, loadBalancerIP string) *LoadBalancerController {
	lbc := &LoadBalancerController{
		aliyunClient:     aliyunClient,
		clientset:        clientset,
		loadBalancerName: loadBalancerName,
		loadBalancerID:   loadBalancerID,
		loadBalancerIP:   loadBalancerIP,
	}

	lbc.nodeQueue = NewTaskQueue(lbc.syncNodes)
	lbc.ingressQueue = NewTaskQueue(lbc.sync)

	lbc.ingressLister.Store, lbc.ingressController = cache.NewInformer(
		&cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				return clientset.Ingresses(api.NamespaceAll).List(options)
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				return clientset.Ingresses(api.NamespaceAll).Watch(options)
			},
		},
		&v1beta1.Ingress{},
		0,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				addIng := obj.(*v1beta1.Ingress)
				if !isAliyunIngress(addIng) {
					glog.Infof("Ignoring add for ingress %v based on annotation %v", addIng.Name, ingressClassKey)
					return
				}
				if !isAssignedToSelf(addIng, loadBalancerName) {
					glog.Infof("Ignoring add for ingress %v based on annotation %v", addIng.Name, ingressLoadbalancerAnno)
					return
				}
				lbc.ingressQueue.enqueue(obj)
			},
			DeleteFunc: func(obj interface{}) {
				delIng := obj.(*v1beta1.Ingress)
				if !isAliyunIngress(delIng) {
					glog.Infof("Ignoring delete for ingress %v based on annotation %v", delIng.Name, ingressClassKey)
					return
				}
				if !isAssignedToSelf(delIng, loadBalancerName) {
					glog.Infof("Ignoring add for ingress %v based on annotation %v", delIng.Name, ingressLoadbalancerAnno)
					return
				}
				glog.Infof("Delete notification received for Ingress %v/%v", delIng.Namespace, delIng.Name)
				lbc.ingressQueue.enqueue(obj)
			},
			UpdateFunc: func(old, cur interface{}) {
				curIng := cur.(*v1beta1.Ingress)
				if !isAliyunIngress(curIng) {
					return
				}
				if !isAssignedToSelf(curIng, loadBalancerName) {
					glog.Infof("Ignoring add for ingress %v based on annotation %v", curIng.Name, ingressLoadbalancerAnno)
					return
				}
				if !reflect.DeepEqual(old, cur) {
					glog.V(3).Infof("Ingress %v changed, syncing", curIng.Name)
				}
				lbc.ingressQueue.enqueue(cur)
			},
		},
	)
	lbc.nodeLister.Store, lbc.nodeController = cache.NewInformer(
		&cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				return clientset.Nodes().List(options)
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				return clientset.Nodes().Watch(options)
			},
		},
		&v1.Node{},
		0,
		cache.ResourceEventHandlerFuncs{
			AddFunc:    lbc.nodeQueue.enqueue,
			DeleteFunc: lbc.nodeQueue.enqueue,
			// Nodes are updated every 10s and we don't care, so no update handler.
		},
	)
	lbc.svcLister.Indexer, lbc.svcController = cache.NewIndexerInformer(
		&cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				return clientset.Services(api.NamespaceAll).List(options)
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				return clientset.Services(api.NamespaceAll).Watch(options)
			},
		},
		&v1.Service{},
		0,
		cache.ResourceEventHandlerFuncs{
			AddFunc: lbc.enqueueIngressForService,
			UpdateFunc: func(old, cur interface{}) {
				if !reflect.DeepEqual(old, cur) {
					lbc.enqueueIngressForService(cur)
				}
			},
			// Ingress deletes matter, service deletes don't.
		},
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
	)

	return lbc
}

func (lbc *LoadBalancerController) Run(stopCh <-chan struct{}) {
	glog.Infof("Starting LoadBalancer Controller")
	go lbc.ingressController.Run(stopCh)
	go lbc.nodeController.Run(stopCh)
	//go lbc.podController.Run(stopCh)
	go lbc.svcController.Run(stopCh)

	// wait until all the sub-controllers have finished their first sync with apiserver. before starting working goroute
	if !cache.WaitForCacheSync(stopCh,
		//lbc.podController.HasSynced,
		lbc.svcController.HasSynced,
		lbc.nodeController.HasSynced,
		lbc.ingressController.HasSynced) {
		return
	}

	go lbc.ingressQueue.run(time.Second, stopCh)
	go lbc.nodeQueue.run(time.Second, stopCh)
	<-stopCh
	glog.Infof("Shutting down Loadbalancer Controller")
}

// enqueueIngressForService enqueues all the Ingress' for a Service.
func (lbc *LoadBalancerController) enqueueIngressForService(obj interface{}) {
	svc := obj.(*v1.Service)
	ings, err := lbc.ingressLister.GetServiceIngress(svc)
	if err != nil {
		glog.V(5).Infof("ignoring service %v: %v", svc.Name, err)
		return
	}
	for _, ing := range ings {
		if !isAliyunIngress(&ing) {
			continue
		}
		lbc.ingressQueue.enqueue(&ing)
	}
}

// sync manages Ingress create/updates/deletes.
func (lbc *LoadBalancerController) sync(key string) (err error) {
	glog.Infof("sync %v", key)

	ingressList, err := lbc.ingressLister.List()
	if err != nil {
		return err
	}

	vServerGroupsMap := map[string]string{}
	actualVServerGroups := sets.NewString()
	desireVServerGroups := sets.NewString()

	{ // get actualVServerGroups
		vserverGroupsResp, err := lbc.aliyunClient.SlbClient.DescribeVServerGroups(&slb.DescribeVServerGroupsArgs{
			LoadBalancerId: lbc.loadBalancerID,
			RegionId:       common.Region(lbc.aliyunClient.RegionID),
		})
		if err != nil {
			glog.Error(err)
			return err
		}
		for _, group := range vserverGroupsResp.VServerGroups.VServerGroup {
			vServerGroupsMap[group.VServerGroupName] = group.VServerGroupId
			actualVServerGroups.Insert(group.VServerGroupName)
		}
		glog.Infof("actural vserver groups: %v", actualVServerGroups)
	}
	{ // get desireVServerGroups
		for _, ingress := range ingressList.Items {
			for _, rule := range ingress.Spec.Rules {
				if rule.IngressRuleValue.HTTP == nil {
					continue
				}
				for _, p := range rule.IngressRuleValue.HTTP.Paths {
					nodePort, err := lbc.getServiceNodePort(p.Backend, ingress.Namespace)
					if err != nil {
						glog.Error(err)
						continue
					}
					desireVServerGroups.Insert(getVServerGroupName(nodePort))
				}
			}
		}
		glog.Infof("desire vserver groups: %v", desireVServerGroups)
	}
	{ // ensure VServerGroups
		vServerGroupsToDelete := actualVServerGroups.Difference(desireVServerGroups)
		vServerGroupsToCreate := desireVServerGroups.Difference(actualVServerGroups)
		for _, name := range vServerGroupsToDelete.List() {
			if _, err := lbc.aliyunClient.SlbClient.DeleteVServerGroup(&slb.DeleteVServerGroupArgs{
				VServerGroupId: vServerGroupsMap[name],
				RegionId:       common.Region(lbc.aliyunClient.RegionID),
			}); err != nil {
				glog.Error(err)
				return err
			}
		}
		for _, name := range vServerGroupsToCreate.List() {
			resp, err := lbc.aliyunClient.SlbClient.CreateVServerGroup(&slb.CreateVServerGroupArgs{
				RegionId:         common.Region(lbc.aliyunClient.RegionID),
				LoadBalancerId:   lbc.loadBalancerID,
				VServerGroupName: name,
				BackendServers:   "", //TODO: check this is OK
			})
			if err != nil {
				glog.Error(err)
				return err
			}
			vServerGroupsMap[name] = resp.VServerGroupId

			readyNodes, err := lbc.getReadyNodeNames()
			if err != nil {
				glog.Error(err)
				return err
			}
			for len(readyNodes) > 0 {
				nodeList := []string{}
				if len(readyNodes) > 20 {
					nodeList = readyNodes[0:20]
					readyNodes = readyNodes[20:]
				} else {
					nodeList = readyNodes
					readyNodes = []string{}
				}
				backendServers := []slb.VBackendServerType{}
				for _, node := range nodeList {
					backendServers = append(backendServers, slb.VBackendServerType{
						ServerId: node,
						Port:     getNodePortFromVServerGroupName(name),
						Weight:   defaultBackendServerWeight,
					})
				}
				backendServersByte, err := json.Marshal(backendServers)
				if err != nil {
					glog.Error(err)
					return err
				}

				if _, err := lbc.aliyunClient.SlbClient.AddVServerGroupBackendServers(&slb.AddVServerGroupBackendServersArgs{
					RegionId:       common.Region(lbc.aliyunClient.RegionID),
					LoadBalancerId: lbc.loadBalancerID,
					VServerGroupId: vServerGroupsMap[name],
					BackendServers: string(backendServersByte),
				}); err != nil {
					glog.Error(err)
					return err
				}
			}
		}
	}

	actualRules := NewRule()
	desireRules := NewRule()
	{ // get actualRules
		rulesResp, err := lbc.aliyunClient.SlbClient.DescribeRules(&slb.DescribeRulesArgs{
			RegionId:       common.Region(lbc.aliyunClient.RegionID),
			LoadBalancerId: lbc.loadBalancerID,
			ListenerPort:   80,
		})
		if err != nil {
			glog.Error(err)
			return err
		}
		for _, rule := range rulesResp.Rules.Rule {
			actualRules.Insert(rule)
		}
		glog.Infof("actual rules: %v", actualRules)
	}
	{ // get desireRules
		for _, ingress := range ingressList.Items {
			for _, rule := range ingress.Spec.Rules {
				if rule.IngressRuleValue.HTTP == nil {
					continue
				}
				for _, p := range rule.IngressRuleValue.HTTP.Paths {
					nodePort, err := lbc.getServiceNodePort(p.Backend, ingress.Namespace)
					if err != nil {
						glog.Errorf("%s", err)
						continue
					}
					// if path is "/", do not set url for aliyun API, otherwise aliyun will return error
					if p.Path == "/" {
						desireRules.Insert(slb.Rule{
							RuleName:       getRuleName(nodePort),
							Domain:         rule.Host,
							VServerGroupId: vServerGroupsMap[getVServerGroupName(nodePort)],
						})
					} else {
						desireRules.Insert(slb.Rule{
							RuleName:       getRuleName(nodePort),
							Domain:         rule.Host,
							Url:            p.Path,
							VServerGroupId: vServerGroupsMap[getVServerGroupName(nodePort)],
						})
					}
				}
			}
		}
		glog.Infof("desire rules: %v", desireRules)
	}
	{ // ensure rules
		rulesToDelete := actualRules.Difference(desireRules)
		rulesToCreate := desireRules.Difference(actualRules)
		glog.Infof("rules to delete: %v", rulesToDelete)
		glog.Infof("rules to create: %v", rulesToCreate)
		// aliyun API support create/delete at most 10 rules in one request though,
		// we create/delete just 1 rules in a request for simplicity. in our usecase,
		// one Ingress only have one rule, so this is ok.
		wg := sync.WaitGroup{}
		wg.Add(rulesToDelete.Len())
		wg.Add(rulesToCreate.Len())
		errs := []error{}
		for _, rule := range rulesToDelete.List() {
			go func(rule slb.Rule) {
				defer wg.Done()
				args := &slb.DeleteRulesArgs{
					RegionId: common.Region(lbc.aliyunClient.RegionID),
					RuleIds:  "[\"" + rule.RuleId + "\"]", // the parameter must be a json string array.
				}
				glog.Infof("delete args: %v", args)
				if err := lbc.aliyunClient.SlbClient.DeleteRules(args); err != nil {
					glog.Error(err)
					errs = append(errs, err)
				}
			}(rule)
		}
		for _, rule := range rulesToCreate.List() {
			go func(rule slb.Rule) {
				defer wg.Done()
				ruleList := []slb.Rule{rule}
				ruleListByte, _ := json.Marshal(ruleList)
				glog.Infof("%s", string(ruleListByte))

				if err := lbc.aliyunClient.SlbClient.CreateRules(&slb.CreateRulesArgs{
					RegionId:       common.Region(lbc.aliyunClient.RegionID),
					LoadBalancerId: lbc.loadBalancerID,
					ListenerPort:   80,
					RuleList:       string(ruleListByte),
				}); err != nil {
					glog.Error(err)
					errs = append(errs, err)
				}
			}(rule)
		}
		wg.Wait()
		if len(errs) != 0 {
			return errors.NewAggregate(errs)
		}
	}

	glog.Infof("update ingress status")
	{ // update ingress status
		wg := sync.WaitGroup{}
		wg.Add(len(ingressList.Items))
		errs := []error{}
		for _, ingress := range ingressList.Items {
			go func(ingress v1beta1.Ingress) {
				defer wg.Done()

				if len(ingress.Status.LoadBalancer.Ingress) != 0 && ingress.Status.LoadBalancer.Ingress[0].IP == lbc.loadBalancerIP {
					return
				}
				if err := lbc.tryUpdateIngressStatus(ingress.Namespace, ingress.Name); err != nil {
					errs = append(errs, err)
				}
			}(ingress)
		}
		wg.Wait()

		if len(errs) <= 0 {
			glog.Infof("ingress transmigration finished")
		}
		return errors.NewAggregate(errs)
	}
}

func (lbc *LoadBalancerController) tryUpdateIngressStatus(namespace, name string) error {
	for i := 0; i < updateRetryNum; i++ {
		ingress, err := lbc.clientset.Extensions().Ingresses(namespace).Get(name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		ingress.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{
			{IP: lbc.loadBalancerIP},
		}
		if _, err = lbc.clientset.Extensions().Ingresses(namespace).UpdateStatus(ingress); err == nil {
			return nil
		} else {
			glog.Error(err)
		}
		time.Sleep(updateRetryInterval)
	}
	return fmt.Errorf("failed to update ingress status after %d retries", updateRetryNum)
}

// sync manages Node create/deletes.
func (lbc *LoadBalancerController) syncNodes(key string) (err error) {
	readyNodes, err := lbc.getReadyNodeNames()
	if err != nil {
		glog.Error(err)
		return err
	}
	desireBackendServers := sets.NewString()
	for _, node := range readyNodes {
		desireBackendServers.Insert(node)
	}
	vserverGroupsResp, err := lbc.aliyunClient.SlbClient.DescribeVServerGroups(&slb.DescribeVServerGroupsArgs{
		LoadBalancerId: lbc.loadBalancerID,
		RegionId:       common.Region(lbc.aliyunClient.RegionID),
	})
	if err != nil {
		glog.Error(err)
		return err
	}

	for _, group := range vserverGroupsResp.VServerGroups.VServerGroup {
		vServerGroupAttrResp, err := lbc.aliyunClient.SlbClient.DescribeVServerGroupAttribute(&slb.DescribeVServerGroupAttributeArgs{
			RegionId:       common.Region(lbc.aliyunClient.RegionID),
			VServerGroupId: group.VServerGroupId,
		})
		if err != nil {
			glog.Error(err)
			return err
		}
		actualBackendServers := sets.NewString()
		for _, backendServer := range vServerGroupAttrResp.BackendServers.BackendServer {
			actualBackendServers.Insert(backendServer.ServerId)
		}

		backendServerToCreate := desireBackendServers.Difference(actualBackendServers).List()
		backendServerToDelete := actualBackendServers.Difference(desireBackendServers).List()

		for len(backendServerToDelete) > 0 {
			nodeList := []string{}
			if len(backendServerToDelete) > 20 {
				nodeList = backendServerToDelete[0:20]
				backendServerToDelete = backendServerToDelete[20:]
			} else {
				nodeList = backendServerToDelete
				backendServerToDelete = []string{}
			}

			backendServers := []slb.VBackendServerType{}
			for _, node := range nodeList {
				backendServers = append(backendServers, slb.VBackendServerType{
					ServerId: node,
					Port:     getNodePortFromVServerGroupName(group.VServerGroupName),
				})
			}
			backendServersByte, err := json.Marshal(backendServers)
			if err != nil {
				glog.Error(err)
				return err
			}

			if _, err := lbc.aliyunClient.SlbClient.RemoveVServerGroupBackendServers(&slb.RemoveVServerGroupBackendServersArgs{
				RegionId:       common.Region(lbc.aliyunClient.RegionID),
				LoadBalancerId: lbc.loadBalancerID,
				VServerGroupId: group.VServerGroupId,
				BackendServers: string(backendServersByte),
			}); err != nil {
				glog.Error(err)
				return err
			}
		}
		for len(backendServerToCreate) > 0 {
			nodeList := []string{}
			if len(backendServerToDelete) > 20 {
				nodeList = backendServerToCreate[0:20]
				backendServerToCreate = backendServerToCreate[20:]
			} else {
				nodeList = backendServerToCreate
				backendServerToCreate = []string{}
			}

			backendServers := []slb.VBackendServerType{}
			for _, node := range nodeList {
				backendServers = append(backendServers, slb.VBackendServerType{
					ServerId: node,
					Port:     getNodePortFromVServerGroupName(group.VServerGroupName),
					Weight:   defaultBackendServerWeight,
				})
			}

			backendServersByte, err := json.Marshal(backendServers)
			if err != nil {
				glog.Error(err)
				return err
			}

			if _, err := lbc.aliyunClient.SlbClient.AddVServerGroupBackendServers(&slb.AddVServerGroupBackendServersArgs{
				RegionId:       common.Region(lbc.aliyunClient.RegionID),
				LoadBalancerId: lbc.loadBalancerID,
				VServerGroupId: group.VServerGroupId,
				BackendServers: string(backendServersByte),
			}); err != nil {
				glog.Error(err)
				return err
			}
		}
	}
	return nil
}

// use NodePort as VServerGroupName, since NodePort is unique globally
func getVServerGroupName(nodePort int) string {
	return strconv.Itoa(nodePort)
}

func getNodePortFromVServerGroupName(name string) int {
	port, _ := strconv.Atoi(name)
	return port
}

// use NodePort as RuleName, since NodePort is unique globally
func getRuleName(nodePort int) string {
	return strconv.Itoa(nodePort)
}

func getNodeReadyPredicate() NodeConditionPredicate {
	return func(node *v1.Node) bool {
		for ix := range node.Status.Conditions {
			condition := &node.Status.Conditions[ix]
			if condition.Type == v1.NodeReady {
				return condition.Status == v1.ConditionTrue
			}
		}
		return false
	}
}

// getReadyNodeNames returns names of schedulable, ready nodes from the node lister.
func (lbc *LoadBalancerController) getReadyNodeNames() ([]string, error) {
	nodeNames := []string{}
	nodes, err := lbc.nodeLister.NodeCondition(getNodeReadyPredicate()).List()
	if err != nil {
		return nodeNames, err
	}
	for _, n := range nodes {
		/*
			if n.Spec.Unschedulable {
				continue
			}
		*/
		nodeNames = append(nodeNames, n.Name)
	}
	return nodeNames, nil
}

// errorNodePortNotFound is an implementation of error.
type errorNodePortNotFound struct {
	backend v1beta1.IngressBackend
	origErr error
}

func (e errorNodePortNotFound) Error() string {
	return fmt.Sprintf("Could not find nodeport for backend %+v: %v", e.backend, e.origErr)
}

func (lbc *LoadBalancerController) getServiceNodePort(be v1beta1.IngressBackend, namespace string) (int, error) {
	obj, exists, err := lbc.svcLister.Indexer.Get(
		&v1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      be.ServiceName,
				Namespace: namespace,
			},
		})
	if !exists {
		return invalidPort, errorNodePortNotFound{be, fmt.Errorf("Service %v/%v not found in store", namespace, be.ServiceName)}
	}
	if err != nil {
		return invalidPort, errorNodePortNotFound{be, err}
	}
	var nodePort int
	for _, p := range obj.(*v1.Service).Spec.Ports {
		switch be.ServicePort.Type {
		case intstr.Int:
			if p.Port == be.ServicePort.IntVal {
				nodePort = int(p.NodePort)
				break
			}
		default:
			if p.Name == be.ServicePort.StrVal {
				nodePort = int(p.NodePort)
				break
			}
		}
	}
	if nodePort != invalidPort {
		return nodePort, nil
	}
	return invalidPort, errorNodePortNotFound{be, fmt.Errorf("Could not find matching nodeport from service.")}
}
