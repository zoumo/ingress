package main

import (
	"encoding/json"
	"flag"
	"os"
	"strings"

	"github.com/denverdino/aliyungo/common"
	"github.com/denverdino/aliyungo/slb"
	"github.com/golang/glog"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	tprapi "k8s.io/ingress/controllers/aliyun/api"
	"k8s.io/ingress/controllers/aliyun/client"
	"k8s.io/ingress/controllers/aliyun/controller"
)

func main() {
	flag.Set("logtostderr", "true")
	flag.Parse()
	aliyunLoadbalancerName := getAliyunLoadBalancerNameFromEnv()
	loadbalancerName := getK8sLoadBalancerNameFromEnv()
	aliyunClient := client.NewAliyunClientFormEnv()

	// creates the in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}
	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	tprclient, err := client.NewTPRClient(config)
	if err != nil {
		panic(err.Error())
	}

	loadbalancerID, loadbalancerIP, err := ensureLoadBalancer(aliyunClient, aliyunLoadbalancerName)
	if err != nil {
		glog.Fatal(err)
	}
	if err := ensureHTTPListener(aliyunClient, loadbalancerID); err != nil {
		glog.Fatal(err)
	}

	if err := updateLoadBalancerIDAndIP(tprclient, loadbalancerID, loadbalancerIP); err != nil {
		glog.Fatal(err)
	}

	glog.Infof("%s", loadbalancerIP)

	lbc := controller.NewLoadBalancerController(aliyunClient, clientset, loadbalancerName, loadbalancerID, loadbalancerIP)

	lbc.Run(wait.NeverStop)
}

func ensureLoadBalancer(aliyunClient *client.AliyunClient, loadbalancerName string) (loadbalancerID, loadbalancerIP string, err error) {
	lbs, err := aliyunClient.SlbClient.DescribeLoadBalancers(&slb.DescribeLoadBalancersArgs{
		RegionId: common.Region(aliyunClient.RegionID),
	})
	if err != nil {
		return "", "", err
	}

	for _, lb := range lbs {
		if lb.LoadBalancerName == loadbalancerName {
			return lb.LoadBalancerId, lb.Address, nil
		}
	}

	resp, err := aliyunClient.SlbClient.CreateLoadBalancer(&slb.CreateLoadBalancerArgs{
		RegionId:           common.Region(aliyunClient.RegionID),
		LoadBalancerName:   loadbalancerName,
		AddressType:        slb.InternetAddressType,
		InternetChargeType: slb.PayByTraffic,
	})
	if err != nil {
		return "", "", err
	}

	return resp.LoadBalancerId, resp.Address, nil
}

func ensureHTTPListener(aliyunClient *client.AliyunClient, loadbalancerID string) error {
	const listenerAlreadyExistMessage = "There is already a listener bound to the port on the specified load balancer"
	if err := aliyunClient.SlbClient.CreateLoadBalancerHTTPListener(&slb.CreateLoadBalancerHTTPListenerArgs{
		LoadBalancerId:    loadbalancerID,
		ListenerPort:      80,
		BackendServerPort: 80,
		Bandwidth:         -1, // pay by traffic and does not limit bandwidth
		StickySession:     slb.OffFlag,
		HealthCheck:       slb.OffFlag,
	}); err != nil && !strings.Contains(err.Error(), listenerAlreadyExistMessage) {
		return err
	}

	return aliyunClient.SlbClient.StartLoadBalancerListener(loadbalancerID, 80)
}

func updateLoadBalancerIDAndIP(tprclient *rest.RESTClient, loadbalancerID, loadbalancerIP string) error {
	loadbalancerName := os.Getenv("LOADBALANCER_NAME")
	if loadbalancerName == "" {
		glog.Fatalf("missing LOADBALANCER_NAME environment variable")
	}
	glog.Infof("update loadbalancer IP and ID")
	const namespace = "kube-system"
	const resource = "loadbalancers"

	var lb tprapi.LoadBalancer
	body, err := tprclient.Get().
		Resource(resource).
		Namespace(namespace).
		Name(loadbalancerName).
		DoRaw()
	if err != nil {
		glog.Error(err)
		return err
	}

	json.Unmarshal(body, &lb)

	if lb.Spec.AliyunLoadBalancer.LoadBalancerID == loadbalancerID && lb.Spec.AliyunLoadBalancer.LoadBalancerIP == loadbalancerIP {
		return nil
	}
	lb.Spec.AliyunLoadBalancer.LoadBalancerID = loadbalancerID
	lb.Spec.AliyunLoadBalancer.LoadBalancerIP = loadbalancerIP

	body, _ = json.Marshal(&lb)
	if _, err := tprclient.Put().
		Resource(resource).
		Namespace(namespace).
		Name(loadbalancerName).
		Body(body).
		DoRaw(); err != nil {
		glog.Error(err)
		return err
	}

	return nil
}

func getAliyunLoadBalancerNameFromEnv() string {
	clusterName := os.Getenv("CLUSTER_NAME")
	if clusterName == "" {
		glog.Fatalf("missing CLUSTER_NAME environment variable")
	}
	loadbalancerName := os.Getenv("LOADBALANCER_NAME")
	if loadbalancerName == "" {
		glog.Fatalf("missing LOADBALANCER_NAME environment variable")
	}
	return clusterName + "-" + loadbalancerName
}

func getK8sLoadBalancerNameFromEnv() string {
	loadbalancerName := os.Getenv("LOADBALANCER_NAME")
	if loadbalancerName == "" {
		glog.Fatalf("missing LOADBALANCER_NAME environment variable")
	}
	return loadbalancerName
}
