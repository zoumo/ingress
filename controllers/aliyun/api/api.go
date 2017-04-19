package api

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/pkg/api/v1"
)

type LoadBalancer struct {
	metav1.TypeMeta `json:",inline"`
	v1.ObjectMeta   `json:"metadata,omitempty"`

	Spec   LoadBalancerSpec   `json:"spec,omitempty"`
	Status LoadBalancerStatus `json:"status,omitempty"`
}

type LoadBalancerSpec struct {
	NginxLoadBalancer *NginxLoadBalancer `json:"nginxLoadBalancer,omitempty"`
	//HaproxyLoadBalancer *HaproxyLoadBalancer
	AliyunLoadBalancer *AliyunLoadBalancer `json:"aliyunLoadBalancer,omitempty"`
	//AnchnetLoadBalancer *AnchnetLoadBalancer
}

type NginxLoadBalancer struct {
	Service v1.ObjectReference `json:"service,omitempty"`
}

type AliyunLoadBalancer struct {
	LoadBalancerName string `json:"loadbalancerName,omitempty"`
	LoadBalancerID   string `json:"loadbalancerID,omitempty"`
	LoadBalancerIP   string `json:"loadbalancerIP,omitempty"`
}

type LoadBalancerStatus struct {
	Phase   LoadBalancerPhase `json:"phase,omitempty"`
	Message string            `json:"message,omitempty"`
	Reason  string            `json:"reason,omitempty"`
}

type LoadBalancerPhase string

const (
	LoadBalancerAvailable LoadBalancerPhase = "Available"
	LoadBalancerBound     LoadBalancerPhase = "Bound"
	LoadBalancerReleased  LoadBalancerPhase = "Released"
	LoadBalancerFailed    LoadBalancerPhase = "Failed"
)

type LoadBalancerClaim struct {
	metav1.TypeMeta `json:",inline"`
	v1.ObjectMeta   `json:"metadata,omitempty"`

	Spec   LoadBalancerClaimSpec   `json:"spec,omitempty"`
	Status LoadBalancerClaimStatus `json:"status,omitempty"`
}

type LoadBalancerClaimSpec struct {
	// the binding reference to the LoadBalancer backing this claim.
	LoadBalancerName string `json:"loadBalancerName,omitempty"`
}

type LoadBalancerClaimStatus struct {
	Phase   LoadBalancerClaimPhase `json:"phase,omitempty"`
	Message string                 `json:"message,omitempty"`
}

type LoadBalancerClaimPhase string

const (
	LoadBalancerClaimPending LoadBalancerClaimPhase = "Pending"
	LoadBalancerClaimBound   LoadBalancerClaimPhase = "Bound"
	LoadBalancerClaimFailed  LoadBalancerClaimPhase = "Failed"
)
