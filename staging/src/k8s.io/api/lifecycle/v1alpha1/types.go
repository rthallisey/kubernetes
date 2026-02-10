/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// LifecycleTransition defines the desired intent for a Kubernetes resource to
// undergo a lifecycle change.
type LifecycleTransition struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Spec defines the desired lifecycle transition.
	Spec LifecycleTransitionSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// LifecycleTransitionList contains a list of LifecycleTransition resources.
type LifecycleTransitionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	Items           []LifecycleTransition `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// LifecycleTransitionSpec describes the parameters of a lifecycle transition.
type LifecycleTransitionSpec struct {
	// start identifies the initial state of the lifecycle transition.
	Start string `json:"start" protobuf:"bytes,1,opt,name=start"`

	// end identifies the desired terminal state of the lifecycle transition.
	End string `json:"end" protobuf:"bytes,2,opt,name=end"`

	// nodeName identifies the specific Node that has a Driver capable of
	// reconciling the LifecycleTransition.
	// +optional
	NodeName *string `json:"nodeName,omitempty" protobuf:"bytes,3,opt,name=nodeName"`

	// nodeSelector defines which Nodes have Drivers capable of
	// reconciling the LifecycleTransition.
	// +optional
	NodeSelector *v1.NodeSelector `json:"nodeSelector,omitempty" protobuf:"bytes,4,opt,name=nodeSelector"`

	// allNodes indicates that all Nodes are capable of
	// reconciling the LifecycleTransition.
	// +optional
	AllNodes *bool `json:"allNodes,omitempty" protobuf:"varint,5,opt,name=allNodes"`

	// sla specifies the duration by which the transition from Start to End
	// must be completed.
	// +optional
	Sla *metav1.Duration `json:"sla,omitempty" protobuf:"bytes,6,opt,name=sla"`

	// driver specifies the unique identifier of the lifecycle driver
	// responsible for reconciling this transition.
	Driver string `json:"driver" protobuf:"bytes,7,opt,name=driver"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// LifecycleEvent represents a binding between a LifecycleTransition and the
// Kubelet/Driver responsible for executing it.
type LifecycleEvent struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Spec defines the binding parameters for claiming a transition.
	Spec LifecycleEventSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`

	// Status reports the current state of the transition claim.
	// +optional
	Status LifecycleEventStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// LifecycleEventList contains a list of LifecycleEvent resources.
type LifecycleEventList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	Items           []LifecycleEvent `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// LifecycleEventSpec defines the binding parameters to claim a transition.
type LifecycleEventSpec struct {
	// transitionName refers to a LifecycleTransition object by name.
	TransitionName string `json:"transitionName" protobuf:"bytes,1,opt,name=transitionName"`

	// bindingNode identifies a specific Node whose Kubelet can claim the event.
	BindingNode string `json:"bindingNode" protobuf:"bytes,2,opt,name=bindingNode"`
}

// LifecycleEventStatus reports the current state of the transition claim.
type LifecycleEventStatus struct {
	// claimStatus represents the current phase of the event's lifecycle.
	// +optional
	ClaimStatus LifecycleEventClaimStatus `json:"claimStatus,omitempty" protobuf:"bytes,1,opt,name=claimStatus"`

	// driver is a copy of the driver name from the LifecycleTransition.
	// +optional
	Driver string `json:"driver,omitempty" protobuf:"bytes,2,opt,name=driver"`

	// sla specifies the deadline by which the transition must complete.
	// +optional
	Sla *metav1.Time `json:"sla,omitempty" protobuf:"bytes,3,opt,name=sla"`
}

// LifecycleEventClaimStatus represents the phase of a LifecycleEvent.
type LifecycleEventClaimStatus string

const (
	LifecycleEventPending    LifecycleEventClaimStatus = "Pending"
	LifecycleEventClaimed    LifecycleEventClaimStatus = "Claimed"
	LifecycleEventFailed     LifecycleEventClaimStatus = "Failed"
	LifecycleEventSlaExpired LifecycleEventClaimStatus = "SlaExpired"
	LifecycleEventSucceeded  LifecycleEventClaimStatus = "Succeeded"
)
