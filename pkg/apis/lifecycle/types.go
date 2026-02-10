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

package lifecycle

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/apis/core"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// LifecycleTransition defines the desired intent for a Kubernetes resource to
// undergo a lifecycle change. It acts as the source of truth for the requested
// start and end states, the location of the change, and how long it is allowed
// to take.
type LifecycleTransition struct {
	metav1.TypeMeta
	// Standard object's metadata.
	// +optional
	metav1.ObjectMeta

	// Spec defines the desired lifecycle transition.
	Spec LifecycleTransitionSpec
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// LifecycleTransitionList contains a list of LifecycleTransition resources.
type LifecycleTransitionList struct {
	metav1.TypeMeta
	// Standard list metadata.
	// +optional
	metav1.ListMeta

	// Items is the list of LifecycleTransitions.
	Items []LifecycleTransition
}

const (
	// LifecycleTransitionSelectorNodeName can be used in a [metav1.ListOptions]
	// field selector to filter based on [LifecycleTransitionSpec.NodeName].
	LifecycleTransitionSelectorNodeName = "spec.nodeName"
	// LifecycleTransitionSelectorDriver can be used in a [metav1.ListOptions]
	// field selector to filter based on [LifecycleTransitionSpec.Driver].
	LifecycleTransitionSelectorDriver = "spec.driver"
	// LifecycleEventSelectorBindingNode can be used in a [metav1.ListOptions]
	// field selector to filter based on [LifecycleEventSpec.BindingNode].
	LifecycleEventSelectorBindingNode = "spec.bindingNode"
)

// LifecycleTransitionSpec describes the parameters of a lifecycle transition.
type LifecycleTransitionSpec struct {
	// Start identifies the initial state of the lifecycle transition.
	// This value is reflected as a Condition reason on the target resource.
	Start string

	// End identifies the desired terminal state of the lifecycle transition.
	// Once the driver completes its work, this state is published as a
	// Condition reason on the target resource.
	End string

	// NodeName identifies the specific Node that has a Driver capable of
	// reconciling the LifecycleTransition.
	//
	// Exactly one of NodeName, NodeSelector, or AllNodes must be set.
	// This field is immutable.
	//
	// +optional
	// +oneOf=LifecycleNodeSelection
	NodeName *string

	// NodeSelector defines which Nodes have Drivers capable of
	// reconciling the LifecycleTransition.
	//
	// Must use exactly one term.
	//
	// Exactly one of NodeName, NodeSelector, or AllNodes must be set.
	// This field is immutable.
	//
	// +optional
	// +oneOf=LifecycleNodeSelection
	NodeSelector *core.NodeSelector

	// AllNodes indicates that all Nodes are capable of
	// reconciling the LifecycleTransition.
	//
	// Exactly one of NodeName, NodeSelector, or AllNodes must be set.
	// This field is immutable.
	//
	// +optional
	// +oneOf=LifecycleNodeSelection
	AllNodes *bool

	// Sla specifies the duration by which the transition from Start to End
	// must be completed.
	//
	// If the transition doesn't reach completion in the required duration, the
	// associated LifecycleEvent will transition to the "SlaExpired" status.
	// +optional
	Sla *metav1.Duration

	// Driver specifies the unique identifier of the lifecycle driver
	// responsible for reconciling this transition.
	Driver string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// LifecycleEvent represents a binding between a LifecycleTransition and the
// Kubelet/Driver responsible for executing it.
type LifecycleEvent struct {
	metav1.TypeMeta
	// Standard object's metadata.
	// +optional
	metav1.ObjectMeta

	// Spec defines the binding parameters for claiming a transition.
	Spec LifecycleEventSpec

	// Status reports the current state of the transition claim.
	// +optional
	Status LifecycleEventStatus
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// LifecycleEventList contains a list of LifecycleEvent resources.
type LifecycleEventList struct {
	metav1.TypeMeta
	// Standard list metadata.
	// +optional
	metav1.ListMeta

	// Items is the list of LifecycleEvents.
	Items []LifecycleEvent
}

// LifecycleEventSpec defines the binding parameters to effectively claim
// a transition for execution.
type LifecycleEventSpec struct {
	// TransitionName refers to a LifecycleTransition object by name.
	TransitionName string

	// BindingNode identifies a specific Node whose Kubelet can claim the
	// corresponding LifecycleEvent.
	BindingNode string
}

// LifecycleEventStatus reports the current state of the transition claim
// and identifies the active driver.
type LifecycleEventStatus struct {
	// ClaimStatus represents the current phase of the event's lifecycle.
	// +optional
	ClaimStatus LifecycleEventClaimStatus

	// Driver is a copy of the driver name from the LifecycleTransition.
	// It is needed by the Kubelet to match with a registered Driver.
	// +optional
	Driver string

	// Sla specifies the deadline by which the transition from Start to End
	// must be completed.
	//
	// The timestamp is calculated by adding the SLA duration to the
	// current time after the LifecycleEvent reaches the Claimed state.
	// +optional
	Sla *metav1.Time
}

// LifecycleEventClaimStatus represents the phase of a LifecycleEvent.
type LifecycleEventClaimStatus string

const (
	// LifecycleEventPending indicates the event has been created but has
	// not yet been claimed by the Kubelet/Driver.
	LifecycleEventPending LifecycleEventClaimStatus = "Pending"

	// LifecycleEventClaimed indicates that the Kubelet and the designated
	// Driver have successfully taken ownership and are actively
	// reconciling the transition.
	LifecycleEventClaimed LifecycleEventClaimStatus = "Claimed"

	// LifecycleEventFailed indicates that the event was attempted to be
	// claimed, but the claim failed.
	LifecycleEventFailed LifecycleEventClaimStatus = "Failed"

	// LifecycleEventSlaExpired indicates that the transition failed to
	// reach the End state within the duration specified in the
	// LifecycleTransition SLA.
	LifecycleEventSlaExpired LifecycleEventClaimStatus = "SlaExpired"

	// LifecycleEventSucceeded indicates that the transition was attempted
	// and completed successfully.
	LifecycleEventSucceeded LifecycleEventClaimStatus = "Succeeded"
)
