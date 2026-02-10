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

package validation

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/lifecycle"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateLifecycleTransitionNodeSelection(t *testing.T) {
	trueVal := true
	falseVal := false
	nodeName := "node-a"

	validNodeName := &lifecycle.LifecycleTransition{
		ObjectMeta: metav1.ObjectMeta{Name: "lt-node-name"},
		Spec: lifecycle.LifecycleTransitionSpec{
			Start:    "start",
			End:      "end",
			Driver:   "driver.example.com",
			NodeName: &nodeName,
		},
	}
	errList := ValidateLifecycleTransition(validNodeName)
	require.Empty(t, errList)

	missingSelector := validNodeName.DeepCopy()
	missingSelector.Spec.NodeName = nil
	errList = ValidateLifecycleTransition(missingSelector)
	require.NotEmpty(t, errList)
	assert.Contains(t, errList.ToAggregate().Error(), "exactly one of `nodeName`, `nodeSelector`, or `allNodes` is required")

	multipleSelectors := validNodeName.DeepCopy()
	multipleSelectors.Spec.AllNodes = &trueVal
	errList = ValidateLifecycleTransition(multipleSelectors)
	require.NotEmpty(t, errList)
	assert.Contains(t, errList.ToAggregate().Error(), "exactly one of `nodeName`, `nodeSelector`, or `allNodes` is required")

	allNodesFalse := validNodeName.DeepCopy()
	allNodesFalse.Spec.NodeName = nil
	allNodesFalse.Spec.AllNodes = &falseVal
	errList = ValidateLifecycleTransition(allNodesFalse)
	require.NotEmpty(t, errList)
	assert.Contains(t, errList.ToAggregate().Error(), "must be either unset or set to true")

	selector := &core.NodeSelector{NodeSelectorTerms: []core.NodeSelectorTerm{{}}}
	validNodeSelector := validNodeName.DeepCopy()
	validNodeSelector.Spec.NodeName = nil
	validNodeSelector.Spec.NodeSelector = selector
	errList = ValidateLifecycleTransition(validNodeSelector)
	require.Empty(t, errList)
}

func TestValidateLifecycleTransitionUpdateNodeSelectionImmutable(t *testing.T) {
	nodeA := "node-a"
	nodeB := "node-b"

	oldObj := &lifecycle.LifecycleTransition{
		ObjectMeta: metav1.ObjectMeta{Name: "lt"},
		Spec: lifecycle.LifecycleTransitionSpec{
			Start:    "start",
			End:      "end",
			Driver:   "driver.example.com",
			NodeName: &nodeA,
		},
	}
	newObj := oldObj.DeepCopy()
	newObj.Spec.NodeName = &nodeB

	errList := ValidateLifecycleTransitionUpdate(newObj, oldObj)
	require.NotEmpty(t, errList)
	assert.Contains(t, errList.ToAggregate().Error(), "field is immutable")
}

func TestValidateLifecycleEventUpdateImmutableSpecFields(t *testing.T) {
	oldObj := &lifecycle.LifecycleEvent{
		ObjectMeta: metav1.ObjectMeta{Name: "event-a"},
		Spec: lifecycle.LifecycleEventSpec{
			TransitionName: "transition-a",
			BindingNode:    "node-a",
		},
	}
	newObj := oldObj.DeepCopy()
	newObj.Spec.TransitionName = "transition-b"
	newObj.Spec.BindingNode = "node-b"

	errList := ValidateLifecycleEventUpdate(newObj, oldObj)
	require.NotEmpty(t, errList)
	errText := errList.ToAggregate().Error()
	assert.Contains(t, errText, "spec.transitionName")
	assert.Contains(t, errText, "spec.bindingNode")
	assert.Contains(t, errText, "field is immutable")
}
