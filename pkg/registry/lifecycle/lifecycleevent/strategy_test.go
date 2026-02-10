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

package lifecycleevent

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/apis/lifecycle"

	"github.com/stretchr/testify/assert"
)

func TestPrepareForCreateDefaultsPendingStatus(t *testing.T) {
	event := &lifecycle.LifecycleEvent{
		ObjectMeta: metav1.ObjectMeta{Name: "event-a"},
		Spec: lifecycle.LifecycleEventSpec{
			TransitionName: "transition-a",
			BindingNode:    "node-a",
		},
		Status: lifecycle.LifecycleEventStatus{ClaimStatus: lifecycle.LifecycleEventSucceeded},
	}

	Strategy.PrepareForCreate(context.Background(), event)
	assert.Equal(t, lifecycle.LifecycleEventPending, event.Status.ClaimStatus)
}

func TestPrepareForUpdatePreservesOldStatus(t *testing.T) {
	oldObj := &lifecycle.LifecycleEvent{
		ObjectMeta: metav1.ObjectMeta{Name: "event-a"},
		Spec: lifecycle.LifecycleEventSpec{
			TransitionName: "transition-a",
			BindingNode:    "node-a",
		},
		Status: lifecycle.LifecycleEventStatus{ClaimStatus: lifecycle.LifecycleEventClaimed},
	}
	newObj := oldObj.DeepCopy()
	newObj.Status.ClaimStatus = lifecycle.LifecycleEventSucceeded

	Strategy.PrepareForUpdate(context.Background(), newObj, oldObj)
	assert.Equal(t, lifecycle.LifecycleEventClaimed, newObj.Status.ClaimStatus)
}

func TestStatusStrategyPrepareForUpdatePreservesSpecAndMetadata(t *testing.T) {
	oldObj := &lifecycle.LifecycleEvent{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "event-a",
			ResourceVersion: "7",
			Labels:          map[string]string{"a": "b"},
		},
		Spec: lifecycle.LifecycleEventSpec{
			TransitionName: "transition-a",
			BindingNode:    "node-a",
		},
		Status: lifecycle.LifecycleEventStatus{ClaimStatus: lifecycle.LifecycleEventPending},
	}
	newObj := oldObj.DeepCopy()
	newObj.Spec.TransitionName = "changed"
	newObj.ObjectMeta.Labels = map[string]string{"x": "y"}
	newObj.Status.ClaimStatus = lifecycle.LifecycleEventClaimed

	StatusStrategy.PrepareForUpdate(context.Background(), newObj, oldObj)

	assert.Equal(t, oldObj.Spec, newObj.Spec)
	assert.Equal(t, oldObj.ObjectMeta.Name, newObj.ObjectMeta.Name)
	assert.Equal(t, oldObj.ObjectMeta.ResourceVersion, newObj.ObjectMeta.ResourceVersion)
	assert.Equal(t, lifecycle.LifecycleEventClaimed, newObj.Status.ClaimStatus)
}
