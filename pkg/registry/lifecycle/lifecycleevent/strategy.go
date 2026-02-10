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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/lifecycle"
	"k8s.io/kubernetes/pkg/apis/lifecycle/validation"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// lifecycleEventStrategy implements behavior for LifecycleEvent objects.
type lifecycleEventStrategy struct {
	runtime.ObjectTyper
	names.NameGenerator
}

// Strategy is the default logic that applies when creating and updating
// LifecycleEvent objects.
var Strategy = lifecycleEventStrategy{legacyscheme.Scheme, names.SimpleNameGenerator}

func (lifecycleEventStrategy) NamespaceScoped() bool {
	return false
}

// GetResetFields returns the set of fields that get reset by the strategy and
// should not be modified by the user. For a create/update that is the status.
func (lifecycleEventStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return map[fieldpath.APIVersion]*fieldpath.Set{
		"lifecycle.k8s.io/v1alpha1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
	}
}

func (lifecycleEventStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	event := obj.(*lifecycle.LifecycleEvent)
	// default to Pending.
	event.Status = lifecycle.LifecycleEventStatus{
		ClaimStatus: lifecycle.LifecycleEventPending,
	}
}

func (lifecycleEventStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	event := obj.(*lifecycle.LifecycleEvent)
	return validation.ValidateLifecycleEvent(event)
}

func (lifecycleEventStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

func (lifecycleEventStrategy) Canonicalize(obj runtime.Object) {
}

func (lifecycleEventStrategy) AllowCreateOnUpdate() bool {
	return false
}

func (lifecycleEventStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newEvent := obj.(*lifecycle.LifecycleEvent)
	oldEvent := old.(*lifecycle.LifecycleEvent)
	newEvent.Status = oldEvent.Status
}

func (lifecycleEventStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return validation.ValidateLifecycleEventUpdate(obj.(*lifecycle.LifecycleEvent), old.(*lifecycle.LifecycleEvent))
}

func (lifecycleEventStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

func (lifecycleEventStrategy) AllowUnconditionalUpdate() bool {
	return true
}

// lifecycleEventStatusStrategy implements the status subresource strategy.
type lifecycleEventStatusStrategy struct {
	lifecycleEventStrategy
}

// StatusStrategy is the logic that applies when updating the status of a
// LifecycleEvent object.
var StatusStrategy = lifecycleEventStatusStrategy{Strategy}

// GetResetFields returns the set of fields that get reset by the strategy and
// should not be modified by the user. For a status update that is the spec.
func (lifecycleEventStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return map[fieldpath.APIVersion]*fieldpath.Set{
		"lifecycle.k8s.io/v1alpha1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("metadata"),
			fieldpath.MakePathOrDie("spec"),
		),
	}
}

func (lifecycleEventStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newEvent := obj.(*lifecycle.LifecycleEvent)
	oldEvent := old.(*lifecycle.LifecycleEvent)
	// Spec is not allowed to be set via the status subresource.
	newEvent.Spec = oldEvent.Spec
	metav1.ResetObjectMetaForStatus(&newEvent.ObjectMeta, &oldEvent.ObjectMeta)
}

func (lifecycleEventStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	// Only validate that the object metadata update is valid.
	// Status-specific validation can be added here later.
	return validation.ValidateLifecycleEventUpdate(obj.(*lifecycle.LifecycleEvent), old.(*lifecycle.LifecycleEvent))
}
