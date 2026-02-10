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

package lifecycletransition

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/lifecycle"
	"k8s.io/kubernetes/pkg/apis/lifecycle/validation"
)

// lifecycleTransitionStrategy implements behavior for LifecycleTransition objects.
type lifecycleTransitionStrategy struct {
	runtime.ObjectTyper
	names.NameGenerator
}

// Strategy is the default logic that applies when creating and updating
// LifecycleTransition objects.
var Strategy = lifecycleTransitionStrategy{legacyscheme.Scheme, names.SimpleNameGenerator}

func (lifecycleTransitionStrategy) NamespaceScoped() bool {
	return false
}

func (lifecycleTransitionStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
}

func (lifecycleTransitionStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	transition := obj.(*lifecycle.LifecycleTransition)
	return validation.ValidateLifecycleTransition(transition)
}

func (lifecycleTransitionStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

func (lifecycleTransitionStrategy) Canonicalize(obj runtime.Object) {
}

func (lifecycleTransitionStrategy) AllowCreateOnUpdate() bool {
	return false
}

func (lifecycleTransitionStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
}

func (lifecycleTransitionStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return validation.ValidateLifecycleTransitionUpdate(obj.(*lifecycle.LifecycleTransition), old.(*lifecycle.LifecycleTransition))
}

func (lifecycleTransitionStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

func (lifecycleTransitionStrategy) AllowUnconditionalUpdate() bool {
	return true
}

// GetAttrs returns labels and fields of a given object for filtering purposes.
func GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	transition, ok := obj.(*lifecycle.LifecycleTransition)
	if !ok {
		return nil, nil, fmt.Errorf("not a LifecycleTransition")
	}
	return labels.Set(transition.ObjectMeta.Labels), toSelectableFields(transition), nil
}

// Match returns a generic matcher for a given label and field selector.
func Match(label labels.Selector, field fields.Selector) storage.SelectionPredicate {
	return storage.SelectionPredicate{
		Label:       label,
		Field:       field,
		GetAttrs:    GetAttrs,
		IndexFields: []string{lifecycle.LifecycleTransitionSelectorDriver},
	}
}

// toSelectableFields returns a field set that represents the object for
// field selector filtering.
func toSelectableFields(transition *lifecycle.LifecycleTransition) fields.Set {
	fieldSet := make(fields.Set, 3)
	if transition.Spec.NodeName == nil {
		fieldSet[lifecycle.LifecycleTransitionSelectorNodeName] = ""
	} else {
		fieldSet[lifecycle.LifecycleTransitionSelectorNodeName] = *transition.Spec.NodeName
	}
	fieldSet[lifecycle.LifecycleTransitionSelectorDriver] = transition.Spec.Driver
	// Adds metadata.name (and metadata.namespace for namespaced resources).
	return generic.AddObjectMetaFieldsSet(fieldSet, &transition.ObjectMeta, false)
}

// TriggerFunc maps watched field selectors to trigger functions.
// Only one index is supported by the watch cache.
var TriggerFunc = map[string]storage.IndexerFunc{
	lifecycle.LifecycleTransitionSelectorDriver: driverTriggerFunc,
}

func driverTriggerFunc(obj runtime.Object) string {
	return obj.(*lifecycle.LifecycleTransition).Spec.Driver
}

// Indexers returns the indexers for LifecycleTransition storage.
func Indexers() *cache.Indexers {
	return &cache.Indexers{
		storage.FieldIndex(lifecycle.LifecycleTransitionSelectorDriver): driverIndexFunc,
	}
}

func driverIndexFunc(obj interface{}) ([]string, error) {
	transition, ok := obj.(*lifecycle.LifecycleTransition)
	if !ok {
		return nil, fmt.Errorf("not a LifecycleTransition")
	}
	return []string{transition.Spec.Driver}, nil
}
