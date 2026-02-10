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
	"fmt"
	"strings"

	apimachineryvalidation "k8s.io/apimachinery/pkg/api/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	apivalidation "k8s.io/kubernetes/pkg/apis/core/validation"
	"k8s.io/kubernetes/pkg/apis/lifecycle"
)

// ValidateLifecycleTransition tests whether required fields in the
// LifecycleTransition are set correctly.
func ValidateLifecycleTransition(transition *lifecycle.LifecycleTransition) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMeta(&transition.ObjectMeta, false, apimachineryvalidation.NameIsDNSSubdomain, field.NewPath("metadata"))
	allErrs = append(allErrs, validateLifecycleTransitionSpec(&transition.Spec, field.NewPath("spec"))...)
	return allErrs
}

// ValidateLifecycleTransitionUpdate tests whether an update to a
// LifecycleTransition is valid. Node selection fields are immutable.
func ValidateLifecycleTransitionUpdate(newTransition, oldTransition *lifecycle.LifecycleTransition) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMetaUpdate(&newTransition.ObjectMeta, &oldTransition.ObjectMeta, field.NewPath("metadata"))
	allErrs = append(allErrs, validateLifecycleTransitionSpecUpdate(&newTransition.Spec, &oldTransition.Spec, field.NewPath("spec"))...)
	return allErrs
}

func validateLifecycleTransitionSpec(spec *lifecycle.LifecycleTransitionSpec, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	if len(spec.Start) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("start"), ""))
	}
	if len(spec.End) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("end"), ""))
	}
	if len(spec.Driver) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("driver"), ""))
	}

	allErrs = append(allErrs, validateLifecycleNodeSelection(spec, fldPath)...)

	return allErrs
}

func validateLifecycleTransitionSpecUpdate(spec, oldSpec *lifecycle.LifecycleTransitionSpec, fldPath *field.Path) field.ErrorList {
	allErrs := validateLifecycleTransitionSpec(spec, fldPath)

	// Node selection is immutable.
	allErrs = append(allErrs, apimachineryvalidation.ValidateImmutableField(spec.NodeName, oldSpec.NodeName, fldPath.Child("nodeName"))...)
	allErrs = append(allErrs, apimachineryvalidation.ValidateImmutableField(spec.NodeSelector, oldSpec.NodeSelector, fldPath.Child("nodeSelector"))...)
	allErrs = append(allErrs, apimachineryvalidation.ValidateImmutableField(spec.AllNodes, oldSpec.AllNodes, fldPath.Child("allNodes"))...)

	return allErrs
}

// validateLifecycleNodeSelection validates that exactly one of NodeName,
// NodeSelector, or AllNodes is set, following the same pattern as the
// resource API's node selection validation.
func validateLifecycleNodeSelection(spec *lifecycle.LifecycleTransitionSpec, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	setFields := make([]string, 0, 3)

	if spec.NodeName != nil {
		if *spec.NodeName != "" {
			setFields = append(setFields, "`nodeName`")
			allErrs = append(allErrs, validateNodeName(*spec.NodeName, fldPath.Child("nodeName"))...)
		} else {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("nodeName"), *spec.NodeName,
				"must be either unset or set to a non-empty string"))
		}
	}

	if spec.NodeSelector != nil {
		setFields = append(setFields, "`nodeSelector`")
		allErrs = append(allErrs, apivalidation.ValidateNodeSelector(spec.NodeSelector, false, fldPath.Child("nodeSelector"))...)
		if len(spec.NodeSelector.NodeSelectorTerms) != 1 {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("nodeSelector", "nodeSelectorTerms"), spec.NodeSelector.NodeSelectorTerms,
				"must have exactly one node selector term"))
		}
	}

	if spec.AllNodes != nil {
		if *spec.AllNodes {
			setFields = append(setFields, "`allNodes`")
		} else {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("allNodes"), *spec.AllNodes,
				"must be either unset or set to true"))
		}
	}

	switch len(setFields) {
	case 0:
		allErrs = append(allErrs, field.Required(fldPath, "exactly one of `nodeName`, `nodeSelector`, or `allNodes` is required"))
	case 1:
		// exactly one set — valid
	default:
		allErrs = append(allErrs, field.Invalid(fldPath, fmt.Sprintf("{%s}", strings.Join(setFields, ", ")),
			"exactly one of `nodeName`, `nodeSelector`, or `allNodes` is required"))
	}

	return allErrs
}

func validateNodeName(name string, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	for _, msg := range apivalidation.ValidateNodeName(name, false) {
		allErrs = append(allErrs, field.Invalid(fldPath, name, msg))
	}
	return allErrs
}

// ValidateLifecycleEvent tests whether required fields in the
// LifecycleEvent are set correctly.
func ValidateLifecycleEvent(event *lifecycle.LifecycleEvent) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMeta(&event.ObjectMeta, false, apimachineryvalidation.NameIsDNSSubdomain, field.NewPath("metadata"))
	allErrs = append(allErrs, validateLifecycleEventSpec(&event.Spec, field.NewPath("spec"))...)
	return allErrs
}

// ValidateLifecycleEventUpdate tests whether an update to a LifecycleEvent
// is valid.
func ValidateLifecycleEventUpdate(newEvent, oldEvent *lifecycle.LifecycleEvent) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMetaUpdate(&newEvent.ObjectMeta, &oldEvent.ObjectMeta, field.NewPath("metadata"))

	// TransitionName and BindingNode are immutable.
	specPath := field.NewPath("spec")
	allErrs = append(allErrs, apimachineryvalidation.ValidateImmutableField(newEvent.Spec.TransitionName, oldEvent.Spec.TransitionName, specPath.Child("transitionName"))...)
	allErrs = append(allErrs, apimachineryvalidation.ValidateImmutableField(newEvent.Spec.BindingNode, oldEvent.Spec.BindingNode, specPath.Child("bindingNode"))...)

	return allErrs
}

func validateLifecycleEventSpec(spec *lifecycle.LifecycleEventSpec, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	if len(spec.TransitionName) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("transitionName"), ""))
	}
	if len(spec.BindingNode) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("bindingNode"), ""))
	} else {
		allErrs = append(allErrs, validateNodeName(spec.BindingNode, fldPath.Child("bindingNode"))...)
	}

	return allErrs
}
