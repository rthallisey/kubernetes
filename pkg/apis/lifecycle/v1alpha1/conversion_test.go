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
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kubernetes/pkg/apis/lifecycle"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAddFieldLabelConversions(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, addFieldLabelConversions(scheme))

	tests := []struct {
		name      string
		kind      string
		label     string
		value     string
		wantErr   bool
		wantLabel string
		wantValue string
	}{
		{
			name:      "lifecycleevent metadata.name allowed",
			kind:      "LifecycleEvent",
			label:     "metadata.name",
			value:     "event-a",
			wantErr:   false,
			wantLabel: "metadata.name",
			wantValue: "event-a",
		},
		{
			name:      "lifecycleevent bindingNode allowed",
			kind:      "LifecycleEvent",
			label:     lifecycle.LifecycleEventSelectorBindingNode,
			value:     "node-a",
			wantErr:   false,
			wantLabel: lifecycle.LifecycleEventSelectorBindingNode,
			wantValue: "node-a",
		},
		{
			name:    "lifecycleevent unsupported field rejected",
			kind:    "LifecycleEvent",
			label:   lifecycle.LifecycleTransitionSelectorDriver,
			value:   "driver-a",
			wantErr: true,
		},
		{
			name:      "lifecycletransition existing selector still allowed",
			kind:      "LifecycleTransition",
			label:     lifecycle.LifecycleTransitionSelectorDriver,
			value:     "driver-a",
			wantErr:   false,
			wantLabel: lifecycle.LifecycleTransitionSelectorDriver,
			wantValue: "driver-a",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			label, value, err := scheme.ConvertFieldLabel(SchemeGroupVersion.WithKind(tt.kind), tt.label, tt.value)
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantLabel, label)
			assert.Equal(t, tt.wantValue, value)
		})
	}
}
