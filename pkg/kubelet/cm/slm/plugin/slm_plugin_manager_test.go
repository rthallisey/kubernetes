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

package plugin

import (
	"context"
	"testing"

	"k8s.io/client-go/kubernetes/fake"
)

func TestCleanupLifecycleTransitionsSkipsWhenNodeNameEmpty(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	pm := &SLMPluginManager{
		kubeClient: client,
		nodeName:   "",
	}

	pm.cleanupLifecycleTransitions(ctx, "")

	if got := len(client.Actions()); got != 0 {
		t.Fatalf("expected no API actions when nodeName is empty, got %d actions: %#v", got, client.Actions())
	}
}
