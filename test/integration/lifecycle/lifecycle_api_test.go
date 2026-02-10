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
	"context"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	lifecyclev1alpha1 "k8s.io/api/lifecycle/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	kubeapiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	"k8s.io/kubernetes/pkg/apis/lifecycle"
	"k8s.io/kubernetes/test/integration/framework"
)

func TestLifecycleTransitionValidationAndImmutability(t *testing.T) {
	ctx := context.Background()
	client, tearDownFn := startLifecycleTestServer(t)
	defer tearDownFn()

	nodeName := "node-a"
	transition := &lifecyclev1alpha1.LifecycleTransition{
		ObjectMeta: metav1.ObjectMeta{Name: "transition-valid"},
		Spec: lifecyclev1alpha1.LifecycleTransitionSpec{
			Start:    "drain-started",
			End:      "drain-complete",
			NodeName: &nodeName,
			Driver:   "example.com/driver",
		},
	}
	created, err := client.LifecycleV1alpha1().LifecycleTransitions().Create(ctx, transition, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create valid transition: %v", err)
	}

	invalidNoSelector := &lifecyclev1alpha1.LifecycleTransition{
		ObjectMeta: metav1.ObjectMeta{Name: "transition-invalid-no-selector"},
		Spec: lifecyclev1alpha1.LifecycleTransitionSpec{
			Start:  "drain-started",
			End:    "drain-complete",
			Driver: "example.com/driver",
		},
	}
	_, err = client.LifecycleV1alpha1().LifecycleTransitions().Create(ctx, invalidNoSelector, metav1.CreateOptions{})
	if !apierrors.IsInvalid(err) {
		t.Fatalf("expected invalid error creating transition without node selection, got: %v", err)
	}

	updated := created.DeepCopy()
	otherNode := "node-b"
	updated.Spec.NodeName = &otherNode
	_, err = client.LifecycleV1alpha1().LifecycleTransitions().Update(ctx, updated, metav1.UpdateOptions{})
	if !apierrors.IsInvalid(err) {
		t.Fatalf("expected invalid error updating immutable node selection, got: %v", err)
	}
}

func TestLifecycleEventSpecImmutability(t *testing.T) {
	ctx := context.Background()
	client, tearDownFn := startLifecycleTestServer(t)
	defer tearDownFn()

	transitionName := "transition-for-event-immutability"
	createTransitionOrDie(t, ctx, client, transitionName)

	event := &lifecyclev1alpha1.LifecycleEvent{
		ObjectMeta: metav1.ObjectMeta{Name: "event-immutability"},
		Spec: lifecyclev1alpha1.LifecycleEventSpec{
			TransitionName: transitionName,
			BindingNode:    "node-a",
		},
	}
	created, err := client.LifecycleV1alpha1().LifecycleEvents().Create(ctx, event, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create lifecycle event: %v", err)
	}

	updated := created.DeepCopy()
	updated.Spec.TransitionName = "different-transition"
	updated.Spec.BindingNode = "node-b"
	_, err = client.LifecycleV1alpha1().LifecycleEvents().Update(ctx, updated, metav1.UpdateOptions{})
	if !apierrors.IsInvalid(err) {
		t.Fatalf("expected invalid error updating immutable lifecycle event spec fields, got: %v", err)
	}
}

func TestLifecycleEventCreateDefaultsPending(t *testing.T) {
	ctx := context.Background()
	client, tearDownFn := startLifecycleTestServer(t)
	defer tearDownFn()

	transitionName := "transition-default-pending"
	createTransitionOrDie(t, ctx, client, transitionName)

	event := &lifecyclev1alpha1.LifecycleEvent{
		ObjectMeta: metav1.ObjectMeta{Name: "event-default-pending"},
		Spec: lifecyclev1alpha1.LifecycleEventSpec{
			TransitionName: transitionName,
			BindingNode:    "node-a",
		},
		Status: lifecyclev1alpha1.LifecycleEventStatus{
			ClaimStatus: lifecyclev1alpha1.LifecycleEventSucceeded,
		},
	}
	created, err := client.LifecycleV1alpha1().LifecycleEvents().Create(ctx, event, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create lifecycle event: %v", err)
	}
	if created.Status.ClaimStatus != lifecyclev1alpha1.LifecycleEventPending {
		t.Fatalf("expected default claimStatus %q, got %q", lifecyclev1alpha1.LifecycleEventPending, created.Status.ClaimStatus)
	}
}

func TestLifecycleEventStatusSubresourcePreservesSpec(t *testing.T) {
	ctx := context.Background()
	client, tearDownFn := startLifecycleTestServer(t)
	defer tearDownFn()

	transitionName := "transition-status-subresource"
	createTransitionOrDie(t, ctx, client, transitionName)

	event := &lifecyclev1alpha1.LifecycleEvent{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "event-status-subresource",
			Labels: map[string]string{"kept": "true"},
		},
		Spec: lifecyclev1alpha1.LifecycleEventSpec{
			TransitionName: transitionName,
			BindingNode:    "node-a",
		},
	}
	created, err := client.LifecycleV1alpha1().LifecycleEvents().Create(ctx, event, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create lifecycle event: %v", err)
	}

	origSpec := created.Spec
	origLabels := map[string]string{}
	for k, v := range created.Labels {
		origLabels[k] = v
	}

	created.Spec.TransitionName = "mutated-via-status"
	created.Labels["mutated"] = "should-not-stick"
	created.Status.ClaimStatus = lifecyclev1alpha1.LifecycleEventClaimed

	statusUpdated, err := client.LifecycleV1alpha1().LifecycleEvents().UpdateStatus(ctx, created, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("update lifecycle event status: %v", err)
	}

	if statusUpdated.Spec != origSpec {
		t.Fatalf("expected spec to be preserved on status update; got %#v, want %#v", statusUpdated.Spec, origSpec)
	}
	if statusUpdated.Status.ClaimStatus != lifecyclev1alpha1.LifecycleEventClaimed {
		t.Fatalf("expected claimStatus %q, got %q", lifecyclev1alpha1.LifecycleEventClaimed, statusUpdated.Status.ClaimStatus)
	}
	if len(statusUpdated.Labels) != len(origLabels) {
		t.Fatalf("expected metadata labels to be preserved on status update; got %#v, want %#v", statusUpdated.Labels, origLabels)
	}
	for k, v := range origLabels {
		if statusUpdated.Labels[k] != v {
			t.Fatalf("expected label %q=%q to be preserved on status update; got %#v", k, v, statusUpdated.Labels)
		}
	}
}

func startLifecycleTestServer(t *testing.T) (clientset.Interface, func()) {
	t.Helper()
	flags := append([]string{}, framework.DefaultTestServerFlags()...)
	flags = append(flags,
		"--feature-gates=SpecializedLifecycleManagement=true",
		fmt.Sprintf("--runtime-config=%s=true", lifecyclev1alpha1.SchemeGroupVersion),
	)
	server := kubeapiservertesting.StartTestServerOrDie(t, nil, flags, framework.SharedEtcd())
	client := clientset.NewForConfigOrDie(server.ClientConfig)
	return client, server.TearDownFn
}

func createTransitionOrDie(t *testing.T, ctx context.Context, client clientset.Interface, name string) {
	t.Helper()
	nodeName := "node-a"
	_, err := client.LifecycleV1alpha1().LifecycleTransitions().Create(ctx, &lifecyclev1alpha1.LifecycleTransition{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: lifecyclev1alpha1.LifecycleTransitionSpec{
			Start:    "drain-started",
			End:      "drain-complete",
			NodeName: &nodeName,
			Driver:   "example.com/driver",
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create transition %q: %v", name, err)
	}
}

func TestLifecycleTransitionNodeSelectorValidation(t *testing.T) {
	ctx := context.Background()
	client, tearDownFn := startLifecycleTestServer(t)
	defer tearDownFn()

	transition := &lifecyclev1alpha1.LifecycleTransition{
		ObjectMeta: metav1.ObjectMeta{Name: "transition-invalid-multiple-selector-terms"},
		Spec: lifecyclev1alpha1.LifecycleTransitionSpec{
			Start:  "drain-started",
			End:    "drain-complete",
			Driver: "example.com/driver",
			NodeSelector: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{}, {}},
			},
		},
	}
	_, err := client.LifecycleV1alpha1().LifecycleTransitions().Create(ctx, transition, metav1.CreateOptions{})
	if !apierrors.IsInvalid(err) {
		t.Fatalf("expected invalid error for nodeSelector with multiple terms, got: %v", err)
	}
}

func TestNodeAuthorizerLifecycleEventListWatchRequiresBindingNodeSelector(t *testing.T) {
	ctx := context.Background()
	flags := append([]string{}, framework.DefaultTestServerFlags()...)
	flags = append(flags,
		"--runtime-config=api/all=true",
		"--authorization-mode=Node,RBAC",
		"--enable-admission-plugins=NodeRestriction",
		"--feature-gates=SpecializedLifecycleManagement=true",
	)
	server := kubeapiservertesting.StartTestServerOrDie(t, nil, flags, framework.SharedEtcd())
	defer server.TearDownFn()

	adminClient := clientset.NewForConfigOrDie(server.ClientConfig)

	nodeA := "node-a"
	nodeB := "node-b"
	if _, err := adminClient.CoreV1().Nodes().Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeA}}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create node %q: %v", nodeA, err)
	}
	if _, err := adminClient.CoreV1().Nodes().Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeB}}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create node %q: %v", nodeB, err)
	}

	transitionName := "transition-nodeauth-binding-selector"
	createTransitionOrDie(t, ctx, adminClient, transitionName)

	events := []*lifecyclev1alpha1.LifecycleEvent{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "event-nodeauth-a"},
			Spec: lifecyclev1alpha1.LifecycleEventSpec{
				TransitionName: transitionName,
				BindingNode:    nodeA,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "event-nodeauth-b"},
			Spec: lifecyclev1alpha1.LifecycleEventSpec{
				TransitionName: transitionName,
				BindingNode:    nodeB,
			},
		},
	}
	for _, ev := range events {
		if _, err := adminClient.LifecycleV1alpha1().LifecycleEvents().Create(ctx, ev, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create lifecycle event %q: %v", ev.Name, err)
		}
	}

	nodeCfg := rest.CopyConfig(server.ClientConfig)
	nodeCfg.Impersonate = rest.ImpersonationConfig{
		UserName: "system:node:" + nodeA,
		Groups:   []string{"system:authenticated", "system:nodes"},
	}
	nodeClient := clientset.NewForConfigOrDie(nodeCfg)

	// list without required bindingNode selector should be forbidden.
	_, err := nodeClient.LifecycleV1alpha1().LifecycleEvents().List(ctx, metav1.ListOptions{})
	if !apierrors.IsForbidden(err) {
		t.Fatalf("expected forbidden list without bindingNode selector, got: %v", err)
	}

	// list with wrong bindingNode selector should be forbidden.
	wrongSelector := fields.OneTermEqualSelector(lifecycle.LifecycleEventSelectorBindingNode, nodeB).String()
	_, err = nodeClient.LifecycleV1alpha1().LifecycleEvents().List(ctx, metav1.ListOptions{FieldSelector: wrongSelector})
	if !apierrors.IsForbidden(err) {
		t.Fatalf("expected forbidden list with wrong bindingNode selector, got: %v", err)
	}

	// list with matching bindingNode selector should be allowed.
	correctSelector := fields.OneTermEqualSelector(lifecycle.LifecycleEventSelectorBindingNode, nodeA).String()
	list, err := nodeClient.LifecycleV1alpha1().LifecycleEvents().List(ctx, metav1.ListOptions{FieldSelector: correctSelector})
	if err != nil {
		t.Fatalf("expected successful list with matching bindingNode selector, got: %v", err)
	}
	if len(list.Items) != 1 || list.Items[0].Spec.BindingNode != nodeA {
		t.Fatalf("expected exactly one event bound to %q, got %#v", nodeA, list.Items)
	}

	// watch without required bindingNode selector should be forbidden.
	_, err = nodeClient.LifecycleV1alpha1().LifecycleEvents().Watch(ctx, metav1.ListOptions{})
	if !apierrors.IsForbidden(err) {
		t.Fatalf("expected forbidden watch without bindingNode selector, got: %v", err)
	}

	// watch with wrong bindingNode selector should be forbidden.
	_, err = nodeClient.LifecycleV1alpha1().LifecycleEvents().Watch(ctx, metav1.ListOptions{FieldSelector: wrongSelector})
	if !apierrors.IsForbidden(err) {
		t.Fatalf("expected forbidden watch with wrong bindingNode selector, got: %v", err)
	}

	// watch with matching bindingNode selector should be allowed.
	w, err := nodeClient.LifecycleV1alpha1().LifecycleEvents().Watch(ctx, metav1.ListOptions{FieldSelector: correctSelector})
	if err != nil {
		t.Fatalf("expected successful watch with matching bindingNode selector, got: %v", err)
	}
	w.Stop()
}

func TestNodeAuthorizerLifecycleTransitionDeleteCollectionRequiresNodeNameSelector(t *testing.T) {
	ctx := context.Background()
	flags := append([]string{}, framework.DefaultTestServerFlags()...)
	flags = append(flags,
		"--runtime-config=api/all=true",
		"--authorization-mode=Node,RBAC",
		"--enable-admission-plugins=NodeRestriction",
		"--feature-gates=SpecializedLifecycleManagement=true",
	)
	server := kubeapiservertesting.StartTestServerOrDie(t, nil, flags, framework.SharedEtcd())
	defer server.TearDownFn()

	adminClient := clientset.NewForConfigOrDie(server.ClientConfig)

	nodeA := "node-a"
	nodeB := "node-b"
	if _, err := adminClient.CoreV1().Nodes().Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeA}}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create node %q: %v", nodeA, err)
	}
	if _, err := adminClient.CoreV1().Nodes().Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeB}}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create node %q: %v", nodeB, err)
	}

	driver := "example.com/driver"
	transitionAName := "transition-nodeauth-deletecollection-a"
	transitionBName := "transition-nodeauth-deletecollection-b"
	if _, err := adminClient.LifecycleV1alpha1().LifecycleTransitions().Create(ctx, &lifecyclev1alpha1.LifecycleTransition{
		ObjectMeta: metav1.ObjectMeta{Name: transitionAName},
		Spec: lifecyclev1alpha1.LifecycleTransitionSpec{
			Start:    "drain-started",
			End:      "drain-complete",
			NodeName: &nodeA,
			Driver:   driver,
		},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create transition %q: %v", transitionAName, err)
	}
	if _, err := adminClient.LifecycleV1alpha1().LifecycleTransitions().Create(ctx, &lifecyclev1alpha1.LifecycleTransition{
		ObjectMeta: metav1.ObjectMeta{Name: transitionBName},
		Spec: lifecyclev1alpha1.LifecycleTransitionSpec{
			Start:    "drain-started",
			End:      "drain-complete",
			NodeName: &nodeB,
			Driver:   driver,
		},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create transition %q: %v", transitionBName, err)
	}

	nodeCfg := rest.CopyConfig(server.ClientConfig)
	nodeCfg.Impersonate = rest.ImpersonationConfig{
		UserName: "system:node:" + nodeA,
		Groups:   []string{"system:authenticated", "system:nodes"},
	}
	nodeClient := clientset.NewForConfigOrDie(nodeCfg)

	// deletecollection without required nodeName field selector should be forbidden.
	err := nodeClient.LifecycleV1alpha1().LifecycleTransitions().DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})
	if !apierrors.IsForbidden(err) {
		t.Fatalf("expected forbidden deletecollection without nodeName selector, got: %v", err)
	}

	// deletecollection with wrong nodeName selector should be forbidden.
	wrongSelector := fields.OneTermEqualSelector(lifecycle.LifecycleTransitionSelectorNodeName, nodeB).String()
	err = nodeClient.LifecycleV1alpha1().LifecycleTransitions().DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{
		FieldSelector: wrongSelector,
	})
	if !apierrors.IsForbidden(err) {
		t.Fatalf("expected forbidden deletecollection with wrong nodeName selector, got: %v", err)
	}

	// deletecollection with matching nodeName selector should be allowed.
	correctSelector := fields.OneTermEqualSelector(lifecycle.LifecycleTransitionSelectorNodeName, nodeA).String()
	err = nodeClient.LifecycleV1alpha1().LifecycleTransitions().DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{
		FieldSelector: correctSelector,
	})
	if err != nil {
		t.Fatalf("expected successful deletecollection with matching nodeName selector, got: %v", err)
	}

	_, err = adminClient.LifecycleV1alpha1().LifecycleTransitions().Get(ctx, transitionAName, metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected transition %q to be deleted, got: %v", transitionAName, err)
	}
	_, err = adminClient.LifecycleV1alpha1().LifecycleTransitions().Get(ctx, transitionBName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("expected transition %q to remain, got: %v", transitionBName, err)
	}
}

func TestLifecycleEventStaysPendingWhenNoNodeClaims(t *testing.T) {
	ctx := context.Background()
	client, tearDownFn := startLifecycleTestServer(t)
	defer tearDownFn()

	transitionName := "transition-skew-pending"
	createTransitionOrDie(t, ctx, client, transitionName)

	eventName := "event-skew-pending"
	created, err := client.LifecycleV1alpha1().LifecycleEvents().Create(ctx, &lifecyclev1alpha1.LifecycleEvent{
		ObjectMeta: metav1.ObjectMeta{Name: eventName},
		Spec: lifecyclev1alpha1.LifecycleEventSpec{
			TransitionName: transitionName,
			BindingNode:    "node-a",
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create lifecycle event: %v", err)
	}
	if created.Status.ClaimStatus != lifecyclev1alpha1.LifecycleEventPending {
		t.Fatalf("expected initial claimStatus %q, got %q", lifecyclev1alpha1.LifecycleEventPending, created.Status.ClaimStatus)
	}

	for i := 0; i < 3; i++ {
		time.Sleep(2 * time.Second)
		ev, err := client.LifecycleV1alpha1().LifecycleEvents().Get(ctx, eventName, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("get lifecycle event (%d): %v", i, err)
		}
		if ev.Status.ClaimStatus != lifecyclev1alpha1.LifecycleEventPending {
			t.Fatalf("expected claimStatus to remain %q, got %q", lifecyclev1alpha1.LifecycleEventPending, ev.Status.ClaimStatus)
		}
		if ev.Status.Driver != "" {
			t.Fatalf("expected empty driver while event is unclaimed, got %q", ev.Status.Driver)
		}
	}
}
