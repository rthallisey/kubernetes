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
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	lifecycleapi "k8s.io/api/lifecycle/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/apis/lifecycle"
	slmplugin "k8s.io/kubernetes/pkg/kubelet/cm/slm/plugin"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testNodeName  = "node-a"
	testDriver    = "test-driver.slm.k8s.io"
	testEventName = "event-a"
	testTransName = "transition-a"
)

type fakePluginGetter struct {
	plugin *slmplugin.SLMPlugin
	err    error
	calls  int
}

func (f *fakePluginGetter) GetPlugin(driverName string) (*slmplugin.SLMPlugin, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return f.plugin, nil
}

func TestClaimEventClaimsAndAddsFinalizer(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset(
		newNode(testNodeName, map[string]string{"role": "worker"}),
		newTransition(testTransName, testDriver, ptrTo(testNodeName), nil, nil, nil),
		newEvent(testEventName, testTransName, testNodeName, lifecycleapi.LifecycleEventPending, nil, nil),
	)

	pg := &fakePluginGetter{plugin: &slmplugin.SLMPlugin{}}
	r := NewReconciler(testNodeName, client, pg)

	event, err := client.LifecycleV1alpha1().LifecycleEvents().Get(ctx, testEventName, metav1.GetOptions{})
	require.NoError(t, err)

	err = r.claimEvent(ctx, event)
	require.NoError(t, err)
	assert.Equal(t, 1, pg.calls)

	updated, err := client.LifecycleV1alpha1().LifecycleEvents().Get(ctx, testEventName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, lifecycleapi.LifecycleEventClaimed, updated.Status.ClaimStatus)
	assert.Equal(t, testDriver, updated.Status.Driver)
	assert.Contains(t, updated.Finalizers, lifecycleEventFinalizer)
}

func TestClaimEventFailsWhenRegisteredDriverIsNotReady(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset(
		newNode(testNodeName, nil),
		newTransition(testTransName, testDriver, ptrTo(testNodeName), nil, nil, nil),
		newEvent(testEventName, testTransName, testNodeName, lifecycleapi.LifecycleEventPending, nil, nil),
	)

	pg := &fakePluginGetter{plugin: &slmplugin.SLMPlugin{}}
	r := NewReconciler(testNodeName, client, pg)
	r.waitForPluginReady = func(ctx context.Context, plugin *slmplugin.SLMPlugin) error {
		return errors.New("driver heartbeat timeout")
	}

	event, err := client.LifecycleV1alpha1().LifecycleEvents().Get(ctx, testEventName, metav1.GetOptions{})
	require.NoError(t, err)
	err = r.claimEvent(ctx, event)
	require.NoError(t, err)
	assert.Equal(t, 1, pg.calls)

	_, err = client.LifecycleV1alpha1().LifecycleEvents().Get(ctx, testEventName, metav1.GetOptions{})
	require.Error(t, err)
	assert.True(t, apierrors.IsNotFound(err), "expected event deletion after Failed terminal state")
}

func TestClaimEventNoDriverSkipsThisCycleAndStaysPending(t *testing.T) {
	ctx := context.Background()
	created := metav1.NewTime(time.Now())
	client := fake.NewSimpleClientset(
		newNode(testNodeName, nil),
		newTransition(testTransName, testDriver, ptrTo(testNodeName), nil, nil, nil),
		newEvent(testEventName, testTransName, testNodeName, lifecycleapi.LifecycleEventPending, nil, &created),
	)

	pg := &fakePluginGetter{err: errors.New("driver not registered")}
	r := NewReconciler(testNodeName, client, pg)

	event, err := client.LifecycleV1alpha1().LifecycleEvents().Get(ctx, testEventName, metav1.GetOptions{})
	require.NoError(t, err)

	err = r.claimEvent(ctx, event)
	require.NoError(t, err)
	assert.Equal(t, 1, pg.calls)

	updated, err := client.LifecycleV1alpha1().LifecycleEvents().Get(ctx, testEventName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, lifecycleapi.LifecycleEventPending, updated.Status.ClaimStatus)
	assert.Empty(t, updated.Status.Driver)
	assert.NotContains(t, updated.Finalizers, lifecycleEventFinalizer)
}

func TestReconcilePendingEventNoDriverStaysPending(t *testing.T) {
	ctx := context.Background()
	created := metav1.NewTime(time.Now())
	client := fake.NewSimpleClientset(
		newNode(testNodeName, nil),
		newTransition(testTransName, testDriver, ptrTo(testNodeName), nil, nil, nil),
		newEvent(testEventName, testTransName, testNodeName, lifecycleapi.LifecycleEventPending, nil, &created),
	)

	pg := &fakePluginGetter{err: errors.New("driver not registered")}
	r := NewReconciler(testNodeName, client, pg)
	r.store = cache.NewStore(cache.MetaNamespaceKeyFunc)
	require.NoError(t, syncStoreFromAPI(ctx, r, client))

	require.NoError(t, r.reconcile(ctx))
	assert.Equal(t, 1, pg.calls)

	updated, err := client.LifecycleV1alpha1().LifecycleEvents().Get(ctx, testEventName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, lifecycleapi.LifecycleEventPending, updated.Status.ClaimStatus)
	assert.Empty(t, updated.Status.Driver)
	assert.NotContains(t, updated.Finalizers, lifecycleEventFinalizer)
}

func TestClaimEventDoesNotFailWhenTransitionIsForDifferentNode(t *testing.T) {
	ctx := context.Background()
	otherNode := "node-b"
	client := fake.NewSimpleClientset(
		newNode(testNodeName, map[string]string{"role": "worker"}),
		newTransition(testTransName, testDriver, &otherNode, nil, nil, nil),
		newEvent(testEventName, testTransName, testNodeName, lifecycleapi.LifecycleEventPending, nil, nil),
	)

	pg := &fakePluginGetter{plugin: &slmplugin.SLMPlugin{}}
	r := NewReconciler(testNodeName, client, pg)

	event, err := client.LifecycleV1alpha1().LifecycleEvents().Get(ctx, testEventName, metav1.GetOptions{})
	require.NoError(t, err)
	err = r.claimEvent(ctx, event)
	require.NoError(t, err)
	assert.Equal(t, 1, pg.calls)

	updated, err := client.LifecycleV1alpha1().LifecycleEvents().Get(ctx, testEventName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, lifecycleapi.LifecycleEventClaimed, updated.Status.ClaimStatus)
}

func TestDriveClaimedEventEnsuresFinalizer(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset(
		newNode(testNodeName, nil),
		newTransition(testTransName, testDriver, ptrTo(testNodeName), nil, nil, nil),
		newEvent(testEventName, testTransName, testNodeName, lifecycleapi.LifecycleEventClaimed, nil, nil),
	)

	pg := &fakePluginGetter{err: errors.New("driver missing")}
	r := NewReconciler(testNodeName, client, pg)

	event, err := client.LifecycleV1alpha1().LifecycleEvents().Get(ctx, testEventName, metav1.GetOptions{})
	require.NoError(t, err)
	err = r.driveClaimedEvent(ctx, event)
	require.NoError(t, err)
	assert.Equal(t, 1, pg.calls)

	_, getErr := client.LifecycleV1alpha1().LifecycleEvents().Get(ctx, testEventName, metav1.GetOptions{})
	require.Error(t, getErr)
	assert.True(t, apierrors.IsNotFound(getErr), "expected event deletion after callback failure")
}

func TestDriveClaimedEventSLAExpiredDeletesEvent(t *testing.T) {
	ctx := context.Background()
	expired := metav1.NewTime(time.Now().Add(-time.Minute))
	client := fake.NewSimpleClientset(
		newNode(testNodeName, nil),
		newEvent(testEventName, testTransName, testNodeName, lifecycleapi.LifecycleEventClaimed, &expired, nil),
	)

	pg := &fakePluginGetter{plugin: &slmplugin.SLMPlugin{}}
	r := NewReconciler(testNodeName, client, pg)

	event, err := client.LifecycleV1alpha1().LifecycleEvents().Get(ctx, testEventName, metav1.GetOptions{})
	require.NoError(t, err)
	err = r.driveClaimedEvent(ctx, event)
	require.NoError(t, err)

	_, err = client.LifecycleV1alpha1().LifecycleEvents().Get(ctx, testEventName, metav1.GetOptions{})
	require.Error(t, err)
	assert.True(t, apierrors.IsNotFound(err), "expected event deletion after SlaExpired terminal state")
	assert.Equal(t, 0, pg.calls, "driver should not be invoked when SLA has already expired")
}

func TestReconcileMultipleEventsOneAtATime(t *testing.T) {
	ctx := context.Background()
	endState := "drain-complete"

	client := fake.NewSimpleClientset(
		newNodeWithCondition(testNodeName, nil, ""),
		newTransition("transition-1", testDriver, ptrTo(testNodeName), nil, nil, nil),
		newTransition("transition-2", testDriver, ptrTo(testNodeName), nil, nil, nil),
		newEvent("event-1", "transition-1", testNodeName, lifecycleapi.LifecycleEventPending, nil, nil),
		newEvent("event-2", "transition-2", testNodeName, lifecycleapi.LifecycleEventPending, nil, nil),
	)
	// Keep end states identical so node condition can satisfy whichever event is claimed.
	require.NoError(t, patchTransitionEndState(ctx, client, "transition-1", endState))
	require.NoError(t, patchTransitionEndState(ctx, client, "transition-2", endState))

	pg := &fakePluginGetter{plugin: &slmplugin.SLMPlugin{}}
	r := NewReconciler(testNodeName, client, pg)
	r.store = cache.NewStore(cache.MetaNamespaceKeyFunc)
	require.NoError(t, syncStoreFromAPI(ctx, r, client))

	// First reconcile: claim one event.
	require.NoError(t, r.reconcile(ctx))
	require.Equal(t, map[lifecycleapi.LifecycleEventClaimStatus]int{
		lifecycleapi.LifecycleEventClaimed: 1,
		lifecycleapi.LifecycleEventPending: 1,
	}, countEventStatuses(ctx, t, client))

	// Second reconcile: keep working on the claimed event. Callback failures now
	// transition the claimed event to Failed and delete it.
	require.NoError(t, syncStoreFromAPI(ctx, r, client))
	err := r.reconcile(ctx)
	require.NoError(t, err)
	require.Equal(t, map[lifecycleapi.LifecycleEventClaimStatus]int{
		lifecycleapi.LifecycleEventPending: 1,
	}, countEventStatuses(ctx, t, client))

	// Third reconcile: with no claimed event left, kubelet claims the remaining one.
	require.NoError(t, syncStoreFromAPI(ctx, r, client))
	require.NoError(t, r.reconcile(ctx))
	require.Equal(t, map[lifecycleapi.LifecycleEventClaimStatus]int{
		lifecycleapi.LifecycleEventClaimed: 1,
	}, countEventStatuses(ctx, t, client))
}

func TestReconcileCleansUpTerminalEvent(t *testing.T) {
	ctx := context.Background()
	ev := newEvent(testEventName, testTransName, testNodeName, lifecycleapi.LifecycleEventFailed, nil, nil)
	ev.Finalizers = []string{lifecycleEventFinalizer}

	client := fake.NewSimpleClientset(
		newNode(testNodeName, nil),
		ev,
	)

	r := NewReconciler(testNodeName, client, &fakePluginGetter{})
	r.store = cache.NewStore(cache.MetaNamespaceKeyFunc)
	require.NoError(t, syncStoreFromAPI(ctx, r, client))

	require.NoError(t, r.reconcile(ctx))

	_, err := client.LifecycleV1alpha1().LifecycleEvents().Get(ctx, testEventName, metav1.GetOptions{})
	require.Error(t, err)
	assert.True(t, apierrors.IsNotFound(err), "expected terminal event to be deleted by cleanup reconcile")
}

func TestInitInformerUsesBindingNodeFieldSelector(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()

	client := fake.NewSimpleClientset(
		newEvent(testEventName, testTransName, testNodeName, lifecycleapi.LifecycleEventPending, nil, nil),
	)
	r := NewReconciler(testNodeName, client, &fakePluginGetter{})

	expectedSelector := fields.OneTermEqualSelector(lifecycle.LifecycleEventSelectorBindingNode, testNodeName).String()

	var mu sync.Mutex
	listSelectors := []string{}
	watchSelectors := []string{}

	client.PrependReactor("list", "lifecycleevents", func(action k8stesting.Action) (bool, runtime.Object, error) {
		la, ok := action.(k8stesting.ListAction)
		require.True(t, ok)
		mu.Lock()
		listSelectors = append(listSelectors, la.GetListRestrictions().Fields.String())
		mu.Unlock()
		return false, nil, nil
	})

	client.PrependWatchReactor("lifecycleevents", func(action k8stesting.Action) (bool, watch.Interface, error) {
		wa, ok := action.(k8stesting.WatchAction)
		require.True(t, ok)
		mu.Lock()
		watchSelectors = append(watchSelectors, wa.GetWatchRestrictions().Fields.String())
		mu.Unlock()
		return true, watch.NewFake(), nil
	})

	err := r.initInformer(ctx)
	require.Error(t, err)

	mu.Lock()
	if len(listSelectors) > 0 {
		assert.Equal(t, expectedSelector, listSelectors[0])
	}
	mu.Unlock()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(watchSelectors) > 0 && watchSelectors[0] == expectedSelector
	}, time.Second, 10*time.Millisecond)
}

func TestBuildEndRequestSLAExpiredField(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	transition := newTransition(testTransName, testDriver, ptrTo(testNodeName), nil, nil, nil)

	tests := []struct {
		name        string
		slaDeadline *metav1.Time
		expected    bool
	}{
		{
			name:        "no sla",
			slaDeadline: nil,
			expected:    false,
		},
		{
			name:        "future deadline",
			slaDeadline: ptrTo(metav1.NewTime(now.Add(time.Second))),
			expected:    false,
		},
		{
			name:        "exact deadline",
			slaDeadline: ptrTo(metav1.NewTime(now)),
			expected:    true,
		},
		{
			name:        "past deadline",
			slaDeadline: ptrTo(metav1.NewTime(now.Add(-time.Second))),
			expected:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ev := newEvent(testEventName, testTransName, testNodeName, lifecycleapi.LifecycleEventClaimed, tt.slaDeadline, nil)
			req := buildEndRequest(ev, transition, testNodeName, now)
			assert.Equal(t, tt.expected, req.SlaExpired)
		})
	}
}

func TestTerminalCleanupSequence(t *testing.T) {
	tests := []struct {
		name     string
		claim    lifecycleapi.LifecycleEventClaimStatus
		invoke   func(r *Reconciler, ctx context.Context, ev *lifecycleapi.LifecycleEvent) error
		expected []string
	}{
		{
			name:  "succeeded sequence",
			claim: lifecycleapi.LifecycleEventSucceeded,
			invoke: func(r *Reconciler, ctx context.Context, ev *lifecycleapi.LifecycleEvent) error {
				return r.succeedEvent(ctx, ev)
			},
			expected: []string{
				"update/lifecycleevents/status",
				"get/lifecycleevents/",
				"update/lifecycleevents/",
				"delete/lifecycleevents/",
			},
		},
		{
			name:  "failed sequence",
			claim: lifecycleapi.LifecycleEventFailed,
			invoke: func(r *Reconciler, ctx context.Context, ev *lifecycleapi.LifecycleEvent) error {
				return r.failEvent(ctx, ev, testDriver)
			},
			expected: []string{
				"update/lifecycleevents/status",
				"get/lifecycleevents/",
				"update/lifecycleevents/",
				"delete/lifecycleevents/",
			},
		},
		{
			name:  "slaexpired sequence",
			claim: lifecycleapi.LifecycleEventSlaExpired,
			invoke: func(r *Reconciler, ctx context.Context, ev *lifecycleapi.LifecycleEvent) error {
				return r.slaExpireEvent(ctx, ev)
			},
			expected: []string{
				"update/lifecycleevents/status",
				"get/lifecycleevents/",
				"update/lifecycleevents/",
				"delete/lifecycleevents/",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			ev := newEvent(testEventName, testTransName, testNodeName, lifecycleapi.LifecycleEventClaimed, nil, nil)
			ev.Finalizers = []string{lifecycleEventFinalizer}
			client := fake.NewSimpleClientset(ev)
			r := NewReconciler(testNodeName, client, &fakePluginGetter{})

			gotEv, err := client.LifecycleV1alpha1().LifecycleEvents().Get(ctx, testEventName, metav1.GetOptions{})
			require.NoError(t, err)
			client.ClearActions()
			err = tt.invoke(r, ctx, gotEv)
			require.NoError(t, err)

			assert.Equal(t, tt.expected, actionSummaries(client.Actions()))
		})
	}
}

func newNode(name string, labels map[string]string) *v1.Node {
	return &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: labels,
		},
	}
}

func newTransition(name, driver string, nodeName *string, selector *v1.NodeSelector, allNodes *bool, sla *metav1.Duration) *lifecycleapi.LifecycleTransition {
	return &lifecycleapi.LifecycleTransition{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: lifecycleapi.LifecycleTransitionSpec{
			Start:        "drain-started",
			End:          "drain-complete",
			NodeName:     nodeName,
			NodeSelector: selector,
			AllNodes:     allNodes,
			Sla:          sla,
			Driver:       driver,
		},
	}
}

func newEvent(name, transitionName, bindingNode string, status lifecycleapi.LifecycleEventClaimStatus, sla *metav1.Time, created *metav1.Time) *lifecycleapi.LifecycleEvent {
	event := &lifecycleapi.LifecycleEvent{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: lifecycleapi.LifecycleEventSpec{
			TransitionName: transitionName,
			BindingNode:    bindingNode,
		},
		Status: lifecycleapi.LifecycleEventStatus{
			ClaimStatus: status,
		},
	}
	if sla != nil {
		event.Status.Sla = sla
	}
	if created != nil {
		event.CreationTimestamp = *created
	}
	return event
}

func ptrTo[T any](v T) *T {
	return &v
}

func actionSummaries(actions []k8stesting.Action) []string {
	out := make([]string, 0, len(actions))
	for _, a := range actions {
		out = append(out, fmt.Sprintf("%s/%s/%s", a.GetVerb(), a.GetResource().Resource, a.GetSubresource()))
	}
	return out
}

func newNodeWithCondition(name string, labels map[string]string, reason string) *v1.Node {
	node := newNode(name, labels)
	if reason != "" {
		node.Status.Conditions = []v1.NodeCondition{
			{
				Type:   slmplugin.LifecycleTransitionConditionType,
				Status: v1.ConditionTrue,
				Reason: reason,
			},
		}
	}
	return node
}

func setNodeConditionReason(ctx context.Context, client *fake.Clientset, nodeName, reason string) error {
	node, err := client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	for i := range node.Status.Conditions {
		if node.Status.Conditions[i].Type == slmplugin.LifecycleTransitionConditionType {
			node.Status.Conditions[i].Reason = reason
			_, err = client.CoreV1().Nodes().UpdateStatus(ctx, node, metav1.UpdateOptions{})
			return err
		}
	}
	node.Status.Conditions = append(node.Status.Conditions, v1.NodeCondition{
		Type:   slmplugin.LifecycleTransitionConditionType,
		Status: v1.ConditionTrue,
		Reason: reason,
	})
	_, err = client.CoreV1().Nodes().UpdateStatus(ctx, node, metav1.UpdateOptions{})
	return err
}

func patchTransitionEndState(ctx context.Context, client *fake.Clientset, transitionName, end string) error {
	transition, err := client.LifecycleV1alpha1().LifecycleTransitions().Get(ctx, transitionName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	transition.Spec.End = end
	_, err = client.LifecycleV1alpha1().LifecycleTransitions().Update(ctx, transition, metav1.UpdateOptions{})
	return err
}

func syncStoreFromAPI(ctx context.Context, r *Reconciler, client *fake.Clientset) error {
	list, err := client.LifecycleV1alpha1().LifecycleEvents().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	items := make([]interface{}, 0, len(list.Items))
	for i := range list.Items {
		items = append(items, list.Items[i].DeepCopy())
	}
	return r.store.Replace(items, "0")
}

func countEventStatuses(ctx context.Context, t *testing.T, client *fake.Clientset) map[lifecycleapi.LifecycleEventClaimStatus]int {
	t.Helper()
	list, err := client.LifecycleV1alpha1().LifecycleEvents().List(ctx, metav1.ListOptions{})
	require.NoError(t, err)

	counts := map[lifecycleapi.LifecycleEventClaimStatus]int{}
	for i := range list.Items {
		counts[list.Items[i].Status.ClaimStatus]++
	}
	return counts
}
