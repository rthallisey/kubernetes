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

// Package lifecycleevent implements a reconciler that watches
// LifecycleEvent objects bound to this node via an informer cache.
// It claims at most one event at a time and drives it through the
// Start to End transition by calling the registered SLM driver's gRPC
// methods and verifying progress via the Node's LifecycleTransition
// condition.
package lifecycleevent

import (
	"context"
	"fmt"
	"time"

	lifecycleapi "k8s.io/api/lifecycle/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	slmpbv1alpha1 "k8s.io/kubelet/pkg/apis/slm/v1alpha1"
	slmplugin "k8s.io/kubernetes/pkg/kubelet/cm/slm/plugin"
)

const (
	// reconcileInterval is how often the reconciler is invoked.
	reconcileInterval = 10 * time.Second
)

// PluginGetter is the interface used by the reconciler to look up a
// registered SLM driver plugin by name.
type PluginGetter interface {
	GetPlugin(driverName string) (*slmplugin.SLMPlugin, error)
}

// Reconciler watches LifecycleEvents via an informer and periodically
// claims and drives events for a single node.
type Reconciler struct {
	nodeName     string
	kubeClient   kubernetes.Interface
	pluginGetter PluginGetter

	// store is the informer-backed cache of LifecycleEvent objects.
	store cache.Store
}

// NewReconciler creates a new LifecycleEvent reconciler for the given node.
func NewReconciler(nodeName string, kubeClient kubernetes.Interface, pluginGetter PluginGetter) *Reconciler {
	return &Reconciler{
		nodeName:     nodeName,
		kubeClient:   kubeClient,
		pluginGetter: pluginGetter,
	}
}

// Run starts the LifecycleEvent informer, waits for it to sync, and then
// enters the periodic reconcile loop. It blocks until ctx is cancelled.
func (r *Reconciler) Run(ctx context.Context) {
	logger := klog.FromContext(ctx).WithName("lifecycle-event-reconciler")
	logger.Info("Starting LifecycleEvent reconciler", "nodeName", r.nodeName, "interval", reconcileInterval)

	if err := r.initInformer(ctx); err != nil {
		logger.Error(err, "Failed to initialize LifecycleEvent informer, reconciler will not run")
		return
	}

	wait.UntilWithContext(ctx, func(ctx context.Context) {
		if err := r.reconcile(ctx); err != nil {
			logger.Error(err, "LifecycleEvent reconcile error")
		}
	}, reconcileInterval)
	logger.Info("LifecycleEvent reconciler stopped")
}

// initInformer sets up a shared informer that watches all LifecycleEvents
// and populates the local cache. Filtering by bindingNode is done
// client-side from the cache since there is no server-side field selector
// registered for spec.bindingNode.
func (r *Reconciler) initInformer(ctx context.Context) error {
	logger := klog.FromContext(ctx).WithName("lifecycle-event-reconciler")

	eventClient := r.kubeClient.LifecycleV1alpha1().LifecycleEvents()

	informer := cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListWithContextFunc: func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
				list, err := eventClient.List(ctx, options)
				if err == nil {
					logger.V(5).Info("Listed LifecycleEvents", "numItems", len(list.Items))
				}
				return list, err
			},
			WatchFuncWithContext: func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
				w, err := eventClient.Watch(ctx, options)
				logger.V(5).Info("Started watching LifecycleEvents", "err", err)
				return w, err
			},
		},
		&lifecycleapi.LifecycleEvent{},
		0, // no resync — the periodic reconcile loop handles re-evaluation
		cache.Indexers{},
	)
	r.store = informer.GetStore()

	handler, err := informer.AddEventHandlerWithOptions(
		cache.ResourceEventHandlerFuncs{},
		cache.HandlerOptions{Logger: &logger},
	)
	if err != nil {
		return fmt.Errorf("registering event handler on the LifecycleEvent informer: %w", err)
	}

	logger.V(3).Info("Starting LifecycleEvent informer and waiting for sync")
	go informer.RunWithContext(ctx)

	for !handler.HasSynced() {
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return fmt.Errorf("sync LifecycleEvent informer: %w", context.Cause(ctx))
		}
	}
	logger.V(3).Info("LifecycleEvent informer has synced")
	return nil
}

// reconcile is called periodically by wait.UntilWithContext.
//
// How it works:
//  1. Read all LifecycleEvents from the informer cache and filter for bindingNode == this node.
//  2. Look for an already-Claimed event. If found, drive it (only one at a time).
//  3. If no Claimed event exists, look for a Pending event and claim it.
func (r *Reconciler) reconcile(ctx context.Context) error {
	logger := klog.FromContext(ctx).WithName("lifecycle-event-reconciler")

	// Read all events from the informer cache (no API call).
	var claimed *lifecycleapi.LifecycleEvent
	var pending *lifecycleapi.LifecycleEvent

	for _, obj := range r.store.List() {
		ev, ok := obj.(*lifecycleapi.LifecycleEvent)
		if !ok {
			continue
		}
		logger.V(5).Info("Evaluating LifecycleEvent from cache",
			"event", ev.Name,
			"bindingNode", ev.Spec.BindingNode,
			"claimStatus", ev.Status.ClaimStatus,
		)
		if ev.Spec.BindingNode != r.nodeName {
			continue
		}

		switch ev.Status.ClaimStatus {
		case lifecycleapi.LifecycleEventClaimed:
			if claimed == nil {
				claimed = ev
			}
		case lifecycleapi.LifecycleEventPending, "":
			if pending == nil {
				pending = ev
			}
		}
	}

	// If there's an already Claimed event, drive that event to an end state.
	if claimed != nil {
		return r.driveClaimedEvent(ctx, claimed)
	}

	// No Claimed event, so try to claim a Pending one.
	if pending != nil {
		logger.V(3).Info("Found Pending LifecycleEvent, attempting to claim",
			"event", pending.Name,
			"transition", pending.Spec.TransitionName,
		)
		return r.claimEvent(ctx, pending)
	}

	logger.V(5).Info("No actionable LifecycleEvents for this node")
	return nil
}

// claimEvent transitions a Pending LifecycleEvent to Claimed.
func (r *Reconciler) claimEvent(ctx context.Context, ev *lifecycleapi.LifecycleEvent) error {
	logger := klog.FromContext(ctx).WithName("lifecycle-event-reconciler")

	// Look up the associated LifecycleTransition to get the driver name.
	transition, err := r.kubeClient.LifecycleV1alpha1().LifecycleTransitions().Get(ctx, ev.Spec.TransitionName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get LifecycleTransition %q for event %q: %w", ev.Spec.TransitionName, ev.Name, err)
	}

	driverName := transition.Spec.Driver
	logger.V(3).Info("Resolved LifecycleTransition for event",
		"event", ev.Name,
		"transition", transition.Name,
		"driver", driverName,
	)

	// Verify a driver is registered for this transition.
	_, err = r.pluginGetter.GetPlugin(driverName)
	if err != nil {
		logger.V(3).Info("No registered driver for transition, skipping claim",
			"event", ev.Name,
			"driver", driverName,
			"err", err,
		)
		return nil
	}

	// DeepCopy to avoid mutating the informer cache.
	updated := ev.DeepCopy()
	updated.Status.ClaimStatus = lifecycleapi.LifecycleEventClaimed
	updated.Status.Driver = driverName

	// Calculate SLA deadline if the transition has one.
	if transition.Spec.Sla != nil {
		deadline := metav1.NewTime(time.Now().Add(transition.Spec.Sla.Duration))
		updated.Status.Sla = &deadline
	}

	logger.V(3).Info("Updating LifecycleEvent status to Claimed",
		"event", updated.Name,
		"driver", updated.Status.Driver,
		"claimStatus", updated.Status.ClaimStatus,
	)

	_, err = r.kubeClient.LifecycleV1alpha1().LifecycleEvents().UpdateStatus(ctx, updated, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("update LifecycleEvent %q status to Claimed: %w", ev.Name, err)
	}

	logger.Info("Claimed LifecycleEvent",
		"event", ev.Name,
		"transition", ev.Spec.TransitionName,
		"driver", driverName,
	)
	return nil
}

// driveClaimedEvent checks the Node condition and calls the appropriate gRPC
// method (Start or End) on the driver.
//
// summary:
//   - If the Node condition reason == end string, then mark Succeeded (transition complete).
//   - If the Node condition reason == start string, then call EndLifecycleTransition.
//   - Otherwise condition is missing/different, then call StartLifecycleTransition.
func (r *Reconciler) driveClaimedEvent(ctx context.Context, ev *lifecycleapi.LifecycleEvent) error {
	logger := klog.FromContext(ctx).WithName("lifecycle-event-reconciler")

	// Look up the associated LifecycleTransition.
	transition, err := r.kubeClient.LifecycleV1alpha1().LifecycleTransitions().Get(ctx, ev.Spec.TransitionName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get LifecycleTransition %q for claimed event %q: %w", ev.Spec.TransitionName, ev.Name, err)
	}

	startState := transition.Spec.Start
	endState := transition.Spec.End
	driverName := ev.Status.Driver

	// Read the current Node condition.
	conditionReason, err := r.getNodeConditionReason(ctx)
	if err != nil {
		return fmt.Errorf("get Node condition for %q: %w", r.nodeName, err)
	}

	logger.V(3).Info("Evaluating claimed LifecycleEvent",
		"event", ev.Name,
		"transition", ev.Spec.TransitionName,
		"driver", driverName,
		"conditionReason", conditionReason,
		"startState", startState,
		"endState", endState,
	)

	switch conditionReason {
	case endState:
		// The transition reached the end state — mark as Succeeded and clean up.
		logger.Info("Transition reached end state, marking Succeeded",
			"event", ev.Name,
			"endState", endState,
		)
		return r.succeedEvent(ctx, ev)

	case startState:
		// The start state has been published — call EndLifecycleTransition.
		logger.V(3).Info("Node condition matches start state, calling EndLifecycleTransition",
			"event", ev.Name,
			"startState", startState,
		)
		return r.callEnd(ctx, ev, transition, driverName)

	default:
		// Condition not set or does not match start — call StartLifecycleTransition.
		logger.V(3).Info("Node condition does not match start state, calling StartLifecycleTransition",
			"event", ev.Name,
			"conditionReason", conditionReason,
			"startState", startState,
		)
		return r.callStart(ctx, ev, transition, driverName)
	}
}

// callStart invokes the driver's StartLifecycleTransition gRPC.
func (r *Reconciler) callStart(ctx context.Context, ev *lifecycleapi.LifecycleEvent, transition *lifecycleapi.LifecycleTransition, driverName string) error {
	plugin, err := r.pluginGetter.GetPlugin(driverName)
	if err != nil {
		return fmt.Errorf("get SLM plugin %q: %w", driverName, err)
	}

	req := &slmpbv1alpha1.StartLifecycleTransitionRequest{
		TransitionName: transition.Name,
		EventName:      ev.Name,
		NodeName:       r.nodeName,
		Start:          transition.Spec.Start,
		End:            transition.Spec.End,
	}

	_, err = plugin.StartLifecycleTransition(ctx, req)
	if err != nil {
		return fmt.Errorf("StartLifecycleTransition gRPC for event %q: %w", ev.Name, err)
	}

	return nil
}

// callEnd invokes the driver's EndLifecycleTransition gRPC.
func (r *Reconciler) callEnd(ctx context.Context, ev *lifecycleapi.LifecycleEvent, transition *lifecycleapi.LifecycleTransition, driverName string) error {
	plugin, err := r.pluginGetter.GetPlugin(driverName)
	if err != nil {
		return fmt.Errorf("get SLM plugin %q: %w", driverName, err)
	}

	req := &slmpbv1alpha1.EndLifecycleTransitionRequest{
		TransitionName: transition.Name,
		EventName:      ev.Name,
		NodeName:       r.nodeName,
		Start:          transition.Spec.Start,
		End:            transition.Spec.End,
	}

	_, err = plugin.EndLifecycleTransition(ctx, req)
	if err != nil {
		return fmt.Errorf("EndLifecycleTransition gRPC for event %q: %w", ev.Name, err)
	}

	return nil
}

// succeedEvent transitions the LifecycleEvent to Succeeded and deletes it.
func (r *Reconciler) succeedEvent(ctx context.Context, ev *lifecycleapi.LifecycleEvent) error {
	logger := klog.FromContext(ctx).WithName("lifecycle-event-reconciler")

	// DeepCopy to avoid mutating the informer cache.
	updated := ev.DeepCopy()
	updated.Status.ClaimStatus = lifecycleapi.LifecycleEventSucceeded
	_, err := r.kubeClient.LifecycleV1alpha1().LifecycleEvents().UpdateStatus(ctx, updated, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("update LifecycleEvent %q status to Succeeded: %w", ev.Name, err)
	}

	// Delete the event (garbage collection per KEP).
	err = r.kubeClient.LifecycleV1alpha1().LifecycleEvents().Delete(ctx, ev.Name, metav1.DeleteOptions{})
	if err != nil {
		return fmt.Errorf("delete completed LifecycleEvent %q: %w", ev.Name, err)
	}

	logger.Info("LifecycleEvent completed and deleted", "event", ev.Name)
	return nil
}

// getNodeConditionReason returns the Reason field of the LifecycleTransition
// condition on this node, or "" if the condition is not present.
func (r *Reconciler) getNodeConditionReason(ctx context.Context) (string, error) {
	node, err := r.kubeClient.CoreV1().Nodes().Get(ctx, r.nodeName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("get node %q: %w", r.nodeName, err)
	}

	for _, c := range node.Status.Conditions {
		if c.Type == slmplugin.LifecycleTransitionConditionType {
			return c.Reason, nil
		}
	}

	return "", nil
}
