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
	"slices"
	"time"

	lifecycleapi "k8s.io/api/lifecycle/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
	slmpbv1alpha1 "k8s.io/kubelet/pkg/apis/slm/v1alpha1"
	"k8s.io/kubernetes/pkg/apis/lifecycle"
	slmplugin "k8s.io/kubernetes/pkg/kubelet/cm/slm/plugin"
)

const (
	// reconcileInterval is how often the reconciler is invoked.
	reconcileInterval = 10 * time.Second

	// claimFailureTimeout is how long a registered driver has to become ready
	// during claim before the event is marked as Failed.
	claimFailureTimeout = 30 * time.Second

	// lifecycleEventFinalizer is added after claim so kubelet can control
	// terminal-state cleanup.
	lifecycleEventFinalizer = "lifecycle.k8s.io/claim-protection"
)

// PluginGetter is the interface used by the reconciler to look up a
// registered SLM driver plugin by name.
type PluginGetter interface {
	GetPlugin(driverName string) (*slmplugin.SLMPlugin, error)
}

// Reconciler watches LifecycleEvents via an informer and periodically
// claims and drives events for a single node.
type Reconciler struct {
	nodeName           string
	kubeClient         kubernetes.Interface
	pluginGetter       PluginGetter
	now                func() time.Time
	waitForPluginReady func(ctx context.Context, plugin *slmplugin.SLMPlugin) error

	// store is the informer-backed cache of LifecycleEvent objects.
	store cache.Store
}

// NewReconciler creates a new LifecycleEvent reconciler for the given node.
func NewReconciler(nodeName string, kubeClient kubernetes.Interface, pluginGetter PluginGetter) *Reconciler {
	return &Reconciler{
		nodeName:     nodeName,
		kubeClient:   kubeClient,
		pluginGetter: pluginGetter,
		now:          time.Now,
		waitForPluginReady: func(ctx context.Context, plugin *slmplugin.SLMPlugin) error {
			return plugin.WaitForReady(ctx, claimFailureTimeout)
		},
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

// initInformer sets up a shared informer that watches LifecycleEvents bound to
// this node and populates the local cache.
func (r *Reconciler) initInformer(ctx context.Context) error {
	logger := klog.FromContext(ctx).WithName("lifecycle-event-reconciler")

	eventClient := r.kubeClient.LifecycleV1alpha1().LifecycleEvents()
	bindingNodeSelector := fields.OneTermEqualSelector(lifecycle.LifecycleEventSelectorBindingNode, r.nodeName).String()

	informer := cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListWithContextFunc: func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
				options.FieldSelector = bindingNodeSelector
				list, err := eventClient.List(ctx, options)
				if err == nil {
					logger.V(5).Info("Listed LifecycleEvents", "numItems", len(list.Items), "fieldSelector", bindingNodeSelector)
				}
				return list, err
			},
			WatchFuncWithContext: func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
				options.FieldSelector = bindingNodeSelector
				w, err := eventClient.Watch(ctx, options)
				logger.V(5).Info("Started watching LifecycleEvents", "fieldSelector", bindingNodeSelector, "err", err)
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
	var terminal *lifecycleapi.LifecycleEvent

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
		case lifecycleapi.LifecycleEventSucceeded, lifecycleapi.LifecycleEventFailed, lifecycleapi.LifecycleEventSlaExpired:
			if terminal == nil {
				terminal = ev
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

	// Retry terminal cleanup so transient failures in removeFinalizer/delete do
	// not leak terminal events forever.
	if terminal != nil {
		logger.V(3).Info("Found terminal LifecycleEvent, retrying cleanup",
			"event", terminal.Name,
			"claimStatus", terminal.Status.ClaimStatus,
		)
		return r.cleanupTerminalEvent(ctx, terminal)
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
	plugin, err := r.pluginGetter.GetPlugin(driverName)
	if err != nil {
		logger.V(3).Info("No registered driver for transition, skipping claim",
			"event", ev.Name,
			"driver", driverName,
			"err", err,
		)
		return nil
	}

	if err := r.waitForPluginReady(ctx, plugin); err != nil {
		logger.Info("Registered driver did not become ready during claim timeout, marking LifecycleEvent Failed",
			"event", ev.Name,
			"driver", driverName,
			"timeout", claimFailureTimeout,
			"err", err,
		)
		return r.failEvent(ctx, ev, driverName)
	}

	updated, err := r.ensureFinalizer(ctx, ev)
	if err != nil {
		return fmt.Errorf("ensure finalizer on LifecycleEvent %q: %w", ev.Name, err)
	}
	claimed := false
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest, err := r.kubeClient.LifecycleV1alpha1().LifecycleEvents().Get(ctx, updated.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		// Only claim if this event is still Pending.
		if latest.Status.ClaimStatus != lifecycleapi.LifecycleEventPending && latest.Status.ClaimStatus != "" {
			return nil
		}

		statusUpdate := latest.DeepCopy()
		statusUpdate.Status.ClaimStatus = lifecycleapi.LifecycleEventClaimed
		statusUpdate.Status.Driver = driverName

		// Calculate SLA deadline if the transition has one.
		if transition.Spec.Sla != nil {
			deadline := metav1.NewTime(r.now().Add(transition.Spec.Sla.Duration))
			statusUpdate.Status.Sla = &deadline
		}

		logger.V(3).Info("Updating LifecycleEvent status to Claimed (CAS)",
			"event", statusUpdate.Name,
			"driver", statusUpdate.Status.Driver,
			"claimStatus", statusUpdate.Status.ClaimStatus,
		)
		if _, err := r.kubeClient.LifecycleV1alpha1().LifecycleEvents().UpdateStatus(ctx, statusUpdate, metav1.UpdateOptions{}); err != nil {
			return err
		}
		claimed = true
		return nil
	})
	if err != nil {
		return fmt.Errorf("update LifecycleEvent %q status to Claimed: %w", ev.Name, err)
	}

	if claimed {
		logger.Info("Claimed LifecycleEvent",
			"event", ev.Name,
			"transition", ev.Spec.TransitionName,
			"driver", driverName,
		)
	} else {
		logger.V(3).Info("LifecycleEvent no longer Pending; skipping claim",
			"event", ev.Name,
		)
	}
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

	updated, err := r.ensureFinalizer(ctx, ev)
	if err != nil {
		return fmt.Errorf("ensure finalizer on claimed LifecycleEvent %q: %w", ev.Name, err)
	}
	ev = updated

	slaExpired := isSLAExpiredAt(ev, r.now())
	if slaExpired {
		logger.Info("LifecycleEvent SLA expired, marking SlaExpired",
			"event", ev.Name,
			"slaDeadline", ev.Status.Sla,
		)
		return r.slaExpireEvent(ctx, ev)
	}

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
		if err := r.callEnd(ctx, ev, transition, driverName); err != nil {
			logger.Info("EndLifecycleTransition failed, marking LifecycleEvent Failed",
				"event", ev.Name,
				"driver", driverName,
				"err", err,
			)
			return r.failEvent(ctx, ev, driverName)
		}
		return nil

	default:
		// Condition not set or does not match start — call StartLifecycleTransition.
		logger.V(3).Info("Node condition does not match start state, calling StartLifecycleTransition",
			"event", ev.Name,
			"conditionReason", conditionReason,
			"startState", startState,
		)
		if err := r.callStart(ctx, ev, transition, driverName); err != nil {
			logger.Info("StartLifecycleTransition failed, marking LifecycleEvent Failed",
				"event", ev.Name,
				"driver", driverName,
				"err", err,
			)
			return r.failEvent(ctx, ev, driverName)
		}
		return nil
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

	req := buildEndRequest(ev, transition, r.nodeName, r.now())

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

	if _, err := r.removeFinalizer(ctx, ev.Name); err != nil {
		return fmt.Errorf("remove finalizer from LifecycleEvent %q: %w", ev.Name, err)
	}

	// Delete the event (garbage collection per KEP).
	err = r.kubeClient.LifecycleV1alpha1().LifecycleEvents().Delete(ctx, ev.Name, metav1.DeleteOptions{})
	if err != nil {
		return fmt.Errorf("delete completed LifecycleEvent %q: %w", ev.Name, err)
	}

	logger.Info("LifecycleEvent completed and deleted", "event", ev.Name)
	return nil
}

// failEvent transitions the LifecycleEvent to Failed and removes the kubelet
// finalizer so the object can be garbage collected.
func (r *Reconciler) failEvent(ctx context.Context, ev *lifecycleapi.LifecycleEvent, driverName string) error {
	logger := klog.FromContext(ctx).WithName("lifecycle-event-reconciler")

	updated := ev.DeepCopy()
	updated.Status.ClaimStatus = lifecycleapi.LifecycleEventFailed
	updated.Status.Driver = driverName

	if _, err := r.kubeClient.LifecycleV1alpha1().LifecycleEvents().UpdateStatus(ctx, updated, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("update LifecycleEvent %q status to Failed: %w", ev.Name, err)
	}

	if _, err := r.removeFinalizer(ctx, ev.Name); err != nil {
		return fmt.Errorf("remove finalizer from failed LifecycleEvent %q: %w", ev.Name, err)
	}

	if err := r.kubeClient.LifecycleV1alpha1().LifecycleEvents().Delete(ctx, ev.Name, metav1.DeleteOptions{}); err != nil {
		return fmt.Errorf("delete failed LifecycleEvent %q: %w", ev.Name, err)
	}

	logger.Info("LifecycleEvent failed and deleted",
		"event", ev.Name,
		"driver", driverName,
	)
	return nil
}

// slaExpireEvent transitions the LifecycleEvent to SlaExpired and removes the
// kubelet finalizer so the object can be garbage collected.
func (r *Reconciler) slaExpireEvent(ctx context.Context, ev *lifecycleapi.LifecycleEvent) error {
	logger := klog.FromContext(ctx).WithName("lifecycle-event-reconciler")

	updated := ev.DeepCopy()
	updated.Status.ClaimStatus = lifecycleapi.LifecycleEventSlaExpired

	if _, err := r.kubeClient.LifecycleV1alpha1().LifecycleEvents().UpdateStatus(ctx, updated, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("update LifecycleEvent %q status to SlaExpired: %w", ev.Name, err)
	}

	if _, err := r.removeFinalizer(ctx, ev.Name); err != nil {
		return fmt.Errorf("remove finalizer from SLA-expired LifecycleEvent %q: %w", ev.Name, err)
	}

	if err := r.kubeClient.LifecycleV1alpha1().LifecycleEvents().Delete(ctx, ev.Name, metav1.DeleteOptions{}); err != nil {
		return fmt.Errorf("delete SLA-expired LifecycleEvent %q: %w", ev.Name, err)
	}

	logger.Info("LifecycleEvent SLA-expired and deleted", "event", ev.Name)
	return nil
}

func (r *Reconciler) cleanupTerminalEvent(ctx context.Context, ev *lifecycleapi.LifecycleEvent) error {
	logger := klog.FromContext(ctx).WithName("lifecycle-event-reconciler")

	if _, err := r.removeFinalizer(ctx, ev.Name); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("remove finalizer from terminal LifecycleEvent %q: %w", ev.Name, err)
	}

	if err := r.kubeClient.LifecycleV1alpha1().LifecycleEvents().Delete(ctx, ev.Name, metav1.DeleteOptions{}); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("delete terminal LifecycleEvent %q: %w", ev.Name, err)
	}

	logger.Info("LifecycleEvent terminal cleanup completed",
		"event", ev.Name,
		"claimStatus", ev.Status.ClaimStatus,
	)
	return nil
}

func (r *Reconciler) ensureFinalizer(ctx context.Context, ev *lifecycleapi.LifecycleEvent) (*lifecycleapi.LifecycleEvent, error) {
	var result *lifecycleapi.LifecycleEvent
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest, err := r.kubeClient.LifecycleV1alpha1().LifecycleEvents().Get(ctx, ev.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if slices.Contains(latest.Finalizers, lifecycleEventFinalizer) {
			result = latest
			return nil
		}

		updated := latest.DeepCopy()
		updated.Finalizers = append(updated.Finalizers, lifecycleEventFinalizer)
		out, err := r.kubeClient.LifecycleV1alpha1().LifecycleEvents().Update(ctx, updated, metav1.UpdateOptions{})
		if err != nil {
			return err
		}
		result = out
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (r *Reconciler) removeFinalizer(ctx context.Context, eventName string) (*lifecycleapi.LifecycleEvent, error) {
	var result *lifecycleapi.LifecycleEvent
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		ev, err := r.kubeClient.LifecycleV1alpha1().LifecycleEvents().Get(ctx, eventName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if !slices.Contains(ev.Finalizers, lifecycleEventFinalizer) {
			result = ev
			return nil
		}

		updated := ev.DeepCopy()
		updated.Finalizers = slices.DeleteFunc(updated.Finalizers, func(s string) bool {
			return s == lifecycleEventFinalizer
		})
		out, err := r.kubeClient.LifecycleV1alpha1().LifecycleEvents().Update(ctx, updated, metav1.UpdateOptions{})
		if err != nil {
			return err
		}
		result = out
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func isSLAExpired(ev *lifecycleapi.LifecycleEvent) bool {
	return isSLAExpiredAt(ev, time.Now())
}

func isSLAExpiredAt(ev *lifecycleapi.LifecycleEvent, now time.Time) bool {
	if ev == nil || ev.Status.Sla == nil {
		return false
	}
	return !now.Before(ev.Status.Sla.Time)
}

func buildEndRequest(ev *lifecycleapi.LifecycleEvent, transition *lifecycleapi.LifecycleTransition, nodeName string, now time.Time) *slmpbv1alpha1.EndLifecycleTransitionRequest {
	return &slmpbv1alpha1.EndLifecycleTransitionRequest{
		TransitionName: transition.Name,
		EventName:      ev.Name,
		NodeName:       nodeName,
		Start:          transition.Spec.Start,
		End:            transition.Spec.End,
		SlaExpired:     isSLAExpiredAt(ev, now),
	}
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
