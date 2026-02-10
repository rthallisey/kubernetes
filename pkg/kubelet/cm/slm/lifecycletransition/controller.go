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

// Package lifecycletransition provides a controller that synchronises
// LifecycleTransition objects in the API server with the desired state
// published by an SLM driver.
//
// The design follows the same reconciliation pattern used by the DRA
// ResourceSlice controller in
// staging/src/k8s.io/dynamic-resource-allocation/resourceslice/.
package lifecycletransition

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	lifecycleapi "k8s.io/api/lifecycle/v1alpha1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/diff"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	lifecycleclient "k8s.io/client-go/kubernetes/typed/lifecycle/v1alpha1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

const (
	// syncDelay defines how long to wait between receiving the most
	// recent informer event and syncing again.
	syncDelay = 5 * time.Second
)

// Controller synchronises LifecycleTransition objects for one driver with the
// API server. An SLM driver publishes desired transitions via Update and the
// controller ensures the API server matches that desired state.
//
// Unlike ResourceSlice, LifecycleTransitions have no owner reference.
type Controller struct {
	cancel     func(cause error)
	driverName string
	client     lifecycleclient.LifecycleTransitionInterface
	wg         sync.WaitGroup

	// queue is keyed by transition name.
	queue workqueue.TypedRateLimitingInterface[string]
	store cache.Store

	errorHandler func(ctx context.Context, err error, msg string)

	// numCreates/numUpdates/numDeletes are stats counters.
	numCreates int64
	numUpdates int64
	numDeletes int64

	mutex sync.RWMutex
	// desired is the current desired set of transitions published by the
	// driver. The entire pointer is replaced atomically; callers reading it
	// only need the RLock.
	desired *DriverTransitions
}

// DriverTransitions is the complete set of LifecycleTransitions that a driver
// wants to exist in the API server.
type DriverTransitions struct {
	// Transitions is keyed by desired object name.
	Transitions map[string]TransitionSpec
}

// DeepCopy returns a deep copy of DriverTransitions.
func (dt *DriverTransitions) DeepCopy() *DriverTransitions {
	if dt == nil {
		return nil
	}
	out := &DriverTransitions{
		Transitions: make(map[string]TransitionSpec, len(dt.Transitions)),
	}
	for k, v := range dt.Transitions {
		out.Transitions[k] = v.DeepCopy()
	}
	return out
}

// TransitionSpec describes one desired LifecycleTransition. Fields map
// directly to LifecycleTransitionSpec.
type TransitionSpec struct {
	Start        string
	End          string
	NodeName     *string
	NodeSelector *lifecycleapi.LifecycleTransitionSpec // used only for its NodeSelector field
	AllNodes     *bool
	Sla          *metav1.Duration
}

// DeepCopy returns a deep copy of TransitionSpec.
func (ts TransitionSpec) DeepCopy() TransitionSpec {
	out := ts
	if ts.NodeName != nil {
		s := *ts.NodeName
		out.NodeName = &s
	}
	if ts.AllNodes != nil {
		b := *ts.AllNodes
		out.AllNodes = &b
	}
	if ts.Sla != nil {
		d := *ts.Sla
		out.Sla = &d
	}
	// NodeSelector deep copy is handled through the API type if set.
	return out
}

// Options contains settings for StartController.
type Options struct {
	// DriverName is the name of the SLM driver. Required.
	DriverName string

	// KubeClient is used to access the lifecycle API. Required.
	KubeClient kubernetes.Interface

	// Resources is the initial desired set of transitions. Nil means "none".
	Resources *DriverTransitions

	// ErrorHandler is called when the controller encounters a problem.
	// The default logs the error.
	ErrorHandler func(ctx context.Context, err error, msg string)
}

// StartController constructs and starts a new controller.
func StartController(ctx context.Context, options Options) (*Controller, error) {
	logger := klog.FromContext(ctx)
	c, err := newController(ctx, options)
	if err != nil {
		return nil, fmt.Errorf("create controller: %w", err)
	}

	logger.V(3).Info("Starting LifecycleTransition controller")
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer logger.V(3).Info("LifecycleTransition controller stopped")
		c.run(ctx)
	}()
	return c, nil
}

func newController(ctx context.Context, options Options) (*Controller, error) {
	if options.KubeClient == nil {
		return nil, errors.New("KubeClient is nil")
	}
	if options.DriverName == "" {
		return nil, errors.New("SLM driver name is empty")
	}

	ctx, cancel := context.WithCancelCause(ctx)

	c := &Controller{
		cancel:     cancel,
		driverName: options.DriverName,
		client:     options.KubeClient.LifecycleV1alpha1().LifecycleTransitions(),
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "lifecycle_transitions_" + options.DriverName},
		),
		errorHandler: options.ErrorHandler,
	}
	if c.errorHandler == nil {
		c.errorHandler = func(ctx context.Context, err error, msg string) {
			utilruntime.HandleErrorWithContext(ctx, err, msg)
		}
	}
	if err := c.initInformer(ctx); err != nil {
		return nil, err
	}

	c.Update(options.Resources)

	return c, nil
}

// initInformer sets up a shared informer that watches LifecycleTransitions
// by spec.driver.
func (c *Controller) initInformer(ctx context.Context) error {
	logger := klog.FromContext(ctx)

	driverSelector := fields.OneTermEqualSelector("spec.driver", c.driverName).String()

	informer := cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListWithContextFunc: func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
				options.FieldSelector = driverSelector
				list, err := c.client.List(ctx, options)
				if err == nil {
					logger.V(5).Info("Listed LifecycleTransitions", "numItems", len(list.Items), "fieldSelector", driverSelector)
				}
				return list, err
			},
			WatchFuncWithContext: func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
				options.FieldSelector = driverSelector
				w, err := c.client.Watch(ctx, options)
				logger.V(5).Info("Started watching LifecycleTransitions", "fieldSelector", driverSelector, "err", err)
				return w, err
			},
		},
		&lifecycleapi.LifecycleTransition{},
		0, // no resync
		cache.Indexers{},
	)
	c.store = informer.GetStore()

	handler, err := informer.AddEventHandlerWithOptions(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			t, ok := obj.(*lifecycleapi.LifecycleTransition)
			if !ok {
				return
			}
			logger.V(5).Info("LifecycleTransition add", "name", t.Name)
			c.queue.AddAfter(t.Name, syncDelay)
		},
		UpdateFunc: func(old, new any) {
			newT, ok := new.(*lifecycleapi.LifecycleTransition)
			if !ok {
				return
			}
			if loggerV := logger.V(6); loggerV.Enabled() {
				loggerV.Info("LifecycleTransition update", "name", newT.Name, "diff", diff.Diff(old, new))
			} else {
				logger.V(5).Info("LifecycleTransition update", "name", newT.Name)
			}
			c.queue.AddAfter(newT.Name, syncDelay)
		},
		DeleteFunc: func(obj any) {
			if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = tombstone.Obj
			}
			t, ok := obj.(*lifecycleapi.LifecycleTransition)
			if !ok {
				return
			}
			logger.V(5).Info("LifecycleTransition delete", "name", t.Name)
			c.queue.AddAfter(t.Name, syncDelay)
		},
	}, cache.HandlerOptions{Logger: &logger})
	if err != nil {
		return fmt.Errorf("registering event handler on the LifecycleTransition informer: %w", err)
	}

	logger.V(3).Info("Starting LifecycleTransition informer and waiting for sync")
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer logger.V(3).Info("LifecycleTransition informer has stopped")
		defer c.queue.ShutDown()
		informer.RunWithContext(ctx)
	}()
	for !handler.HasSynced() {
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return fmt.Errorf("sync LifecycleTransition informer: %w", context.Cause(ctx))
		}
	}
	logger.V(3).Info("LifecycleTransition informer has synced")
	return nil
}

// Stop cancels all background activity and blocks until the controller has stopped.
func (c *Controller) Stop() {
	if c == nil {
		return
	}
	c.cancel(errors.New("LifecycleTransition controller was asked to stop"))
	c.wg.Wait()
}

// Update sets the new desired state of transitions for this driver.
// Nil means "no transitions".
func (c *Controller) Update(resources *DriverTransitions) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Enqueue all old transition names for reconciliation.
	if c.desired != nil {
		for name := range c.desired.Transitions {
			c.queue.Add(name)
		}
	}

	if resources == nil {
		c.desired = &DriverTransitions{Transitions: map[string]TransitionSpec{}}
	} else {
		c.desired = resources.DeepCopy()
	}

	// Enqueue all new transition names.
	for name := range c.desired.Transitions {
		c.queue.Add(name)
	}
}

// GetStats returns operational statistics.
func (c *Controller) GetStats() Stats {
	return Stats{
		NumCreates: atomic.LoadInt64(&c.numCreates),
		NumUpdates: atomic.LoadInt64(&c.numUpdates),
		NumDeletes: atomic.LoadInt64(&c.numDeletes),
	}
}

// Stats contains operational statistics for the controller.
type Stats struct {
	NumCreates int64
	NumUpdates int64
	NumDeletes int64
}

// run processes the work queue.
func (c *Controller) run(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

func (c *Controller) processNextWorkItem(ctx context.Context) bool {
	name, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(name)

	err := c.syncTransition(klog.NewContext(ctx, klog.LoggerWithValues(klog.FromContext(ctx), "transitionName", name)), name)
	if err != nil {
		c.errorHandler(ctx, err, "processing LifecycleTransition")
		c.queue.AddRateLimited(name)
		return true
	}
	c.queue.Forget(name)
	return true
}

// syncTransition reconciles a single transition by name. It compares the
// desired state (from the driver) with the actual state (from the informer
// cache) and creates, updates, or deletes the API object as needed.
func (c *Controller) syncTransition(ctx context.Context, name string) error {
	logger := klog.FromContext(ctx)

	// Read the desired state.
	c.mutex.RLock()
	desired := c.desired
	c.mutex.RUnlock()

	desiredSpec, wantExist := desired.Transitions[name]

	// Read the actual state from the informer cache.
	var existing *lifecycleapi.LifecycleTransition
	obj, exists, err := c.store.GetByKey(name)
	if err != nil {
		return fmt.Errorf("get LifecycleTransition %q from cache: %w", name, err)
	}
	if exists {
		t, ok := obj.(*lifecycleapi.LifecycleTransition)
		if ok && t.Spec.Driver == c.driverName {
			existing = t
		}
	}

	switch {
	case wantExist && existing == nil:
		// Create.
		transition := &lifecycleapi.LifecycleTransition{
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
			},
			Spec: buildSpec(c.driverName, desiredSpec),
		}
		logger.V(5).Info("Creating LifecycleTransition")
		if _, err := c.client.Create(ctx, transition, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("create LifecycleTransition %q: %w", name, err)
		}
		atomic.AddInt64(&c.numCreates, 1)
		logger.V(4).Info("Created LifecycleTransition")

	case wantExist && existing != nil:
		// Update if changed.
		newSpec := buildSpec(c.driverName, desiredSpec)
		if apiequality.Semantic.DeepEqual(existing.Spec, newSpec) {
			logger.V(6).Info("LifecycleTransition unchanged, skipping update")
			return nil
		}
		updated := existing.DeepCopy()
		updated.Spec = newSpec
		logger.V(5).Info("Updating LifecycleTransition")
		if _, err := c.client.Update(ctx, updated, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("update LifecycleTransition %q: %w", name, err)
		}
		atomic.AddInt64(&c.numUpdates, 1)
		logger.V(4).Info("Updated LifecycleTransition")

	case !wantExist && existing != nil:
		// Delete.
		logger.V(5).Info("Deleting LifecycleTransition")
		opts := metav1.DeleteOptions{
			Preconditions: &metav1.Preconditions{
				UID:             &existing.UID,
				ResourceVersion: &existing.ResourceVersion,
			},
		}
		err := c.client.Delete(ctx, name, opts)
		if apierrors.IsNotFound(err) {
			logger.V(5).Info("LifecycleTransition already deleted")
			return nil
		}
		if err != nil {
			return fmt.Errorf("delete LifecycleTransition %q: %w", name, err)
		}
		atomic.AddInt64(&c.numDeletes, 1)
		logger.V(4).Info("Deleted LifecycleTransition")

	default:
		// !wantExist && existing == nil: nothing to do.
		logger.V(6).Info("LifecycleTransition not desired and not present, nothing to do")
	}

	return nil
}

// buildSpec creates the external API spec from the driver's desired spec.
func buildSpec(driverName string, ts TransitionSpec) lifecycleapi.LifecycleTransitionSpec {
	spec := lifecycleapi.LifecycleTransitionSpec{
		Start:    ts.Start,
		End:      ts.End,
		NodeName: ts.NodeName,
		AllNodes: ts.AllNodes,
		Sla:      ts.Sla,
		Driver:   driverName,
	}
	if ts.NodeSelector != nil {
		spec.NodeSelector = ts.NodeSelector.NodeSelector
	}
	return spec
}
