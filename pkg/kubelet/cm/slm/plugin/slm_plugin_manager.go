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
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/apis/lifecycle"
	timedworkers "k8s.io/kubernetes/pkg/controller/tainteviction"
	"k8s.io/kubernetes/pkg/kubelet/pluginmanager/cache"
	"k8s.io/utils/ptr"
)

// cleanupDelay is the time a driver has to re-register after being unregistered
// before the kubelet removes the node-scoped LifecycleTransitions it created.
// This must be long enough to allow a simple container restart.
const cleanupDelay = 30 * time.Second

// SLMPluginManager keeps track of registered SLM lifecycle driver plugins.
// Each plugin has a gRPC endpoint. There may be more than one plugin per driver.
//
// When a driver deregisters and does not come back within [cleanupDelay], the
// manager deletes the node-scoped LifecycleTransitions owned by that driver
// (filtered server-side by spec.nodeName and spec.driver). Transitions with
// AllNodes or NodeSelector are multi-node and are not cleaned up by any
// single kubelet.
//
// To be informed about available plugins, the SLMPluginManager implements the
// [cache.PluginHandler] interface and needs to be added to the plugin manager.
//
// The null SLMPluginManager is not usable, use NewSLMPluginManager.
type SLMPluginManager struct {
	backgroundCtx context.Context
	cancel        func(err error)

	wg    sync.WaitGroup
	mutex sync.RWMutex

	// kubeClient is passed to each SLMPlugin so the kubelet can
	// automatically patch Node conditions after gRPC calls. It is
	// also used to delete LifecycleTransitions during cleanup.
	kubeClient kubernetes.Interface

	// nodeName is the name of the node this kubelet runs on.
	nodeName string

	// driver name -> SLMPlugin instances in the order in which they were added.
	store map[string][]*SLMPlugin

	// pendingCleanups tracks at which time LifecycleTransitions for an
	// SLM driver should be removed. The removal happens in the background
	// in a callback function invoked by the TimedWorkerQueue.
	pendingCleanups *timedworkers.TimedWorkerQueue
}

var _ cache.PluginHandler = &SLMPluginManager{}

// NewSLMPluginManager creates a new SLMPluginManager.
//
// The context can be used to cancel all background activities.
// If desired, Stop can be called in addition or instead of canceling
// the context. It then also waits for background activities to stop.
func NewSLMPluginManager(ctx context.Context, kubeClient kubernetes.Interface, nodeName string) *SLMPluginManager {
	ctx, cancel := context.WithCancelCause(ctx)
	pm := &SLMPluginManager{
		backgroundCtx: klog.NewContext(ctx, klog.LoggerWithName(klog.FromContext(ctx), "SLM registration handler")),
		cancel:        cancel,
		kubeClient:    kubeClient,
		nodeName:      nodeName,
		store:         make(map[string][]*SLMPlugin),
	}

	pm.pendingCleanups = timedworkers.CreateWorkerQueue(func(ctx context.Context, fireAt time.Time, args *timedworkers.WorkArgs) error {
		pm.cleanupLifecycleTransitions(ctx, args.Object.Name)
		return nil
	})

	// At kubelet startup, no SLM driver has registered yet. Clean up any
	// orphaned node-scoped LifecycleTransitions so pods are not stuck
	// waiting for a driver that may never come back.
	pm.wg.Add(1)
	go func() {
		defer pm.wg.Done()

		ctx := pm.backgroundCtx
		logger := klog.LoggerWithName(klog.FromContext(ctx), "startup")
		ctx = klog.NewContext(ctx, logger)
		pm.cleanupLifecycleTransitions(ctx, "" /* all drivers */)
	}()

	return pm
}

// Stop cancels any remaining background activities and blocks until all
// goroutines have stopped.
func (pm *SLMPluginManager) Stop() {
	defer pm.wg.Wait()
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	logger := klog.FromContext(pm.backgroundCtx)
	pm.cancel(errors.New("Stop was called"))

	for driverName, plugins := range pm.store {
		workerArg := timedworkers.NewWorkArgs(driverName, "")
		pm.pendingCleanups.CancelWork(logger, workerArg.KeyFromWorkArgs())
		for _, plugin := range plugins {
			if err := plugin.conn.Close(); err != nil {
				logger.Error(err, "Closing gRPC connection", "driverName", plugin.driverName, "endpoint", plugin.endpoint)
			}
		}
	}
}

// cleanupLifecycleTransitions deletes LifecycleTransitions owned by a driver
// (or all drivers if driver is empty) that are scoped to this node.
//
// Only transitions with spec.nodeName set to this node are deleted.
// Transitions with AllNodes or NodeSelector are not owned by a single
// node and are left for the driver (or an administrator) to manage.
//
// It is called in a stand-alone goroutine at kubelet startup and as a
// callback of a TimedWorkersQueue. In both cases the caller has no way
// of handling errors, so cleanupLifecycleTransitions implements its own
// retry mechanism with exponential backoff.
func (pm *SLMPluginManager) cleanupLifecycleTransitions(ctx context.Context, driver string) {
	if pm.kubeClient == nil {
		return
	}
	logger := klog.FromContext(ctx)
	if pm.nodeName == "" {
		logger.Info("Skipping LifecycleTransition cleanup because nodeName is empty")
		return
	}

	backoff := wait.Backoff{
		Duration: time.Second,
		Factor:   2,
		Jitter:   0.2,
		Cap:      5 * time.Minute,
		Steps:    100,
	}

	_ = wait.ExponentialBackoffWithContext(ctx, backoff, func(ctx context.Context) (bool, error) {
		fieldSelector := fields.Set{
			// Only delete transitions scoped to this specific node.
			// AllNodes and NodeSelector transitions have an empty
			// spec.nodeName and will not match.
			lifecycle.LifecycleTransitionSelectorNodeName: pm.nodeName,
		}
		if driver != "" {
			fieldSelector[lifecycle.LifecycleTransitionSelectorDriver] = driver
		}

		err := pm.kubeClient.LifecycleV1alpha1().LifecycleTransitions().DeleteCollection(
			ctx,
			metav1.DeleteOptions{},
			metav1.ListOptions{FieldSelector: fieldSelector.String()},
		)
		switch {
		case err == nil:
			logger.V(3).Info("Deleted LifecycleTransitions", "fieldSelector", fieldSelector)
			return true, nil
		case apierrors.IsUnauthorized(err):
			logger.V(5).Info("Deleting LifecycleTransitions failed, retrying", "fieldSelector", fieldSelector, "err", err)
			return false, nil
		case apierrors.IsNotFound(err):
			logger.V(5).Info("LifecycleTransitions not found, nothing to delete", "fieldSelector", fieldSelector)
			return true, nil
		default:
			logger.V(3).Info("Deleting LifecycleTransitions failed, retrying", "fieldSelector", fieldSelector, "err", err)
			return false, nil
		}
	})
}

// sync must be called each time the information about a plugin changes.
// The mutex must be locked for writing.
func (pm *SLMPluginManager) sync(driverName string) {
	if pm.kubeClient == nil {
		return
	}
	ctx := pm.backgroundCtx
	logger := klog.FromContext(pm.backgroundCtx)
	workerArgs := timedworkers.NewWorkArgs(driverName, "")

	// Is the SLM driver usable again?
	if pm.usable(driverName) {
		// Yes: cancel any pending LifecycleTransition cleanup.
		pm.pendingCleanups.CancelWork(logger, workerArgs.KeyFromWorkArgs())
		return
	}

	// No: ensure that we clean up node-scoped LifecycleTransitions of the
	// driver. If already queued, the original timeout continues to apply.
	if pm.pendingCleanups.GetWorkerUnsafe(workerArgs.KeyFromWorkArgs()) != nil {
		return
	}
	now := time.Now()
	fireAt := now.Add(cleanupDelay)
	logger = klog.LoggerWithName(logger, "driver-cleanup")
	logger = klog.LoggerWithValues(logger, "driverName", driverName)
	ctx = klog.NewContext(ctx, logger)
	pm.pendingCleanups.AddWork(ctx, timedworkers.NewWorkArgs(driverName, ""), now, fireAt)
}

// usable returns true if at least one endpoint is registered for the driver.
// Must be called while holding the mutex.
func (pm *SLMPluginManager) usable(driverName string) bool {
	return len(pm.store[driverName]) > 0
}

// GetPlugin returns the SLM plugin for the given driver name.
// It returns an informative error if the driver is not registered.
func (pm *SLMPluginManager) GetPlugin(driverName string) (*SLMPlugin, error) {
	if driverName == "" {
		return nil, errors.New("SLM driver name is empty")
	}
	plugin := pm.get(driverName)
	if plugin == nil {
		return nil, fmt.Errorf("SLM driver %s is not registered", driverName)
	}
	return plugin, nil
}

func (pm *SLMPluginManager) get(driverName string) *SLMPlugin {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	logger := klog.FromContext(pm.backgroundCtx)

	plugins := pm.store[driverName]
	if len(plugins) == 0 {
		logger.V(5).Info("No SLM plugin registered", "driverName", driverName)
		return nil
	}

	// Heuristic: pick the most recent one.
	plugin := plugins[len(plugins)-1]
	logger.V(5).Info("Using latest SLM plugin", "driverName", driverName, "endpoint", plugin.endpoint)
	return plugin
}

// RegisterPlugin implements [cache.PluginHandler].
// It is called by the plugin manager when a plugin is ready to be registered.
//
// SLM uses the versions array in the registration API to enumerate supported
// gRPC services, using the "<gRPC package name>.<service name>" format
// (e.g., "v1alpha1.SLMPlugin").
func (pm *SLMPluginManager) RegisterPlugin(driverName string, endpoint string, supportedServices []string, pluginClientTimeout *time.Duration) error {
	chosenService, err := pm.validateSupportedServices(driverName, supportedServices)
	if err != nil {
		return fmt.Errorf("invalid supported gRPC services of SLM driver plugin %s at endpoint %s: %w", driverName, endpoint, err)
	}

	timeout := ptr.Deref(pluginClientTimeout, defaultClientCallTimeout)

	if err := pm.add(driverName, endpoint, chosenService, timeout); err != nil {
		return err
	}

	return nil
}

func (pm *SLMPluginManager) add(driverName string, endpoint string, chosenService string, clientCallTimeout time.Duration) error {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	p := &SLMPlugin{
		driverName:        driverName,
		endpoint:          endpoint,
		chosenService:     chosenService,
		clientCallTimeout: clientCallTimeout,
		backgroundCtx:     pm.backgroundCtx,
		kubeClient:        pm.kubeClient,
	}

	for _, oldP := range pm.store[driverName] {
		if oldP.endpoint == endpoint {
			return fmt.Errorf("endpoint %s already registered for SLM driver plugin %s", endpoint, driverName)
		}
	}

	logger := klog.FromContext(pm.backgroundCtx)

	target := "unix:" + endpoint
	logger.V(4).Info("Creating new gRPC connection", "target", target)
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("create gRPC connection to SLM driver %s plugin at endpoint %s: %w", driverName, endpoint, err)
	}
	p.conn = conn

	// Eagerly connect so we discover availability early.
	conn.Connect()

	pm.store[driverName] = append(pm.store[driverName], p)
	logger.V(3).Info("Registered SLM plugin", "driverName", driverName, "endpoint", endpoint, "chosenService", chosenService, "numPlugins", len(pm.store[driverName]))

	// Driver retured, so cancel any pending cleanup.
	pm.sync(driverName)

	return nil
}

// DeRegisterPlugin implements [cache.PluginHandler].
// It is called by the plugin manager when a plugin's registration socket is removed.
func (pm *SLMPluginManager) DeRegisterPlugin(driverName, endpoint string) {
	pm.remove(driverName, endpoint)
}

func (pm *SLMPluginManager) remove(driverName, endpoint string) {
	logger := klog.FromContext(pm.backgroundCtx)
	var p *SLMPlugin
	defer func() {
		if p != nil && p.conn != nil {
			if err := p.conn.Close(); err != nil {
				logger.Error(err, "Closing gRPC connection", "driverName", driverName, "endpoint", endpoint)
			}
		}
	}()
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	plugins := pm.store[driverName]
	i := slices.IndexFunc(plugins, func(plugin *SLMPlugin) bool { return plugin.driverName == driverName && plugin.endpoint == endpoint })
	if i == -1 {
		return
	}
	p = plugins[i]

	if len(plugins) == 1 {
		delete(pm.store, driverName)
	} else {
		pm.store[driverName] = slices.Delete(plugins, i, i+1)
	}

	logger.V(3).Info("Unregistered SLM plugin", "driverName", driverName, "endpoint", endpoint, "numPlugins", len(pm.store[driverName]))

	// Driver gone — schedule LifecycleTransition cleanup.
	pm.sync(driverName)
}

// ValidatePlugin implements [cache.PluginHandler].
// It is called by the plugin manager upon detection of a new registration socket.
func (pm *SLMPluginManager) ValidatePlugin(driverName string, endpoint string, supportedServices []string) error {
	_, err := pm.validateSupportedServices(driverName, supportedServices)
	if err != nil {
		return fmt.Errorf("invalid supported gRPC services of SLM driver plugin %s at endpoint %s: %w", driverName, endpoint, err)
	}
	return nil
}

// validateSupportedServices identifies the highest supported gRPC service
// and returns its name (e.g., "v1alpha1.SLMPlugin"). An error is returned
// if the plugin is unusable.
func (pm *SLMPluginManager) validateSupportedServices(driverName string, supportedServices []string) (string, error) {
	if len(supportedServices) == 0 {
		return "", errors.New("empty list of supported gRPC services (aka supported versions)")
	}

	for _, service := range supportedServices {
		if slices.Contains(servicesSupportedByKubelet, service) {
			return service, nil
		}
	}

	return "", fmt.Errorf("none of the services supported by the plugin (%q) are supported by the kubelet (%q)", supportedServices, servicesSupportedByKubelet)
}
