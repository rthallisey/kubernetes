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

package slm

import (
	"context"

	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/cm/slm/lifecycleevent"
	slmplugin "k8s.io/kubernetes/pkg/kubelet/cm/slm/plugin"
	"k8s.io/kubernetes/pkg/kubelet/pluginmanager/cache"
)

// Manager is responsible for managing Specialized Lifecycle Management
// drivers and coordinating lifecycle transitions.
type Manager struct {
	// slmPlugins manages the registered SLM driver plugins and handles
	// cleanup of LifecycleTransitions when drivers deregister.
	slmPlugins *slmplugin.SLMPluginManager

	// kubeClient is used to interact with the lifecycle API.
	kubeClient kubernetes.Interface

	// nodeName is the name of the Node this kubelet is running on.
	nodeName string

	// eventReconciler drives the LifecycleEvent claim/transition loop.
	eventReconciler *lifecycleevent.Reconciler
}

// NewManager creates a new SLM Manager.
func NewManager(logger klog.Logger, kubeClient kubernetes.Interface, nodeName string) *Manager {
	return &Manager{
		kubeClient: kubeClient,
		nodeName:   nodeName,
	}
}

// Start initializes the SLM manager and begins watching for LifecycleEvents.
func (m *Manager) Start(ctx context.Context) error {
	logger := klog.FromContext(ctx)
	logger.Info("Starting Specialized Lifecycle Management (SLM) manager")

	m.slmPlugins = slmplugin.NewSLMPluginManager(ctx, m.kubeClient, m.nodeName)

	// Start the LifecycleEvent reconciler. It periodically polls for
	// LifecycleEvents bound to this node, claims at most one at a time,
	// and drives that event from Start to End using Conditions on the Node.
	m.eventReconciler = lifecycleevent.NewReconciler(m.nodeName, m.kubeClient, m.slmPlugins)
	go m.eventReconciler.Run(ctx)

	return nil
}

// Stop shuts down the SLM manager and all plugin connections.
func (m *Manager) Stop() {
	if m.slmPlugins != nil {
		m.slmPlugins.Stop()
	}
}

// GetWatcherHandler returns the PluginHandler used by the kubelet plugin
// manager to register/deregister SLM driver plugins.
//
// Must be called after Start.
func (m *Manager) GetWatcherHandler() cache.PluginHandler {
	return m.slmPlugins
}

// GetPlugin returns the SLMPlugin for the named driver, or an error
// if the driver is not registered.
func (m *Manager) GetPlugin(driverName string) (*slmplugin.SLMPlugin, error) {
	return m.slmPlugins.GetPlugin(driverName)
}
