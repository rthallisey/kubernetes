//go:build linux || windows

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

package nodeshutdown

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	lifecycleapi "k8s.io/api/lifecycle/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/features"
)

const lifecycleTransitionConditionType v1.NodeConditionType = "LifecycleTransition"

// attemptToResumeShutdown restores node shutdown state from persisted lifecycle
// state if kubelet restarts while shutdown drain is already in progress.
func (m *managerImpl) attemptToResumeShutdown() {
	if !utilfeature.DefaultFeatureGate.Enabled(features.SpecializedLifecycleManagement) {
		return
	}
	if m.kubeClient == nil || m.nodeName == "" {
		return
	}

	ctx := context.Background()
	hasClaimedEvent, err := m.hasClaimedLifecycleEvent(ctx)
	if err != nil {
		m.logger.Error(err, "Failed checking claimed LifecycleEvents for shutdown recovery")
		return
	}
	if !hasClaimedEvent {
		return
	}

	reason, err := m.getLifecycleTransitionReason(ctx)
	if err != nil {
		m.logger.Error(err, "Failed reading Node LifecycleTransition condition for shutdown recovery", "node", m.nodeName)
		return
	}
	if reason != shutdownDrainStartedReason {
		return
	}

	m.nodeShuttingDownMutex.Lock()
	alreadyShuttingDown := m.nodeShuttingDownNow
	m.nodeShuttingDownNow = true
	m.nodeShuttingDownMutex.Unlock()
	if alreadyShuttingDown {
		return
	}

	m.logger.Info("Recovered shutdown state from lifecycle state, resuming graceful node shutdown",
		"node", m.nodeName,
		"conditionReason", reason,
	)
	go m.syncNodeStatus()
	go func() {
		if err := m.resumeShutdownEvent(); err != nil {
			m.logger.Error(err, "Failed resuming graceful node shutdown from lifecycle state")
		}
	}()
}

func (m *managerImpl) hasClaimedLifecycleEvent(ctx context.Context) (bool, error) {
	events, err := m.kubeClient.LifecycleV1alpha1().LifecycleEvents().List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, fmt.Errorf("list LifecycleEvents: %w", err)
	}

	for _, event := range events.Items {
		if event.Spec.BindingNode != m.nodeName {
			continue
		}
		if event.Status.ClaimStatus == lifecycleapi.LifecycleEventClaimed {
			return true, nil
		}
	}
	return false, nil
}

func (m *managerImpl) getLifecycleTransitionReason(ctx context.Context) (string, error) {
	node, err := m.kubeClient.CoreV1().Nodes().Get(ctx, m.nodeName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("get node %q: %w", m.nodeName, err)
	}
	for _, condition := range node.Status.Conditions {
		if condition.Type == lifecycleTransitionConditionType {
			return condition.Reason, nil
		}
	}
	return "", nil
}
