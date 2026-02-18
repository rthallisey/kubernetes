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

package app

import (
	"context"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	lifecycleapi "k8s.io/api/lifecycle/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
)

const lifecycleTransitionConditionType v1.NodeConditionType = "LifecycleTransition"

// ExternalControllerOptions configures the centralized-controller example mode
// for the SLM test driver.
type ExternalControllerOptions struct {
	DriverName           string
	NodeName             string
	TransitionName       string
	EventName            string
	StartState           string
	EndState             string
	PollInterval         time.Duration
	DeleteCompletedEvent bool
	AutoCompleteAfter    time.Duration
}

func runExternalController(ctx context.Context, kubeClient kubernetes.Interface, options ExternalControllerOptions) error {
	logger := klog.FromContext(ctx).WithName("slm-external-controller")
	ctx = klog.NewContext(ctx, logger)

	if options.NodeName == "" {
		return fmt.Errorf("node name is required")
	}
	if options.TransitionName == "" {
		return fmt.Errorf("transition name is required")
	}
	if options.StartState == "" || options.EndState == "" {
		return fmt.Errorf("start and end states are required")
	}
	if options.PollInterval <= 0 {
		options.PollInterval = time.Second
	}

	if err := ensureLifecycleTransition(ctx, kubeClient, options); err != nil {
		return err
	}

	var claimTime *time.Time
	ticker := time.NewTicker(options.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}

		ev, err := kubeClient.LifecycleV1alpha1().LifecycleEvents().Get(ctx, options.EventName, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				logger.Info("LifecycleEvent not found yet", "event", options.EventName)
				continue
			}
			return fmt.Errorf("get LifecycleEvent %q: %w", options.EventName, err)
		}

		if ev.Status.ClaimStatus != lifecycleapi.LifecycleEventClaimed {
			if err := updateLifecycleEventStatus(ctx, kubeClient, options, lifecycleapi.LifecycleEventClaimed); err != nil {
				return err
			}
			now := time.Now()
			claimTime = &now
			logger.Info("Claimed LifecycleEvent", "event", options.EventName, "driver", options.DriverName)
		}

		if err := setNodeLifecycleTransitionReason(ctx, kubeClient, options.NodeName, options.StartState, options.TransitionName); err != nil {
			return err
		}

		if options.AutoCompleteAfter <= 0 || claimTime == nil {
			continue
		}
		if time.Since(*claimTime) < options.AutoCompleteAfter {
			continue
		}

		if err := setNodeLifecycleTransitionReason(ctx, kubeClient, options.NodeName, options.EndState, options.TransitionName); err != nil {
			return err
		}
		if err := updateLifecycleEventStatus(ctx, kubeClient, options, lifecycleapi.LifecycleEventSucceeded); err != nil {
			return err
		}
		if options.DeleteCompletedEvent {
			if err := kubeClient.LifecycleV1alpha1().LifecycleEvents().Delete(ctx, options.EventName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("delete LifecycleEvent %q: %w", options.EventName, err)
			}
		}
		logger.Info("Completed LifecycleEvent", "event", options.EventName, "transition", options.TransitionName)
		claimTime = nil
	}
}

func ensureLifecycleTransition(ctx context.Context, kubeClient kubernetes.Interface, options ExternalControllerOptions) error {
	logger := klog.FromContext(ctx)
	transition := &lifecycleapi.LifecycleTransition{
		ObjectMeta: metav1.ObjectMeta{
			Name: options.TransitionName,
		},
		Spec: lifecycleapi.LifecycleTransitionSpec{
			Start:    options.StartState,
			End:      options.EndState,
			NodeName: &options.NodeName,
			Driver:   options.DriverName,
		},
	}
	_, err := kubeClient.LifecycleV1alpha1().LifecycleTransitions().Create(ctx, transition, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return retry.RetryOnConflict(retry.DefaultRetry, func() error {
			existing, getErr := kubeClient.LifecycleV1alpha1().LifecycleTransitions().Get(ctx, options.TransitionName, metav1.GetOptions{})
			if getErr != nil {
				return getErr
			}
			existing.Spec = transition.Spec
			_, updateErr := kubeClient.LifecycleV1alpha1().LifecycleTransitions().Update(ctx, existing, metav1.UpdateOptions{})
			return updateErr
		})
	}
	if err != nil {
		return fmt.Errorf("create LifecycleTransition %q: %w", options.TransitionName, err)
	}
	logger.Info("Created LifecycleTransition", "name", options.TransitionName, "driver", options.DriverName)
	return nil
}

func updateLifecycleEventStatus(ctx context.Context, kubeClient kubernetes.Interface, options ExternalControllerOptions, status lifecycleapi.LifecycleEventClaimStatus) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		ev, err := kubeClient.LifecycleV1alpha1().LifecycleEvents().Get(ctx, options.EventName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		ev.Status.ClaimStatus = status
		ev.Status.Driver = options.DriverName
		_, err = kubeClient.LifecycleV1alpha1().LifecycleEvents().UpdateStatus(ctx, ev, metav1.UpdateOptions{})
		return err
	})
}

func setNodeLifecycleTransitionReason(ctx context.Context, kubeClient kubernetes.Interface, nodeName, reason, transitionName string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		node, err := kubeClient.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
		if err != nil {
			return err
		}

		now := metav1.NewTime(time.Now())
		for i := range node.Status.Conditions {
			c := &node.Status.Conditions[i]
			if c.Type != lifecycleTransitionConditionType {
				continue
			}
			if c.Reason == reason {
				return nil
			}
			c.Status = v1.ConditionTrue
			c.Reason = reason
			c.Message = fmt.Sprintf("Lifecycle Transition '%s'", transitionName)
			c.LastHeartbeatTime = now
			c.LastTransitionTime = now
			_, err = kubeClient.CoreV1().Nodes().UpdateStatus(ctx, node, metav1.UpdateOptions{})
			return err
		}

		node.Status.Conditions = append(node.Status.Conditions, v1.NodeCondition{
			Type:               lifecycleTransitionConditionType,
			Status:             v1.ConditionTrue,
			Reason:             reason,
			Message:            fmt.Sprintf("Lifecycle Transition '%s'", transitionName),
			LastHeartbeatTime:  now,
			LastTransitionTime: now,
		})
		_, err = kubeClient.CoreV1().Nodes().UpdateStatus(ctx, node, metav1.UpdateOptions{})
		return err
	})
}
