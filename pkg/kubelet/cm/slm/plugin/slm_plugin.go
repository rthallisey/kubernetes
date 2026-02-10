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
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	slmpbv1alpha1 "k8s.io/kubelet/pkg/apis/slm/v1alpha1"
)

// defaultClientCallTimeout is the default amount of time that an SLM driver
// has to respond to any of the gRPC calls.
const defaultClientCallTimeout = 45 * time.Second

// LifecycleTransitionConditionType is the Node condition type used to report
// lifecycle transition state, as defined in the SLM KEP.
const LifecycleTransitionConditionType v1.NodeConditionType = "LifecycleTransition"

// All gRPC service versions supported by the kubelet for SLM plugins.
// Sorted by most recent first, oldest last.
var servicesSupportedByKubelet = []string{
	slmpbv1alpha1.SLMPluginService,
}

// SLMPlugin contains information about one registered SLM lifecycle driver
// plugin. It wraps the gRPC connection used to call the driver's
// StartLifecycleTransition and EndLifecycleTransition RPCs.
//
// After a successful (non-error) gRPC response, the plugin automatically
// patches the target Node's .status.conditions with the lifecycle state
// returned by the driver. This frees individual drivers from having to
// implement the Node condition update themselves.
type SLMPlugin struct {
	driverName        string
	conn              *grpc.ClientConn
	endpoint          string
	chosenService     string
	clientCallTimeout time.Duration
	backgroundCtx     context.Context
	kubeClient        kubernetes.Interface
}

// DriverName returns the name of the lifecycle driver this plugin belongs to.
func (p *SLMPlugin) DriverName() string {
	return p.driverName
}

// WaitForReady waits until the plugin connection is ready or the timeout is reached.
//
// A nil connection is treated as ready to keep lightweight tests from requiring
// a full gRPC transport setup.
func (p *SLMPlugin) WaitForReady(ctx context.Context, timeout time.Duration) error {
	if p == nil || p.conn == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for {
		state := p.conn.GetState()
		if state == connectivity.Ready {
			return nil
		}

		if !p.conn.WaitForStateChange(ctx, state) {
			if ctx.Err() != nil {
				return fmt.Errorf("waiting for SLM driver %q to become ready: %w", p.driverName, ctx.Err())
			}
			return errors.New("waiting for SLM driver connection state change failed")
		}
	}
}

// StartLifecycleTransition calls the driver's StartLifecycleTransition RPC.
// On a successful response with no error field set, it patches the Node
// condition with the returned LifecycleCondition as the reason.
func (p *SLMPlugin) StartLifecycleTransition(
	ctx context.Context,
	req *slmpbv1alpha1.StartLifecycleTransitionRequest,
	opts ...grpc.CallOption,
) (*slmpbv1alpha1.LifecycleTransitionResponse, error) {
	logger := klog.FromContext(ctx).WithName("slm-plugin")
	logger = klog.LoggerWithValues(logger, "driverName", p.driverName, "endpoint", p.endpoint)
	ctx = klog.NewContext(ctx, logger)
	logger.V(4).Info("Calling StartLifecycleTransition rpc", "request", req)

	ctx, cancel := context.WithTimeout(ctx, p.clientCallTimeout)
	defer cancel()

	var err error
	var response *slmpbv1alpha1.LifecycleTransitionResponse
	switch p.chosenService {
	case slmpbv1alpha1.SLMPluginService:
		client := slmpbv1alpha1.NewSLMPluginClient(p.conn)
		response, err = client.StartLifecycleTransition(ctx, req, opts...)
	default:
		return nil, fmt.Errorf("internal error: unsupported chosen service: %q", p.chosenService)
	}
	logger.V(4).Info("Done calling StartLifecycleTransition rpc", "response", response, "err", err)
	if err == nil && response.GetError() != "" {
		return response, fmt.Errorf("StartLifecycleTransition returned driver error: %s", response.GetError())
	}

	if err == nil && response.GetError() == "" {
		if patchErr := p.patchNodeCondition(ctx, response, req.GetTransitionName()); patchErr != nil {
			logger.Error(patchErr, "Failed to patch Node condition after StartLifecycleTransition")
			// Return the gRPC response but also report the patch failure.
			return response, patchErr
		}
	}

	return response, err
}

// EndLifecycleTransition calls the driver's EndLifecycleTransition RPC.
// On a successful response with no error field set, it patches the Node
// condition with the returned LifecycleCondition as the reason.
func (p *SLMPlugin) EndLifecycleTransition(
	ctx context.Context,
	req *slmpbv1alpha1.EndLifecycleTransitionRequest,
	opts ...grpc.CallOption,
) (*slmpbv1alpha1.LifecycleTransitionResponse, error) {
	logger := klog.FromContext(ctx).WithName("slm-plugin")
	logger = klog.LoggerWithValues(logger, "driverName", p.driverName, "endpoint", p.endpoint)
	ctx = klog.NewContext(ctx, logger)
	logger.V(4).Info("Calling EndLifecycleTransition rpc", "request", req)

	ctx, cancel := context.WithTimeout(ctx, p.clientCallTimeout)
	defer cancel()

	var err error
	var response *slmpbv1alpha1.LifecycleTransitionResponse
	switch p.chosenService {
	case slmpbv1alpha1.SLMPluginService:
		client := slmpbv1alpha1.NewSLMPluginClient(p.conn)
		response, err = client.EndLifecycleTransition(ctx, req, opts...)
	default:
		return nil, fmt.Errorf("internal error: unsupported chosen service: %q", p.chosenService)
	}
	logger.V(4).Info("Done calling EndLifecycleTransition rpc", "response", response, "err", err)
	if err == nil && response.GetError() != "" {
		return response, fmt.Errorf("EndLifecycleTransition returned driver error: %s", response.GetError())
	}

	if err == nil && response.GetError() == "" {
		if patchErr := p.patchNodeCondition(ctx, response, req.GetTransitionName()); patchErr != nil {
			logger.Error(patchErr, "Failed to patch Node condition after EndLifecycleTransition")
			return response, patchErr
		}
	}

	return response, err
}

// patchNodeCondition patches the target Node's .status.conditions with a
// LifecycleTransition condition derived from the driver's gRPC response.
//
// The condition looks like:
//
//	conditions:
//	  - type: LifecycleTransition
//	    status: "True"
//	    reason: <response.LifecycleCondition>
//	    message: "Lifecycle Transition '<transitionName>'"
//	    lastHeartbeatTime: <now>
//	    lastTransitionTime: <now>
func (p *SLMPlugin) patchNodeCondition(ctx context.Context, resp *slmpbv1alpha1.LifecycleTransitionResponse, transitionName string) error {
	nodeName := resp.GetNodeName()
	reason := resp.GetLifecycleCondition()
	if nodeName == "" || reason == "" {
		// Nothing to patch if the driver didn't provide the required fields.
		return nil
	}

	logger := klog.FromContext(ctx)
	logger.V(3).Info("Patching Node condition",
		"node", nodeName,
		"conditionType", LifecycleTransitionConditionType,
		"reason", reason,
		"transitionName", transitionName,
	)

	now := metav1.NewTime(time.Now())
	condition := v1.NodeCondition{
		Type:               LifecycleTransitionConditionType,
		Status:             v1.ConditionTrue,
		Reason:             reason,
		Message:            fmt.Sprintf("Lifecycle Transition '%s'", transitionName),
		LastHeartbeatTime:  now,
		LastTransitionTime: now,
	}

	patch, err := json.Marshal(map[string]interface{}{
		"status": map[string]interface{}{
			"conditions": []v1.NodeCondition{condition},
		},
	})
	if err != nil {
		return fmt.Errorf("marshal condition patch: %w", err)
	}

	_, err = p.kubeClient.CoreV1().Nodes().PatchStatus(ctx, nodeName, patch)
	if err != nil {
		return fmt.Errorf("patch node %s status: %w", nodeName, err)
	}

	logger.V(3).Info("Patched Node condition successfully", "node", nodeName, "reason", reason)
	return nil
}
