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
	"net"
	"path"
	"path/filepath"
	"sync"

	"google.golang.org/grpc"

	lifecycleapi "k8s.io/api/lifecycle/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	registerapi "k8s.io/kubelet/pkg/apis/pluginregistration/v1"
	slmpbv1alpha1 "k8s.io/kubelet/pkg/apis/slm/v1alpha1"
)

// TransitionSpec describes a single LifecycleTransition that the driver
// wants to publish on startup.
type TransitionSpec struct {
	Name     string
	Start    string
	End      string
	NodeName *string
}

// ListenerFunc is a function that creates a net.Listener for the given
// Unix domain socket endpoint. This allows the caller to use a proxy-based
// listener for e2e tests.
type ListenerFunc func(ctx context.Context, endpoint string) (net.Listener, error)

// GRPCCall records a single gRPC call made to the plugin.
type GRPCCall struct {
	Method string
	Err    error
}

// SLMTestPlugin ties together the gRPC plugin service, the kubelet
// registration server, and the LifecycleTransitions created by the driver.
type SLMTestPlugin struct {
	driverName  string
	nodeName    string
	slmEndpoint string // path to the SLM gRPC socket
	kubeClient  kubernetes.Interface

	slmServer          *grpc.Server
	registrationServer *grpc.Server

	// transitions tracks which LifecycleTransition objects the driver
	// created so they can be cleaned up on Stop.
	transitions []string

	mu       sync.Mutex
	grpcLog  []GRPCCall
	regState *registerapi.RegistrationStatus
}

// PluginOptions configures StartPlugin. When a listener function is nil,
// the plugin creates a Unix domain socket directly (the standalone binary
// mode). When set, the provided function is called instead (the proxy
// mode used by e2e tests).
type PluginOptions struct {
	// PluginListener, if set, is called to create the listener for the
	// SLM gRPC service socket. The endpoint argument is the full path
	// where the socket should appear (e.g. /var/lib/kubelet/plugins/<driver>/slm.sock).
	PluginListener ListenerFunc

	// RegistrarListener, if set, is called to create the listener for
	// the kubelet plugin registration socket.
	RegistrarListener ListenerFunc

	// PluginDataDirectoryPath overrides the base directory for the SLM socket.
	PluginDataDirectoryPath string

	// RegistrarDirectoryPath overrides the directory for the registration socket.
	RegistrarDirectoryPath string

	// AdvertisedEndpoint, when set, overrides the endpoint returned in
	// plugin registration GetInfo(). This is useful for tests that need
	// kubelet registration to succeed while dialing a different path.
	AdvertisedEndpoint string

	// DisableSLMServer skips starting the SLM gRPC server while still
	// running plugin registration.
	DisableSLMServer bool

	// FailStartCallback forces StartLifecycleTransition to return an error.
	FailStartCallback bool

	// FailEndCallback forces EndLifecycleTransition to return an error.
	FailEndCallback bool
}

// StartPlugin creates and starts all components of the SLM test plugin:
//  1. Creates the desired LifecycleTransition objects in the API server.
//  2. A gRPC server implementing SLMPluginServer (StartLifecycleTransition / EndLifecycleTransition).
//  3. A gRPC registration server that the kubelet plugin watcher discovers.
//
// The opts parameter is optional — pass nil for standalone operation.
func StartPlugin(
	ctx context.Context,
	driverName string,
	kubeClient kubernetes.Interface,
	nodeName string,
	transitions []TransitionSpec,
	opts *PluginOptions,
) (*SLMTestPlugin, error) {
	logger := klog.FromContext(ctx)

	if opts == nil {
		opts = &PluginOptions{}
	}

	p := &SLMTestPlugin{
		driverName: driverName,
		nodeName:   nodeName,
		kubeClient: kubeClient,
	}

	// 1. Create LifecycleTransition objects
	for _, ts := range transitions {
		obj := &lifecycleapi.LifecycleTransition{
			ObjectMeta: metav1.ObjectMeta{
				Name: ts.Name,
			},
			Spec: lifecycleapi.LifecycleTransitionSpec{
				Start:    ts.Start,
				End:      ts.End,
				NodeName: ts.NodeName,
				Driver:   driverName,
			},
		}
		created, err := kubeClient.LifecycleV1alpha1().LifecycleTransitions().Create(ctx, obj, metav1.CreateOptions{})
		if apierrors.IsAlreadyExists(err) {
			logger.Info("LifecycleTransition already exists, updating", "name", ts.Name)
			existing, getErr := kubeClient.LifecycleV1alpha1().LifecycleTransitions().Get(ctx, ts.Name, metav1.GetOptions{})
			if getErr != nil {
				return nil, fmt.Errorf("get existing LifecycleTransition %q: %w", ts.Name, getErr)
			}
			existing.Spec = obj.Spec
			if _, err := kubeClient.LifecycleV1alpha1().LifecycleTransitions().Update(ctx, existing, metav1.UpdateOptions{}); err != nil {
				return nil, fmt.Errorf("update LifecycleTransition %q: %w", ts.Name, err)
			}
		} else if err != nil {
			return nil, fmt.Errorf("create LifecycleTransition %q: %w", ts.Name, err)
		} else {
			logger.Info("Created LifecycleTransition", "name", created.Name, "driver", driverName, "start", ts.Start, "end", ts.End)
		}
		p.transitions = append(p.transitions, ts.Name)
	}

	// 2. Start the SLM gRPC service
	datadir := opts.PluginDataDirectoryPath
	if datadir == "" {
		datadir = path.Join("/var/lib/kubelet/plugins", driverName)
	}
	p.slmEndpoint = path.Join(datadir, "slm.sock")

	if !opts.DisableSLMServer {
		slmListener, err := openListener(ctx, p.slmEndpoint, opts.PluginListener)
		if err != nil {
			return nil, fmt.Errorf("listen for SLM service: %w", err)
		}
		p.slmServer = grpc.NewServer(grpc.UnaryInterceptor(p.recordGRPCCall))
		slmpbv1alpha1.RegisterSLMPluginServer(p.slmServer, &slmService{
			driverName:        driverName,
			nodeName:          nodeName,
			failStartCallback: opts.FailStartCallback,
			failEndCallback:   opts.FailEndCallback,
		})
		go func() {
			logger.Info("SLM gRPC service started", "endpoint", p.slmEndpoint)
			if err := p.slmServer.Serve(slmListener); err != nil {
				logger.Error(err, "SLM gRPC server failed")
			}
		}()
	} else {
		logger.Info("SLM gRPC service intentionally disabled for this plugin instance", "endpoint", p.slmEndpoint)
	}

	// 3. Start the kubelet registration server
	registryDir := opts.RegistrarDirectoryPath
	if registryDir == "" {
		registryDir = "/var/lib/kubelet/plugins_registry"
	}
	registrationSocket := filepath.Join(registryDir, driverName+"-reg.sock")

	registrationEndpoint := p.slmEndpoint
	if opts.AdvertisedEndpoint != "" {
		registrationEndpoint = opts.AdvertisedEndpoint
	}

	regListener, err := openListener(ctx, registrationSocket, opts.RegistrarListener)
	if err != nil {
		if p.slmServer != nil {
			p.slmServer.Stop()
		}
		return nil, fmt.Errorf("listen for registration: %w", err)
	}
	p.registrationServer = grpc.NewServer()
	registerapi.RegisterRegistrationServer(p.registrationServer, &registrationService{
		plugin:            p,
		driverName:        driverName,
		endpoint:          registrationEndpoint,
		supportedVersions: []string{slmpbv1alpha1.SLMPluginService},
	})
	go func() {
		logger.Info("Registration server started", "socket", registrationSocket)
		if err := p.registrationServer.Serve(regListener); err != nil {
			logger.Error(err, "Registration gRPC server failed")
		}
	}()

	logger.Info("SLM test plugin started",
		"driverName", driverName,
		"nodeName", nodeName,
		"slmEndpoint", p.slmEndpoint,
		"registrationSocket", registrationSocket,
		"transitions", len(transitions),
	)
	return p, nil
}

// openListener creates a net.Listener either via the provided ListenerFunc
// (proxy mode) or by creating a local Unix domain socket (standalone mode).
func openListener(ctx context.Context, endpoint string, fn ListenerFunc) (net.Listener, error) {
	if fn != nil {
		return fn(ctx, endpoint)
	}
	return listen(endpoint)
}

// Stop gracefully shuts down all components and deletes the
// LifecycleTransition objects the driver created.
func (p *SLMTestPlugin) Stop() {
	logger := klog.Background()

	// Delete LifecycleTransitions created by this driver.
	for _, name := range p.transitions {
		if err := p.kubeClient.LifecycleV1alpha1().LifecycleTransitions().Delete(context.Background(), name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			logger.Error(err, "Failed to delete LifecycleTransition", "name", name)
		} else {
			logger.Info("Deleted LifecycleTransition", "name", name)
		}
	}

	if p.registrationServer != nil {
		p.registrationServer.GracefulStop()
	}
	if p.slmServer != nil {
		p.slmServer.GracefulStop()
	}
}

// GetGRPCCalls returns a copy of all gRPC calls recorded by the plugin.
func (p *SLMTestPlugin) GetGRPCCalls() []GRPCCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]GRPCCall, len(p.grpcLog))
	copy(out, p.grpcLog)
	return out
}

// IsRegistered returns true if the kubelet has confirmed registration.
func (p *SLMTestPlugin) IsRegistered() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.regState != nil && p.regState.PluginRegistered
}

// recordGRPCCall is a gRPC unary interceptor that logs each call.
func (p *SLMTestPlugin) recordGRPCCall(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	resp, err := handler(ctx, req)
	p.mu.Lock()
	p.grpcLog = append(p.grpcLog, GRPCCall{Method: info.FullMethod, Err: err})
	p.mu.Unlock()
	return resp, err
}

// SLM gRPC service implementation
//
// slmService implements the SLMPluginServer interface. The driver simply
// returns the lifecycle condition and node name in the response. The kubelet's
// SLMPlugin wrapper automatically patches the Node condition after a
// successful response -- individual drivers do not need to do this.
type slmService struct {
	slmpbv1alpha1.UnimplementedSLMPluginServer
	driverName        string
	nodeName          string
	failStartCallback bool
	failEndCallback   bool
}

func (s *slmService) StartLifecycleTransition(ctx context.Context, req *slmpbv1alpha1.StartLifecycleTransitionRequest) (*slmpbv1alpha1.LifecycleTransitionResponse, error) {
	logger := klog.FromContext(ctx)
	logger.Info("StartLifecycleTransition called",
		"transitionName", req.GetTransitionName(),
		"start", req.GetStart(),
		"end", req.GetEnd(),
		"nodeName", req.GetNodeName(),
	)

	targetNode := req.GetNodeName()
	if targetNode == "" {
		targetNode = s.nodeName
	}
	if s.failStartCallback {
		return nil, fmt.Errorf("injected StartLifecycleTransition failure for driver %q", s.driverName)
	}

	return &slmpbv1alpha1.LifecycleTransitionResponse{
		LifecycleCondition: req.GetStart(),
		NodeName:           targetNode,
	}, nil
}

func (s *slmService) EndLifecycleTransition(ctx context.Context, req *slmpbv1alpha1.EndLifecycleTransitionRequest) (*slmpbv1alpha1.LifecycleTransitionResponse, error) {
	logger := klog.FromContext(ctx)
	logger.Info("EndLifecycleTransition called",
		"transitionName", req.GetTransitionName(),
		"nodeName", req.GetNodeName(),
		"end", req.GetEnd(),
	)

	targetNode := req.GetNodeName()
	if targetNode == "" {
		targetNode = s.nodeName
	}
	if s.failEndCallback {
		return nil, fmt.Errorf("injected EndLifecycleTransition failure for driver %q", s.driverName)
	}

	return &slmpbv1alpha1.LifecycleTransitionResponse{
		LifecycleCondition: req.GetEnd(),
		NodeName:           targetNode,
	}, nil
}

// Kubelet plugin registration service
//
// registrationService implements the kubelet Registration gRPC interface
// so the plugin watcher discovers this driver.
type registrationService struct {
	registerapi.UnimplementedRegistrationServer
	plugin            *SLMTestPlugin
	driverName        string
	endpoint          string
	supportedVersions []string
}

func (r *registrationService) GetInfo(ctx context.Context, req *registerapi.InfoRequest) (*registerapi.PluginInfo, error) {
	klog.FromContext(ctx).Info("GetInfo called", "driverName", r.driverName)
	return &registerapi.PluginInfo{
		Type:              registerapi.SLMPlugin,
		Name:              r.driverName,
		Endpoint:          r.endpoint,
		SupportedVersions: r.supportedVersions,
	}, nil
}

func (r *registrationService) NotifyRegistrationStatus(ctx context.Context, status *registerapi.RegistrationStatus) (*registerapi.RegistrationStatusResponse, error) {
	logger := klog.FromContext(ctx)

	r.plugin.mu.Lock()
	r.plugin.regState = status
	r.plugin.mu.Unlock()

	if !status.PluginRegistered {
		logger.Error(nil, "Registration failed", "error", status.Error)
		return nil, fmt.Errorf("registration failed: %s", status.Error)
	}
	logger.Info("Successfully registered with kubelet")
	return &registerapi.RegistrationStatusResponse{}, nil
}
