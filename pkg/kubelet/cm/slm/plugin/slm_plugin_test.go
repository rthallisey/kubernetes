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
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	slmpbv1alpha1 "k8s.io/kubelet/pkg/apis/slm/v1alpha1"
)

type fakeSLMServer struct {
	slmpbv1alpha1.UnimplementedSLMPluginServer
	startResp *slmpbv1alpha1.LifecycleTransitionResponse
	endResp   *slmpbv1alpha1.LifecycleTransitionResponse
}

func (s *fakeSLMServer) StartLifecycleTransition(context.Context, *slmpbv1alpha1.StartLifecycleTransitionRequest) (*slmpbv1alpha1.LifecycleTransitionResponse, error) {
	return s.startResp, nil
}

func (s *fakeSLMServer) EndLifecycleTransition(context.Context, *slmpbv1alpha1.EndLifecycleTransitionRequest) (*slmpbv1alpha1.LifecycleTransitionResponse, error) {
	return s.endResp, nil
}

func startLocalSLMServer(t *testing.T, srv slmpbv1alpha1.SLMPluginServer) (*grpc.ClientConn, func()) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen local grpc server: %v", err)
	}
	server := grpc.NewServer()
	slmpbv1alpha1.RegisterSLMPluginServer(server, srv)
	go func() {
		_ = server.Serve(lis)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	conn, err := grpc.DialContext(ctx, lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	cancel()
	if err != nil {
		t.Fatalf("dial local grpc server: %v", err)
	}

	cleanup := func() {
		_ = conn.Close()
		server.Stop()
		_ = lis.Close()
	}
	return conn, cleanup
}

func TestStartLifecycleTransitionReturnsDriverErrorFromResponse(t *testing.T) {
	conn, cleanup := startLocalSLMServer(t, &fakeSLMServer{
		startResp: &slmpbv1alpha1.LifecycleTransitionResponse{
			Error: "driver-start-failure",
		},
	})
	defer cleanup()

	p := &SLMPlugin{
		driverName:        "test-driver",
		endpoint:          "bufnet",
		conn:              conn,
		chosenService:     slmpbv1alpha1.SLMPluginService,
		clientCallTimeout: time.Second,
	}

	resp, err := p.StartLifecycleTransition(context.Background(), &slmpbv1alpha1.StartLifecycleTransitionRequest{})
	if err == nil {
		t.Fatalf("expected error when driver returns response.error, got nil")
	}
	if resp == nil || resp.GetError() != "driver-start-failure" {
		t.Fatalf("unexpected response: %#v", resp)
	}
	if !strings.Contains(err.Error(), "driver-start-failure") {
		t.Fatalf("expected wrapped error to contain driver error, got: %v", err)
	}
}

func TestEndLifecycleTransitionReturnsDriverErrorFromResponse(t *testing.T) {
	conn, cleanup := startLocalSLMServer(t, &fakeSLMServer{
		endResp: &slmpbv1alpha1.LifecycleTransitionResponse{
			Error: "driver-end-failure",
		},
	})
	defer cleanup()

	p := &SLMPlugin{
		driverName:        "test-driver",
		endpoint:          "bufnet",
		conn:              conn,
		chosenService:     slmpbv1alpha1.SLMPluginService,
		clientCallTimeout: time.Second,
	}

	resp, err := p.EndLifecycleTransition(context.Background(), &slmpbv1alpha1.EndLifecycleTransitionRequest{})
	if err == nil {
		t.Fatalf("expected error when driver returns response.error, got nil")
	}
	if resp == nil || resp.GetError() != "driver-end-failure" {
		t.Fatalf("unexpected response: %#v", resp)
	}
	if !strings.Contains(err.Error(), "driver-end-failure") {
		t.Fatalf("expected wrapped error to contain driver error, got: %v", err)
	}
}
