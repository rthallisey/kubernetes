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

// Package slm contains end-to-end tests for Specialized Lifecycle Management.
//
// These tests use the DRA-style proxy pattern: the SLM test driver runs
// in the e2e test process while a lightweight proxy pod inside the cluster
// bridges the Unix domain sockets that the kubelet expects.
//
// Prerequisites:
//   - A cluster with the SpecializedLifecycleManagement feature gate enabled.
//   - The lifecycle.k8s.io/v1alpha1 API enabled via --runtime-config.
//
// Run:
//
//	go test ./test/e2e/slm/ -v -timeout 10m \
//	  --kubeconfig=$KUBECONFIG \
//	  --repo-root=$(pwd)
package slm

import (
	"context"
	"flag"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	v1 "k8s.io/api/core/v1"
	lifecycleapi "k8s.io/api/lifecycle/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/test/e2e/framework"
	"k8s.io/kubernetes/test/e2e/framework/config"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	e2essh "k8s.io/kubernetes/test/e2e/framework/ssh"
	"k8s.io/kubernetes/test/e2e/framework/testfiles"
	"k8s.io/kubernetes/test/e2e/slm/test-driver/app"
	slmutils "k8s.io/kubernetes/test/e2e/slm/utils"
	"k8s.io/kubernetes/test/utils/ktesting"
	admissionapi "k8s.io/pod-security-admission/api"
	"k8s.io/utils/ptr"

	slmplugin "k8s.io/kubernetes/pkg/kubelet/cm/slm/plugin"

	// Reconfigure framework output.
	_ "k8s.io/kubernetes/test/e2e/framework/debug/init"
	_ "k8s.io/kubernetes/test/e2e/framework/metrics/init"
	_ "k8s.io/kubernetes/test/e2e/framework/node/init"
	_ "k8s.io/kubernetes/test/utils/format"
)

func TestMain(m *testing.M) {
	// Register framework flags (--kubeconfig, --kubelet-root-dir, etc.).
	config.CopyFlags(config.Flags, flag.CommandLine)
	framework.RegisterCommonFlags(flag.CommandLine)
	framework.RegisterClusterFlags(flag.CommandLine)
	flag.Parse()
	framework.AfterReadingAllFlags(&framework.TestContext)

	// Enable loading manifests from the repo root so that
	// test/e2e/testing-manifests/slm/ can be found.
	if framework.TestContext.RepoRoot != "" {
		testfiles.AddFileSource(testfiles.RootFileSource{Root: framework.TestContext.RepoRoot})
	}

	os.Exit(m.Run())
}

func TestSLM(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "SLM Suite")
}

var _ = ginkgo.Describe("SLM", func() {
	f := framework.NewDefaultFramework("slm")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	ginkgo.It("should keep a lifecycle event pending across multiple reconcile intervals when no driver is registered", func(ctx context.Context) {
		tCtx := f.TContext(ctx)

		// Select nodes.
		nodes := slmutils.NewNodes(tCtx, 1, 3)
		workerNode := nodes.NodeNames[0]

		transitionName := "test-no-driver-transition-" + workerNode
		eventName := "test-no-driver-event-" + workerNode

		transition := &lifecycleapi.LifecycleTransition{
			ObjectMeta: metav1.ObjectMeta{Name: transitionName},
			Spec: lifecycleapi.LifecycleTransitionSpec{
				Start:    "test-no-driver-start",
				End:      "test-no-driver-end",
				NodeName: &workerNode,
				Driver:   "missing-driver.example.com",
			},
		}
		_, err := tCtx.Client().LifecycleV1alpha1().LifecycleTransitions().Create(tCtx, transition, metav1.CreateOptions{})
		tCtx.ExpectNoError(err, "create LifecycleTransition")
		tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
			_ = tCtx.Client().LifecycleV1alpha1().LifecycleTransitions().Delete(tCtx, transitionName, metav1.DeleteOptions{})
		})

		event := &lifecycleapi.LifecycleEvent{
			ObjectMeta: metav1.ObjectMeta{Name: eventName},
			Spec: lifecycleapi.LifecycleEventSpec{
				TransitionName: transitionName,
				BindingNode:    workerNode,
			},
		}
		_, err = tCtx.Client().LifecycleV1alpha1().LifecycleEvents().Create(tCtx, event, metav1.CreateOptions{})
		tCtx.ExpectNoError(err, "create LifecycleEvent")
		tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
			_ = tCtx.Client().LifecycleV1alpha1().LifecycleEvents().Delete(tCtx, eventName, metav1.DeleteOptions{})
		})

		// Reconcile runs every 10s in the kubelet SLM event controller.
		// Hold observation for >3 loops to ensure we cover multiple intervals.
		tCtx.Consistently(func(tCtx ktesting.TContext) string {
			ev, err := tCtx.Client().LifecycleV1alpha1().LifecycleEvents().Get(tCtx, eventName, metav1.GetOptions{})
			if err != nil {
				return ""
			}
			return string(ev.Status.ClaimStatus)
		}).WithTimeout(35 * time.Second).Should(gomega.Equal(string(lifecycleapi.LifecycleEventPending)))
	})

	ginkgo.It("should claim a pending lifecycle event after a matching driver registers later", func(ctx context.Context) {
		tCtx := f.TContext(ctx)

		nodes := slmutils.NewNodes(tCtx, 1, 3)
		workerNode := nodes.NodeNames[0]

		driver := slmutils.NewDriverInstance(tCtx)
		driver.Transitions = []app.TransitionSpec{
			{
				Name:     driver.Name + "-" + workerNode,
				Start:    "test-late-driver-start",
				End:      "test-late-driver-end",
				NodeName: &workerNode,
			},
		}

		transitionName := driver.Name + "-" + workerNode
		eventName := "test-late-driver-event-" + workerNode

		transition := &lifecycleapi.LifecycleTransition{
			ObjectMeta: metav1.ObjectMeta{Name: transitionName},
			Spec: lifecycleapi.LifecycleTransitionSpec{
				Start:    "test-late-driver-start",
				End:      "test-late-driver-end",
				NodeName: &workerNode,
				Driver:   driver.Name,
			},
		}
		_, err := tCtx.Client().LifecycleV1alpha1().LifecycleTransitions().Create(tCtx, transition, metav1.CreateOptions{})
		tCtx.ExpectNoError(err, "create LifecycleTransition")
		tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
			_ = tCtx.Client().LifecycleV1alpha1().LifecycleTransitions().Delete(tCtx, transitionName, metav1.DeleteOptions{})
		})

		event := &lifecycleapi.LifecycleEvent{
			ObjectMeta: metav1.ObjectMeta{Name: eventName},
			Spec: lifecycleapi.LifecycleEventSpec{
				TransitionName: transitionName,
				BindingNode:    workerNode,
			},
		}
		_, err = tCtx.Client().LifecycleV1alpha1().LifecycleEvents().Create(tCtx, event, metav1.CreateOptions{})
		tCtx.ExpectNoError(err, "create LifecycleEvent")
		tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
			_ = tCtx.Client().LifecycleV1alpha1().LifecycleEvents().Delete(tCtx, eventName, metav1.DeleteOptions{})
		})

		// Before driver registration, event should remain Pending.
		tCtx.Consistently(func(tCtx ktesting.TContext) string {
			ev, err := tCtx.Client().LifecycleV1alpha1().LifecycleEvents().Get(tCtx, eventName, metav1.GetOptions{})
			if err != nil {
				return ""
			}
			return string(ev.Status.ClaimStatus)
		}).WithTimeout(20 * time.Second).Should(gomega.Equal(string(lifecycleapi.LifecycleEventPending)))

		// Register/start driver after event already exists.
		driver.Run(tCtx, framework.TestContext.KubeletRootDir, nodes)

		// Event should eventually be claimed by kubelet now that a driver exists.
		tCtx.Eventually(func(tCtx ktesting.TContext) string {
			ev, err := tCtx.Client().LifecycleV1alpha1().LifecycleEvents().Get(tCtx, eventName, metav1.GetOptions{})
			if err != nil {
				return ""
			}
			return string(ev.Status.ClaimStatus)
		}).WithTimeout(2 * time.Minute).Should(gomega.Equal(string(lifecycleapi.LifecycleEventClaimed)))
	})

	ginkgo.It("should fail and delete a lifecycle event when a registered driver never becomes ready", func(ctx context.Context) {
		tCtx := f.TContext(ctx)

		nodes := slmutils.NewNodes(tCtx, 1, 3)
		workerNode := nodes.NodeNames[0]

		driver := slmutils.NewDriverInstance(tCtx)
		driver.Transitions = []app.TransitionSpec{
			{
				Start: "test-unready-driver-start",
				End:   "test-unready-driver-end",
			},
		}
		// Register with kubelet, but intentionally do not start the SLM gRPC service.
		driver.PluginOptionsMutator = func(opts *app.PluginOptions, nodeName string) {
			opts.DisableSLMServer = true
		}
		driver.Run(tCtx, framework.TestContext.KubeletRootDir, nodes)

		transitionName := driver.Name + "-" + workerNode
		eventName := "test-unready-driver-event-" + workerNode

		// Transition should already exist from driver startup.
		tCtx.Eventually(func(tCtx ktesting.TContext) error {
			_, err := tCtx.Client().LifecycleV1alpha1().LifecycleTransitions().Get(tCtx, transitionName, metav1.GetOptions{})
			return err
		}).WithTimeout(30*time.Second).Should(gomega.Succeed(), "LifecycleTransition should exist")

		event := &lifecycleapi.LifecycleEvent{
			ObjectMeta: metav1.ObjectMeta{Name: eventName},
			Spec: lifecycleapi.LifecycleEventSpec{
				TransitionName: transitionName,
				BindingNode:    workerNode,
			},
		}
		_, err := tCtx.Client().LifecycleV1alpha1().LifecycleEvents().Create(tCtx, event, metav1.CreateOptions{})
		tCtx.ExpectNoError(err, "create LifecycleEvent")
		tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
			_ = tCtx.Client().LifecycleV1alpha1().LifecycleEvents().Delete(tCtx, eventName, metav1.DeleteOptions{})
		})

		// Claim timeout is 30s; event should remain Pending for a short initial window.
		tCtx.Consistently(func(tCtx ktesting.TContext) string {
			ev, err := tCtx.Client().LifecycleV1alpha1().LifecycleEvents().Get(tCtx, eventName, metav1.GetOptions{})
			if err != nil {
				return ""
			}
			return string(ev.Status.ClaimStatus)
		}).WithTimeout(20 * time.Second).Should(gomega.Equal(string(lifecycleapi.LifecycleEventPending)))

		// After claim timeout, kubelet should transition to Failed and delete the event.
		tCtx.Eventually(func(tCtx ktesting.TContext) bool {
			_, err := tCtx.Client().LifecycleV1alpha1().LifecycleEvents().Get(tCtx, eventName, metav1.GetOptions{})
			return apierrors.IsNotFound(err)
		}).WithTimeout(2 * time.Minute).Should(gomega.BeTrue())
	})

	ginkgo.It("should claim at most one lifecycle event at a time when multiple are pending", func(ctx context.Context) {
		tCtx := f.TContext(ctx)

		nodes := slmutils.NewNodes(tCtx, 1, 3)
		workerNode := nodes.NodeNames[0]

		driver := slmutils.NewDriverInstance(tCtx)
		driver.Transitions = []app.TransitionSpec{
			{
				Start: "test-serial-claim-start",
				End:   "test-serial-claim-end",
			},
		}
		driver.Run(tCtx, framework.TestContext.KubeletRootDir, nodes)

		transitionName := driver.Name + "-" + workerNode
		tCtx.Eventually(func(tCtx ktesting.TContext) error {
			_, err := tCtx.Client().LifecycleV1alpha1().LifecycleTransitions().Get(tCtx, transitionName, metav1.GetOptions{})
			return err
		}).WithTimeout(30*time.Second).Should(gomega.Succeed(), "LifecycleTransition should exist")

		eventName1 := "test-serial-claim-event-1-" + workerNode
		eventName2 := "test-serial-claim-event-2-" + workerNode
		for _, eventName := range []string{eventName1, eventName2} {
			event := &lifecycleapi.LifecycleEvent{
				ObjectMeta: metav1.ObjectMeta{Name: eventName},
				Spec: lifecycleapi.LifecycleEventSpec{
					TransitionName: transitionName,
					BindingNode:    workerNode,
				},
			}
			_, err := tCtx.Client().LifecycleV1alpha1().LifecycleEvents().Create(tCtx, event, metav1.CreateOptions{})
			tCtx.ExpectNoError(err, "create LifecycleEvent %s", eventName)
			tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
				_ = tCtx.Client().LifecycleV1alpha1().LifecycleEvents().Delete(tCtx, eventName, metav1.DeleteOptions{})
			})
		}

		// Eventually we should see exactly one claimed and one still pending.
		tCtx.Eventually(func(tCtx ktesting.TContext) bool {
			claimed, pending := countClaimedAndPending(tCtx, workerNode)
			return claimed == 1 && pending == 1
		}).WithTimeout(2 * time.Minute).Should(gomega.BeTrue())

		// Across multiple reconcile loops, kubelet should never hold >1 claimed event.
		tCtx.Consistently(func(tCtx ktesting.TContext) bool {
			claimed, _ := countClaimedAndPending(tCtx, workerNode)
			return claimed <= 1
		}).WithTimeout(40 * time.Second).Should(gomega.BeTrue())
	})

	ginkgo.It("should resume a claimed lifecycle event after kubelet restart without double-claiming", func(ctx context.Context) {
		e2eskipper.SkipUnlessProviderIs(framework.ProvidersWithSSH...)

		tCtx := f.TContext(ctx)
		nodes := slmutils.NewNodes(tCtx, 1, 3)
		workerNode := nodes.NodeNames[0]

		driver := slmutils.NewDriverInstance(tCtx)
		driver.Transitions = []app.TransitionSpec{
			{
				Start: "test-kubelet-restart-start",
				End:   "test-kubelet-restart-end",
			},
		}
		driver.Run(tCtx, framework.TestContext.KubeletRootDir, nodes)

		transitionName := driver.Name + "-" + workerNode
		tCtx.Eventually(func(tCtx ktesting.TContext) error {
			_, err := tCtx.Client().LifecycleV1alpha1().LifecycleTransitions().Get(tCtx, transitionName, metav1.GetOptions{})
			return err
		}).WithTimeout(30*time.Second).Should(gomega.Succeed(), "LifecycleTransition should exist")

		eventA := "test-kubelet-restart-event-a-" + workerNode
		eventB := "test-kubelet-restart-event-b-" + workerNode
		for _, eventName := range []string{eventA, eventB} {
			event := &lifecycleapi.LifecycleEvent{
				ObjectMeta: metav1.ObjectMeta{Name: eventName},
				Spec: lifecycleapi.LifecycleEventSpec{
					TransitionName: transitionName,
					BindingNode:    workerNode,
				},
			}
			_, err := tCtx.Client().LifecycleV1alpha1().LifecycleEvents().Create(tCtx, event, metav1.CreateOptions{})
			tCtx.ExpectNoError(err, "create LifecycleEvent %s", eventName)
			tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
				_ = tCtx.Client().LifecycleV1alpha1().LifecycleEvents().Delete(tCtx, eventName, metav1.DeleteOptions{})
			})
		}

		// Establish the pre-restart invariant: exactly one Claimed, one Pending.
		tCtx.Eventually(func(tCtx ktesting.TContext) bool {
			claimed, pending := countClaimedAndPending(tCtx, workerNode)
			return claimed == 1 && pending == 1
		}).WithTimeout(2 * time.Minute).Should(gomega.BeTrue())

		restartKubeletOverSSH(tCtx, workerNode)

		// During recovery, kubelet must not hold more than one claimed event.
		tCtx.Consistently(func(tCtx ktesting.TContext) bool {
			claimed, _ := countClaimedAndPending(tCtx, workerNode)
			return claimed <= 1
		}).WithTimeout(40 * time.Second).Should(gomega.BeTrue())

		// Ensure post-restart progress continues (some event eventually completes).
		tCtx.Eventually(func(tCtx ktesting.TContext) bool {
			_, errA := tCtx.Client().LifecycleV1alpha1().LifecycleEvents().Get(tCtx, eventA, metav1.GetOptions{})
			_, errB := tCtx.Client().LifecycleV1alpha1().LifecycleEvents().Get(tCtx, eventB, metav1.GetOptions{})
			return apierrors.IsNotFound(errA) || apierrors.IsNotFound(errB)
		}).WithTimeout(3 * time.Minute).Should(gomega.BeTrue())
	})

	ginkgo.It("should transition a claimed lifecycle event to SlaExpired and delete it when SLA elapses", func(ctx context.Context) {
		tCtx := f.TContext(ctx)

		nodes := slmutils.NewNodes(tCtx, 1, 3)
		workerNode := nodes.NodeNames[0]

		driver := slmutils.NewDriverInstance(tCtx)
		// No pre-published transitions; this test creates one with a short SLA.
		driver.SetUp(tCtx, framework.TestContext.KubeletRootDir, nodes)
		tCtx.CleanupCtx(driver.TearDown)

		transitionName := "test-sla-expire-transition-" + workerNode
		eventName := "test-sla-expire-event-" + workerNode
		sla := metav1.Duration{Duration: 35 * time.Second}

		transition := &lifecycleapi.LifecycleTransition{
			ObjectMeta: metav1.ObjectMeta{Name: transitionName},
			Spec: lifecycleapi.LifecycleTransitionSpec{
				Start:    "test-sla-expire-start",
				End:      "test-sla-expire-end",
				NodeName: &workerNode,
				Driver:   driver.Name,
				Sla:      &sla,
			},
		}
		_, err := tCtx.Client().LifecycleV1alpha1().LifecycleTransitions().Create(tCtx, transition, metav1.CreateOptions{})
		tCtx.ExpectNoError(err, "create LifecycleTransition")
		tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
			_ = tCtx.Client().LifecycleV1alpha1().LifecycleTransitions().Delete(tCtx, transitionName, metav1.DeleteOptions{})
		})

		event := &lifecycleapi.LifecycleEvent{
			ObjectMeta: metav1.ObjectMeta{Name: eventName},
			Spec: lifecycleapi.LifecycleEventSpec{
				TransitionName: transitionName,
				BindingNode:    workerNode,
			},
		}
		_, err = tCtx.Client().LifecycleV1alpha1().LifecycleEvents().Create(tCtx, event, metav1.CreateOptions{})
		tCtx.ExpectNoError(err, "create LifecycleEvent")
		tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
			_ = tCtx.Client().LifecycleV1alpha1().LifecycleEvents().Delete(tCtx, eventName, metav1.DeleteOptions{})
		})

		// First, ensure kubelet has claimed the event.
		tCtx.Eventually(func(tCtx ktesting.TContext) string {
			ev, err := tCtx.Client().LifecycleV1alpha1().LifecycleEvents().Get(tCtx, eventName, metav1.GetOptions{})
			if err != nil {
				return ""
			}
			return string(ev.Status.ClaimStatus)
		}).WithTimeout(2 * time.Minute).Should(gomega.Equal(string(lifecycleapi.LifecycleEventClaimed)))

		// Disrupt the plugin transport so claimed event cannot complete.
		// This forces kubelet to hold the claim until SLA expiry handling runs.
		rsName := driver.Name + "-proxy"
		rs, err := tCtx.Client().AppsV1().ReplicaSets(tCtx.Namespace()).Get(tCtx, rsName, metav1.GetOptions{})
		tCtx.ExpectNoError(err, "get SLM proxy ReplicaSet")
		rs.Spec.Replicas = ptr.To(int32(0))
		_, err = tCtx.Client().AppsV1().ReplicaSets(tCtx.Namespace()).Update(tCtx, rs, metav1.UpdateOptions{})
		tCtx.ExpectNoError(err, "scale down SLM proxy ReplicaSet")

		// After SLA elapses, kubelet should mark SlaExpired and delete the event.
		tCtx.Eventually(func(tCtx ktesting.TContext) bool {
			_, err := tCtx.Client().LifecycleV1alpha1().LifecycleEvents().Get(tCtx, eventName, metav1.GetOptions{})
			return apierrors.IsNotFound(err)
		}).WithTimeout(3 * time.Minute).Should(gomega.BeTrue())
	})

	ginkgo.It("should fail and delete a lifecycle event when the driver start callback returns an error", func(ctx context.Context) {
		tCtx := f.TContext(ctx)

		nodes := slmutils.NewNodes(tCtx, 1, 3)
		workerNode := nodes.NodeNames[0]

		driver := slmutils.NewDriverInstance(tCtx)
		driver.Transitions = []app.TransitionSpec{
			{
				Start: "test-start-error-start",
				End:   "test-start-error-end",
			},
		}
		driver.PluginOptionsMutator = func(opts *app.PluginOptions, nodeName string) {
			opts.FailStartCallback = true
		}
		driver.Run(tCtx, framework.TestContext.KubeletRootDir, nodes)

		transitionName := driver.Name + "-" + workerNode
		eventName := "test-start-error-event-" + workerNode

		tCtx.Eventually(func(tCtx ktesting.TContext) error {
			_, err := tCtx.Client().LifecycleV1alpha1().LifecycleTransitions().Get(tCtx, transitionName, metav1.GetOptions{})
			return err
		}).WithTimeout(30*time.Second).Should(gomega.Succeed(), "LifecycleTransition should exist")

		event := &lifecycleapi.LifecycleEvent{
			ObjectMeta: metav1.ObjectMeta{Name: eventName},
			Spec: lifecycleapi.LifecycleEventSpec{
				TransitionName: transitionName,
				BindingNode:    workerNode,
			},
		}
		_, err := tCtx.Client().LifecycleV1alpha1().LifecycleEvents().Create(tCtx, event, metav1.CreateOptions{})
		tCtx.ExpectNoError(err, "create LifecycleEvent")
		tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
			_ = tCtx.Client().LifecycleV1alpha1().LifecycleEvents().Delete(tCtx, eventName, metav1.DeleteOptions{})
		})

		tCtx.Eventually(func(tCtx ktesting.TContext) bool {
			_, err := tCtx.Client().LifecycleV1alpha1().LifecycleEvents().Get(tCtx, eventName, metav1.GetOptions{})
			return apierrors.IsNotFound(err)
		}).WithTimeout(2*time.Minute).Should(gomega.BeTrue(), "event should be deleted after failed start callback")

		tCtx.Eventually(func() bool {
			plugin, ok := driver.Nodes[workerNode]
			if !ok {
				return false
			}
			for _, call := range plugin.GetGRPCCalls() {
				if call.Method == "/v1alpha1.SLMPlugin/StartLifecycleTransition" && call.Err != nil {
					return true
				}
			}
			return false
		}).WithTimeout(30*time.Second).Should(gomega.BeTrue(), "driver should observe failed StartLifecycleTransition callback")
	})

	ginkgo.It("should fail and delete a lifecycle event when the driver end callback returns an error", func(ctx context.Context) {
		tCtx := f.TContext(ctx)

		nodes := slmutils.NewNodes(tCtx, 1, 3)
		workerNode := nodes.NodeNames[0]

		driver := slmutils.NewDriverInstance(tCtx)
		driver.Transitions = []app.TransitionSpec{
			{
				Start: "test-end-error-start",
				End:   "test-end-error-end",
			},
		}
		driver.PluginOptionsMutator = func(opts *app.PluginOptions, nodeName string) {
			opts.FailEndCallback = true
		}
		driver.Run(tCtx, framework.TestContext.KubeletRootDir, nodes)

		transitionName := driver.Name + "-" + workerNode
		eventName := "test-end-error-event-" + workerNode

		tCtx.Eventually(func(tCtx ktesting.TContext) error {
			_, err := tCtx.Client().LifecycleV1alpha1().LifecycleTransitions().Get(tCtx, transitionName, metav1.GetOptions{})
			return err
		}).WithTimeout(30*time.Second).Should(gomega.Succeed(), "LifecycleTransition should exist")

		event := &lifecycleapi.LifecycleEvent{
			ObjectMeta: metav1.ObjectMeta{Name: eventName},
			Spec: lifecycleapi.LifecycleEventSpec{
				TransitionName: transitionName,
				BindingNode:    workerNode,
			},
		}
		_, err := tCtx.Client().LifecycleV1alpha1().LifecycleEvents().Create(tCtx, event, metav1.CreateOptions{})
		tCtx.ExpectNoError(err, "create LifecycleEvent")
		tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
			_ = tCtx.Client().LifecycleV1alpha1().LifecycleEvents().Delete(tCtx, eventName, metav1.DeleteOptions{})
		})

		tCtx.Eventually(func(tCtx ktesting.TContext) bool {
			_, err := tCtx.Client().LifecycleV1alpha1().LifecycleEvents().Get(tCtx, eventName, metav1.GetOptions{})
			return apierrors.IsNotFound(err)
		}).WithTimeout(2*time.Minute).Should(gomega.BeTrue(), "event should be deleted after failed end callback")

		tCtx.Eventually(func() bool {
			plugin, ok := driver.Nodes[workerNode]
			if !ok {
				return false
			}
			for _, call := range plugin.GetGRPCCalls() {
				if call.Method == "/v1alpha1.SLMPlugin/EndLifecycleTransition" && call.Err != nil {
					return true
				}
			}
			return false
		}).WithTimeout(30*time.Second).Should(gomega.BeTrue(), "driver should observe failed EndLifecycleTransition callback")
	})

	ginkgo.It("should add kubelet claim finalizer on claim and remove it on terminal failed state", func(ctx context.Context) {
		tCtx := f.TContext(ctx)

		nodes := slmutils.NewNodes(tCtx, 1, 3)
		workerNode := nodes.NodeNames[0]

		driver := slmutils.NewDriverInstance(tCtx)
		driver.Transitions = []app.TransitionSpec{
			{
				Start: "test-finalizer-start",
				End:   "test-finalizer-end",
			},
		}
		driver.Run(tCtx, framework.TestContext.KubeletRootDir, nodes)

		transitionName := driver.Name + "-" + workerNode
		tCtx.Eventually(func(tCtx ktesting.TContext) error {
			_, err := tCtx.Client().LifecycleV1alpha1().LifecycleTransitions().Get(tCtx, transitionName, metav1.GetOptions{})
			return err
		}).WithTimeout(30*time.Second).Should(gomega.Succeed(), "LifecycleTransition should exist")

		// verify kubelet adds the claim-protection finalizer once event is claimed.
		claimedEventName := "test-finalizer-claimed-event-" + workerNode
		claimedEvent := &lifecycleapi.LifecycleEvent{
			ObjectMeta: metav1.ObjectMeta{Name: claimedEventName},
			Spec: lifecycleapi.LifecycleEventSpec{
				TransitionName: transitionName,
				BindingNode:    workerNode,
			},
		}
		_, err := tCtx.Client().LifecycleV1alpha1().LifecycleEvents().Create(tCtx, claimedEvent, metav1.CreateOptions{})
		tCtx.ExpectNoError(err, "create LifecycleEvent")
		tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
			_ = tCtx.Client().LifecycleV1alpha1().LifecycleEvents().Delete(tCtx, claimedEventName, metav1.DeleteOptions{})
		})

		tCtx.Eventually(func(tCtx ktesting.TContext) bool {
			ev, err := tCtx.Client().LifecycleV1alpha1().LifecycleEvents().Get(tCtx, claimedEventName, metav1.GetOptions{})
			if err != nil {
				return false
			}
			return ev.Status.ClaimStatus == lifecycleapi.LifecycleEventClaimed &&
				slices.Contains(ev.Finalizers, "lifecycle.k8s.io/claim-protection")
		}).WithTimeout(2*time.Minute).Should(gomega.BeTrue(), "claimed event should have kubelet claim finalizer")

		// verify kubelet removes its finalizer when event reaches Failed.
		// Use a second driver instance with deterministic start-callback failure.
		failDriver := slmutils.NewDriverInstance(tCtx)
		failDriver.Transitions = []app.TransitionSpec{
			{
				Start: "test-finalizer-fail-start",
				End:   "test-finalizer-fail-end",
			},
		}
		failDriver.PluginOptionsMutator = func(opts *app.PluginOptions, nodeName string) {
			opts.FailStartCallback = true
		}
		failDriver.Run(tCtx, framework.TestContext.KubeletRootDir, nodes)

		failTransitionName := failDriver.Name + "-" + workerNode
		tCtx.Eventually(func(tCtx ktesting.TContext) error {
			_, err := tCtx.Client().LifecycleV1alpha1().LifecycleTransitions().Get(tCtx, failTransitionName, metav1.GetOptions{})
			return err
		}).WithTimeout(30*time.Second).Should(gomega.Succeed(), "failing LifecycleTransition should exist")

		failEventName := "test-finalizer-failed-event-" + workerNode
		holdFinalizer := "e2e.slm.k8s.io/hold-delete"
		failEvent := &lifecycleapi.LifecycleEvent{
			ObjectMeta: metav1.ObjectMeta{
				Name:       failEventName,
				Finalizers: []string{holdFinalizer},
			},
			Spec: lifecycleapi.LifecycleEventSpec{
				TransitionName: failTransitionName,
				BindingNode:    workerNode,
			},
		}
		_, err = tCtx.Client().LifecycleV1alpha1().LifecycleEvents().Create(tCtx, failEvent, metav1.CreateOptions{})
		tCtx.ExpectNoError(err, "create failing LifecycleEvent")
		tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
			ev, err := tCtx.Client().LifecycleV1alpha1().LifecycleEvents().Get(tCtx, failEventName, metav1.GetOptions{})
			if err != nil {
				return
			}
			ev = ev.DeepCopy()
			ev.Finalizers = slices.DeleteFunc(ev.Finalizers, func(f string) bool { return f == holdFinalizer })
			_, _ = tCtx.Client().LifecycleV1alpha1().LifecycleEvents().Update(tCtx, ev, metav1.UpdateOptions{})
			_ = tCtx.Client().LifecycleV1alpha1().LifecycleEvents().Delete(tCtx, failEventName, metav1.DeleteOptions{})
		})

		tCtx.Eventually(func(tCtx ktesting.TContext) bool {
			ev, err := tCtx.Client().LifecycleV1alpha1().LifecycleEvents().Get(tCtx, failEventName, metav1.GetOptions{})
			if err != nil {
				return false
			}
			return ev.Status.ClaimStatus == lifecycleapi.LifecycleEventFailed &&
				!slices.Contains(ev.Finalizers, "lifecycle.k8s.io/claim-protection") &&
				slices.Contains(ev.Finalizers, holdFinalizer) &&
				ev.DeletionTimestamp != nil
		}).WithTimeout(2*time.Minute).Should(gomega.BeTrue(), "failed event should have kubelet finalizer removed before delete completion")
	})

	ginkgo.It("should delete node-scoped lifecycle transitions after driver deregistration cleanup delay", func(ctx context.Context) {
		tCtx := f.TContext(ctx)

		nodes := slmutils.NewNodes(tCtx, 1, 1)
		workerNode := nodes.NodeNames[0]

		driver := slmutils.NewDriverInstance(tCtx)
		driver.Transitions = []app.TransitionSpec{
			{
				Start:    "test-deregister-cleanup-start",
				End:      "test-deregister-cleanup-end",
				NodeName: ptr.To(workerNode),
			},
		}
		driver.Run(tCtx, framework.TestContext.KubeletRootDir, nodes)

		transitionName := driver.Name + "-" + workerNode
		tCtx.Eventually(func(tCtx ktesting.TContext) error {
			_, err := tCtx.Client().LifecycleV1alpha1().LifecycleTransitions().Get(tCtx, transitionName, metav1.GetOptions{})
			return err
		}).WithTimeout(30*time.Second).Should(gomega.Succeed(), "LifecycleTransition should exist before deregistration")
		transition, err := tCtx.Client().LifecycleV1alpha1().LifecycleTransitions().Get(tCtx, transitionName, metav1.GetOptions{})
		tCtx.ExpectNoError(err, "get LifecycleTransition before deregistration")
		tCtx.Expect(transition.Spec.NodeName).NotTo(gomega.BeNil(), "cleanup behavior in this test is only for node-scoped transitions")
		tCtx.Expect(*transition.Spec.NodeName).To(gomega.Equal(workerNode), "transition must be scoped to the current node")

		// Force deregistration by stopping all proxy pods so kubelet removes plugin sockets.
		rsName := driver.Name + "-proxy"
		rs, err := tCtx.Client().AppsV1().ReplicaSets(tCtx.Namespace()).Get(tCtx, rsName, metav1.GetOptions{})
		tCtx.ExpectNoError(err, "get SLM proxy ReplicaSet")
		rs.Spec.Replicas = ptr.To(int32(0))
		_, err = tCtx.Client().AppsV1().ReplicaSets(tCtx.Namespace()).Update(tCtx, rs, metav1.UpdateOptions{})
		tCtx.ExpectNoError(err, "scale down SLM proxy ReplicaSet")

		// cleanupDelay in kubelet plugin manager is 30s. Allow extra time for
		// watcher deregistration and API delete retries.
		tCtx.Eventually(func(tCtx ktesting.TContext) bool {
			_, err := tCtx.Client().LifecycleV1alpha1().LifecycleTransitions().Get(tCtx, transitionName, metav1.GetOptions{})
			return apierrors.IsNotFound(err)
		}).WithTimeout(3*time.Minute).Should(gomega.BeTrue(), "node-scoped LifecycleTransition should be deleted after deregistration cleanup")
	})

	ginkgo.It("should not delete allNodes or nodeSelector lifecycle transitions during driver deregistration cleanup", func(ctx context.Context) {
		tCtx := f.TContext(ctx)

		nodes := slmutils.NewNodes(tCtx, 1, 1)
		workerNode := nodes.NodeNames[0]

		driver := slmutils.NewDriverInstance(tCtx)
		driver.Run(tCtx, framework.TestContext.KubeletRootDir, nodes)

		allNodesTransitionName := "test-deregister-keep-allnodes-" + workerNode
		allNodesTransition := &lifecycleapi.LifecycleTransition{
			ObjectMeta: metav1.ObjectMeta{Name: allNodesTransitionName},
			Spec: lifecycleapi.LifecycleTransitionSpec{
				Start:    "test-deregister-keep-allnodes-start",
				End:      "test-deregister-keep-allnodes-end",
				AllNodes: ptr.To(true),
				Driver:   driver.Name,
			},
		}
		_, err := tCtx.Client().LifecycleV1alpha1().LifecycleTransitions().Create(tCtx, allNodesTransition, metav1.CreateOptions{})
		tCtx.ExpectNoError(err, "create allNodes LifecycleTransition")
		tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
			_ = tCtx.Client().LifecycleV1alpha1().LifecycleTransitions().Delete(tCtx, allNodesTransitionName, metav1.DeleteOptions{})
		})

		nodeSelectorTransitionName := "test-deregister-keep-selector-" + workerNode
		nodeSelectorTransition := &lifecycleapi.LifecycleTransition{
			ObjectMeta: metav1.ObjectMeta{Name: nodeSelectorTransitionName},
			Spec: lifecycleapi.LifecycleTransitionSpec{
				Start:  "test-deregister-keep-selector-start",
				End:    "test-deregister-keep-selector-end",
				Driver: driver.Name,
				NodeSelector: &v1.NodeSelector{
					NodeSelectorTerms: []v1.NodeSelectorTerm{
						{
							MatchExpressions: []v1.NodeSelectorRequirement{
								{
									Key:      "kubernetes.io/hostname",
									Operator: v1.NodeSelectorOpIn,
									Values:   []string{workerNode},
								},
							},
						},
					},
				},
			},
		}
		_, err = tCtx.Client().LifecycleV1alpha1().LifecycleTransitions().Create(tCtx, nodeSelectorTransition, metav1.CreateOptions{})
		tCtx.ExpectNoError(err, "create nodeSelector LifecycleTransition")
		tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
			_ = tCtx.Client().LifecycleV1alpha1().LifecycleTransitions().Delete(tCtx, nodeSelectorTransitionName, metav1.DeleteOptions{})
		})

		// Force deregistration by stopping all proxy pods so kubelet removes plugin sockets.
		rsName := driver.Name + "-proxy"
		rs, err := tCtx.Client().AppsV1().ReplicaSets(tCtx.Namespace()).Get(tCtx, rsName, metav1.GetOptions{})
		tCtx.ExpectNoError(err, "get SLM proxy ReplicaSet")
		rs.Spec.Replicas = ptr.To(int32(0))
		_, err = tCtx.Client().AppsV1().ReplicaSets(tCtx.Namespace()).Update(tCtx, rs, metav1.UpdateOptions{})
		tCtx.ExpectNoError(err, "scale down SLM proxy ReplicaSet")

		// cleanupDelay in kubelet plugin manager is 30s. Ensure both transitions
		// remain present well past that window.
		tCtx.Consistently(func(tCtx ktesting.TContext) bool {
			_, errAllNodes := tCtx.Client().LifecycleV1alpha1().LifecycleTransitions().Get(tCtx, allNodesTransitionName, metav1.GetOptions{})
			_, errSelector := tCtx.Client().LifecycleV1alpha1().LifecycleTransitions().Get(tCtx, nodeSelectorTransitionName, metav1.GetOptions{})
			return errAllNodes == nil && errSelector == nil
		}).WithTimeout(90*time.Second).Should(gomega.BeTrue(), "allNodes/nodeSelector transitions should not be deleted by node-scoped cleanup")
	})

	ginkgo.It("should complete a lifecycle event flow", func(ctx context.Context) {
		tCtx := f.TContext(ctx)

		// Select nodes.
		nodes := slmutils.NewNodes(tCtx, 1, 3)

		// Create and deploy the driver with the DRA proxy pattern.
		driver := slmutils.NewDriverInstance(tCtx)
		driver.Transitions = []app.TransitionSpec{
			{
				Start: "test-drain-started",
				End:   "test-drain-complete",
			},
		}
		driver.Run(tCtx, framework.TestContext.KubeletRootDir, nodes)

		// Pick the first node.
		gomega.Expect(driver.Nodenames()).NotTo(gomega.BeEmpty())
		workerNode := driver.Nodenames()[0]
		tCtx.Logf("Using worker node: %s", workerNode)

		// Verify the driver published a LifecycleTransition.
		transitionName := driver.Name + "-" + workerNode
		var transition *lifecycleapi.LifecycleTransition
		tCtx.Eventually(func(tCtx ktesting.TContext) error {
			var err error
			transition, err = tCtx.Client().LifecycleV1alpha1().LifecycleTransitions().Get(tCtx, transitionName, metav1.GetOptions{})
			return err
		}).WithTimeout(30*time.Second).Should(gomega.Succeed(), "LifecycleTransition should exist")
		tCtx.Logf("Found LifecycleTransition %q (start=%q, end=%q, driver=%q)",
			transition.Name, transition.Spec.Start, transition.Spec.End, transition.Spec.Driver)

		endState := transition.Spec.End

		// Create a LifecycleEvent targeting the worker node.
		eventName := "test-event-" + workerNode
		event := &lifecycleapi.LifecycleEvent{
			ObjectMeta: metav1.ObjectMeta{
				Name: eventName,
			},
			Spec: lifecycleapi.LifecycleEventSpec{
				TransitionName: transition.Name,
				BindingNode:    workerNode,
			},
			Status: lifecycleapi.LifecycleEventStatus{
				ClaimStatus: lifecycleapi.LifecycleEventPending,
			},
		}
		_, err := tCtx.Client().LifecycleV1alpha1().LifecycleEvents().Create(tCtx, event, metav1.CreateOptions{})
		tCtx.ExpectNoError(err, "create LifecycleEvent")
		tCtx.Logf("Created LifecycleEvent %q", eventName)

		// Clean up event on failure.
		tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
			_ = tCtx.Client().LifecycleV1alpha1().LifecycleEvents().Delete(tCtx, eventName, metav1.DeleteOptions{})
		})

		// Step 1: Wait for the kubelet to claim the event.
		tCtx.Log("Waiting for LifecycleEvent to be Claimed...")
		tCtx.Eventually(func(tCtx ktesting.TContext) string {
			ev, err := tCtx.Client().LifecycleV1alpha1().LifecycleEvents().Get(tCtx, eventName, metav1.GetOptions{})
			if err != nil {
				return ""
			}
			return string(ev.Status.ClaimStatus)
		}).WithTimeout(2 * time.Minute).Should(gomega.Equal(string(lifecycleapi.LifecycleEventClaimed)))
		tCtx.Log("LifecycleEvent is Claimed")

		// Step 2: Wait for the Node condition show the end state.
		tCtx.Logf("Waiting for driver to pods the end condition")
		tCtx.Eventually(func(tCtx ktesting.TContext) string {
			reason, _ := getNodeConditionReason(tCtx, workerNode)
			return reason
		}).WithTimeout(2 * time.Minute).Should(gomega.Equal(endState))
		tCtx.Log("Node condition shows start state")
		// Step 3: Wait for the event to be deleted.
		tCtx.Log("Waiting for LifecycleEvent to be deleted...")
		tCtx.Eventually(func(tCtx ktesting.TContext) bool {
			_, err := tCtx.Client().LifecycleV1alpha1().LifecycleEvents().Get(tCtx, eventName, metav1.GetOptions{})
			return apierrors.IsNotFound(err)
		}).WithTimeout(2 * time.Minute).Should(gomega.BeTrue())
		tCtx.Log("LifecycleEvent deleted (transition complete)")

		// Verify the LifecycleTransition still exists.
		_, err = tCtx.Client().LifecycleV1alpha1().LifecycleTransitions().Get(tCtx, transitionName, metav1.GetOptions{})
		tCtx.ExpectNoError(err, "LifecycleTransition should still exist")

		// Verify final Node condition.
		reason, err := getNodeConditionReason(tCtx, workerNode)
		tCtx.ExpectNoError(err)
		gomega.Expect(reason).To(gomega.Equal(endState), "final Node condition reason")

		tCtx.Log("SLM e2e test passed: full lifecycle event flow completed successfully")
	})
})

// getNodeConditionReason returns the Reason of the LifecycleTransition
// condition on the node, or "" if the condition is not present.
func getNodeConditionReason(tCtx ktesting.TContext, nodeName string) (string, error) {
	node, err := tCtx.Client().CoreV1().Nodes().Get(tCtx, nodeName, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	for _, c := range node.Status.Conditions {
		if c.Type == slmplugin.LifecycleTransitionConditionType {
			return c.Reason, nil
		}
	}
	return "", nil
}

func countClaimedAndPending(tCtx ktesting.TContext, bindingNode string) (int, int) {
	list, err := tCtx.Client().LifecycleV1alpha1().LifecycleEvents().List(tCtx, metav1.ListOptions{})
	if err != nil {
		return 0, 0
	}

	claimed := 0
	pending := 0
	for i := range list.Items {
		ev := &list.Items[i]
		if ev.Spec.BindingNode != bindingNode {
			continue
		}
		switch ev.Status.ClaimStatus {
		case lifecycleapi.LifecycleEventClaimed:
			claimed++
		case lifecycleapi.LifecycleEventPending:
			pending++
		}
	}
	return claimed, pending
}

func restartKubeletOverSSH(tCtx ktesting.TContext, nodeName string) {
	node, err := tCtx.Client().CoreV1().Nodes().Get(tCtx, nodeName, metav1.GetOptions{})
	tCtx.ExpectNoError(err, "get node %s", nodeName)

	oldHeartbeat := e2enode.GetNodeHeartbeatTime(node)

	run := func(cmd string) error {
		result, err := e2essh.IssueSSHCommandWithResult(tCtx, cmd, framework.TestContext.Provider, node)
		if err != nil {
			return err
		}
		if result.Code != 0 {
			return fmt.Errorf("command %q exited with code %d: %s", cmd, result.Code, result.Stderr)
		}
		return nil
	}

	if err := run("systemctl restart kubelet"); err != nil {
		tCtx.ExpectNoError(run("sudo systemctl restart kubelet"), "restart kubelet on node %s", nodeName)
	}

	e2enode.WaitForNodeHeartbeatAfter(tCtx, tCtx.Client(), nodeName, oldHeartbeat, 3*time.Minute)
	ready := e2enode.WaitForNodeToBeReady(tCtx, tCtx.Client(), nodeName, 3*time.Minute)
	tCtx.Expect(ready).To(gomega.BeTrue(), "node should become Ready after kubelet restart")
}
