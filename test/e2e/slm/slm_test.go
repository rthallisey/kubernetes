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
	"os"
	"testing"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	lifecycleapi "k8s.io/api/lifecycle/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/test/e2e/framework"
	"k8s.io/kubernetes/test/e2e/framework/config"
	"k8s.io/kubernetes/test/e2e/framework/testfiles"
	"k8s.io/kubernetes/test/e2e/slm/test-driver/app"
	slmutils "k8s.io/kubernetes/test/e2e/slm/utils"
	"k8s.io/kubernetes/test/utils/ktesting"
	admissionapi "k8s.io/pod-security-admission/api"

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
