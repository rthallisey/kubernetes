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

// Package utils provides test utilities for deploying the SLM test driver
// using the same proxy pattern that DRA uses. The driver runs in the e2e
// test process while a lightweight proxy pod inside the cluster bridges
// the Unix domain sockets that the kubelet expects.
package utils

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/authentication/serviceaccount"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/klog/v2"
	"k8s.io/kubectl/pkg/cmd/exec"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2ereplicaset "k8s.io/kubernetes/test/e2e/framework/replicaset"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	"k8s.io/kubernetes/test/e2e/slm/test-driver/app"
	"k8s.io/kubernetes/test/e2e/slm/test-driver/deploy/example"
	"k8s.io/kubernetes/test/e2e/storage/drivers/proxy"
	"k8s.io/kubernetes/test/e2e/storage/utils"
	"k8s.io/kubernetes/test/utils/ktesting"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/yaml"
)

// Nodes holds the set of nodes selected for a test.
type Nodes struct {
	NodeNames []string
}

// NewNodes selects schedulable worker nodes for the test.
func NewNodes(tCtx ktesting.TContext, minNodes, maxNodes int) *Nodes {
	nodes := &Nodes{}
	tCtx.Log("selecting nodes")
	nodeList, err := e2enode.GetBoundedReadySchedulableNodes(tCtx, tCtx.Client(), maxNodes)
	tCtx.ExpectNoError(err, "get nodes")
	if len(nodeList.Items) < minNodes {
		e2eskipper.Skipf("%d ready nodes required, only have %d", minNodes, len(nodeList.Items))
	}
	for _, node := range nodeList.Items {
		nodes.NodeNames = append(nodes.NodeNames, node.Name)
	}
	sort.Strings(nodes.NodeNames)
	tCtx.Logf("testing on nodes %v", nodes.NodeNames)
	return nodes
}

// KubeletPlugin holds the per-node plugin instance.
type KubeletPlugin struct {
	*app.SLMTestPlugin
	ClientSet kubernetes.Interface
}

// Driver encapsulates the SLM test driver deployment: proxy pods,
// service accounts, and per-node plugin instances.
type Driver struct {
	cleanup            []func(ktesting.TContext)
	serviceAccountName string

	// Name is the SLM driver name (derived from the test namespace).
	Name string

	// Nodes contains entries for each node the driver runs on.
	Nodes map[string]KubeletPlugin

	// Transitions describes the LifecycleTransitions the driver should publish.
	Transitions []app.TransitionSpec
}

// NewDriverInstance creates a new Driver but does not start it.
func NewDriverInstance(tCtx ktesting.TContext) *Driver {
	d := &Driver{}
	if tCtx != nil {
		d.initName(tCtx)
	}
	return d
}

func (d *Driver) initName(tCtx ktesting.TContext) {
	d.Name = tCtx.Namespace() + ".slm.k8s.io"
}

// Run deploys the driver, starts plugins, and registers cleanup.
func (d *Driver) Run(tCtx ktesting.TContext, kubeletRootDir string, nodes *Nodes) {
	d.SetUp(tCtx, kubeletRootDir, nodes)
	tCtx.CleanupCtx(d.TearDown)
}

// SetUp deploys proxy pods and starts the SLM plugin on each node.
func (d *Driver) SetUp(tCtx ktesting.TContext, kubeletRootDir string, nodes *Nodes) {
	tCtx.Logf("deploying SLM driver %s on nodes %v", d.Name, nodes.NodeNames)
	d.Nodes = make(map[string]KubeletPlugin)

	tCtx = tCtx.WithCancel()
	logger := klog.FromContext(tCtx)
	logger = klog.LoggerWithValues(logger, "driverName", d.Name)
	tCtx = tCtx.WithLogger(logger)
	d.cleanup = append(d.cleanup, func(ktesting.TContext) { tCtx.Cancel("cleaning up test") })

	// LifecycleTransition cleanup is handled by each per-node plugin's
	// Stop() method, which deletes the transitions it created.

	// ── Create RBAC ─────────────────────────────────────────────────
	d.serviceAccountName = "slm-kubelet-plugin-" + d.Name + "-service-account"
	content := example.PluginPermissions
	content = strings.ReplaceAll(content, "slm-kubelet-plugin-namespace", tCtx.Namespace())
	content = strings.ReplaceAll(content, "slm-kubelet-plugin", "slm-kubelet-plugin-"+d.Name)
	d.createFromYAML(tCtx, []byte(content), tCtx.Namespace())

	// ── Deploy proxy ReplicaSet ─────────────────────────────────────
	manifests := []string{
		"test/e2e/testing-manifests/slm/slm-test-driver-proxy.yaml",
	}

	instanceKey := "app.kubernetes.io/instance"
	rsName := ""
	numNodes := int32(len(nodes.NodeNames))
	pluginDataDirectoryPath := path.Join(kubeletRootDir, "plugins", d.Name)
	registrarDirectoryPath := path.Join(kubeletRootDir, "plugins_registry")
	err := utils.CreateFromManifestsTCtx(tCtx, func(item interface{}) error {
		switch item := item.(type) {
		case *appsv1.ReplicaSet:
			rsName = d.Name + "-proxy"
			item.Name = rsName
			item.Spec.Replicas = &numNodes
			item.Spec.Selector.MatchLabels[instanceKey] = d.Name
			item.Spec.Template.Labels[instanceKey] = d.Name
			item.Spec.Template.Spec.ServiceAccountName = d.serviceAccountName
			item.Spec.Template.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0].LabelSelector.MatchLabels[instanceKey] = d.Name
			item.Spec.Template.Spec.Affinity.NodeAffinity = &v1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
					NodeSelectorTerms: []v1.NodeSelectorTerm{
						{
							MatchExpressions: []v1.NodeSelectorRequirement{
								{
									Key:      "kubernetes.io/hostname",
									Operator: v1.NodeSelectorOpIn,
									Values:   nodes.NodeNames,
								},
							},
						},
					},
				},
			}
			item.Spec.Template.Spec.Volumes[0].HostPath.Path = pluginDataDirectoryPath
			item.Spec.Template.Spec.Volumes[1].HostPath.Path = registrarDirectoryPath
			item.Spec.Template.Spec.Containers[0].VolumeMounts[0].MountPath = pluginDataDirectoryPath
			item.Spec.Template.Spec.Containers[0].VolumeMounts[1].MountPath = registrarDirectoryPath
		}
		return nil
	}, manifests...)
	tCtx.ExpectNoError(err, "deploy SLM proxy replicaset")

	rs, err := tCtx.Client().AppsV1().ReplicaSets(tCtx.Namespace()).Get(tCtx, rsName, metav1.GetOptions{})
	tCtx.ExpectNoError(err, "get replicaset")

	// Wait for all pods to be running.
	if err := e2ereplicaset.WaitForReplicaSetTargetAvailableReplicas(tCtx, tCtx.Client(), rs, numNodes); err != nil {
		tCtx.ExpectNoError(err, "all SLM plugin proxies running")
	}
	requirement, err := labels.NewRequirement(instanceKey, selection.Equals, []string{d.Name})
	tCtx.ExpectNoError(err, "create label selector requirement")
	selector := labels.NewSelector().Add(*requirement)
	pods, err := tCtx.Client().CoreV1().Pods(tCtx.Namespace()).List(tCtx, metav1.ListOptions{LabelSelector: selector.String()})
	tCtx.ExpectNoError(err, "list proxy pods")
	tCtx.Expect(numNodes).To(gomega.Equal(int32(len(pods.Items))), "number of proxy pods")
	sort.Slice(pods.Items, func(i, j int) bool {
		return pods.Items[i].Spec.NodeName < pods.Items[j].Spec.NodeName
	})

	// ── Start plugins per node ──────────────────────────────────────
	for _, pod := range pods.Items {
		nodename := pod.Spec.NodeName

		// Build a client impersonating the service account on this node.
		driverClient := d.ImpersonateKubeletPlugin(tCtx, &pod)

		var listenerPort atomic.Int32
		listenerPort.Store(9000)

		// Build node-specific transitions.
		transitions := make([]app.TransitionSpec, 0, len(d.Transitions))
		for _, ts := range d.Transitions {
			t := ts
			if t.NodeName == nil {
				t.NodeName = ptr.To(nodename)
			}
			if t.Name == "" {
				t.Name = d.Name + "-" + nodename
			}
			transitions = append(transitions, t)
		}

		plugin, err := app.StartPlugin(tCtx, d.Name, driverClient, nodename, transitions,
			&app.PluginOptions{
				PluginDataDirectoryPath: pluginDataDirectoryPath,
				RegistrarDirectoryPath:  registrarDirectoryPath,
				PluginListener:          d.listen(tCtx, &pod, &listenerPort),
				RegistrarListener:       d.listen(tCtx, &pod, &listenerPort),
			},
		)
		tCtx.ExpectNoError(err, "start SLM plugin for node %s", nodename)

		d.cleanup = append(d.cleanup, func(tCtx ktesting.TContext) {
			plugin.Stop()

			tCtx.Log("scaling down SLM proxy pods for", d.Name)
			rs, err := tCtx.Client().AppsV1().ReplicaSets(tCtx.Namespace()).Get(tCtx, rsName, metav1.GetOptions{})
			tCtx.ExpectNoError(err, "get ReplicaSet for driver "+d.Name)
			rs.Spec.Replicas = ptr.To(int32(0))
			rs, err = tCtx.Client().AppsV1().ReplicaSets(tCtx.Namespace()).Update(tCtx, rs, metav1.UpdateOptions{})
			tCtx.ExpectNoError(err, "scale down ReplicaSet for driver "+d.Name)
			if err := e2ereplicaset.WaitForReplicaSetTargetAvailableReplicas(tCtx, tCtx.Client(), rs, 0); err != nil {
				tCtx.ExpectNoError(err, "all SLM plugin proxies stopped")
			}
		})
		d.Nodes[nodename] = KubeletPlugin{SLMTestPlugin: plugin, ClientSet: driverClient}
	}

	// Wait for registration.
	tCtx.Log("wait for SLM plugin registration")
	tCtx.Eventually(func(tCtx ktesting.TContext) map[string]bool {
		notRegistered := make(map[string]bool)
		for nodename, plugin := range d.Nodes {
			if !plugin.IsRegistered() {
				notRegistered[nodename] = true
			}
		}
		return notRegistered
	}).WithTimeout(time.Minute).Should(gomega.BeEmpty(), "hosts where the SLM plugin has not been registered yet")
}

// ImpersonateKubeletPlugin creates a Kubernetes client that impersonates the
// driver's service account on the node where the given pod runs.
func (d *Driver) ImpersonateKubeletPlugin(tCtx ktesting.TContext, pod *v1.Pod) kubernetes.Interface {
	tCtx.Helper()
	driverUserInfo := (&serviceaccount.ServiceAccountInfo{
		Name:      d.serviceAccountName,
		Namespace: pod.Namespace,
		NodeName:  pod.Spec.NodeName,
		PodName:   pod.Name,
		PodUID:    string(pod.UID),
	}).UserInfo()
	driverClientConfig := tCtx.RESTConfig()
	driverClientConfig.Impersonate = rest.ImpersonationConfig{
		UserName: driverUserInfo.GetName(),
		Groups:   driverUserInfo.GetGroups(),
		Extra:    driverUserInfo.GetExtra(),
	}
	driverClient, err := kubernetes.NewForConfig(driverClientConfig)
	tCtx.ExpectNoError(err, "create client for driver")
	return driverClient
}

// TearDown executes all cleanup functions in FIFO order.
func (d *Driver) TearDown(tCtx ktesting.TContext) {
	for _, c := range d.cleanup {
		c(tCtx)
	}
	d.cleanup = nil
}

// Nodenames returns a sorted list of node names running the driver.
func (d *Driver) Nodenames() []string {
	var names []string
	for n := range d.Nodes {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// ── Proxy listener infrastructure ────────────────────────────────────────────

// errListenerDone is the special error used to shut down the proxy.
var errListenerDone = errors.New("listener is shutting down")

// listen returns a ListenerFunc that spins up hostpathplugin in proxy mode
// inside the given pod and returns a net.Listener connected to it via
// port forwarding through the API server.
func (d *Driver) listen(tCtx ktesting.TContext, pod *v1.Pod, port *atomic.Int32) app.ListenerFunc {
	return func(ctx context.Context, endpoint string) (l net.Listener, e error) {
		thisPort := port.Add(1)

		logger := klog.FromContext(ctx)
		logger = klog.LoggerWithName(logger, "socket-listener")
		logger = klog.LoggerWithValues(logger, "endpoint", endpoint, "port", thisPort)
		ctx = klog.NewContext(ctx, logger)

		// Start hostpathplugin in proxy mode.
		req := tCtx.Client().CoreV1().RESTClient().Post().
			Resource("pods").
			Namespace(tCtx.Namespace()).
			Name(pod.Name).
			SubResource("exec").
			VersionedParams(&v1.PodExecOptions{
				Container: pod.Spec.Containers[0].Name,
				Command: []string{
					"/hostpathplugin",
					"--v=5",
					"--endpoint=" + endpoint,
					fmt.Sprintf("--proxy-endpoint=tcp://:%d", thisPort),
				},
				Stdout: true,
				Stderr: true,
			}, scheme.ParameterCodec)

		var wg sync.WaitGroup
		wg.Add(1)
		cmdCtx, cmdCancel := context.WithCancelCause(ctx)
		go func() {
			defer wg.Done()
			cmdLogger := klog.LoggerWithName(logger, "hostpathplugin")
			cmdCtx := klog.NewContext(cmdCtx, cmdLogger)
			logger.V(1).Info("Starting...")
			defer logger.V(1).Info("Stopped")

			delayFn := wait.Backoff{
				Duration: time.Second,
				Cap:      30 * time.Second,
				Steps:    30,
				Factor:   2.0,
				Jitter:   1.0,
			}.DelayWithReset(clock.RealClock{}, 5*time.Minute)
			runHostpathPlugin := func(ctx context.Context) (bool, error) {
				if err := execute(ctx, req.URL(), tCtx.RESTConfig(), 5); err != nil && ctx.Err() == nil {
					klog.FromContext(ctx).V(5).Info("execution failed, will retry", "err", err)
				}
				return false, nil
			}
			_ = delayFn.Until(cmdCtx, true, true, runHostpathPlugin)

			// Clean up the socket.
			cleanReq := tCtx.Client().CoreV1().RESTClient().Post().
				Resource("pods").
				Namespace(tCtx.Namespace()).
				Name(pod.Name).
				SubResource("exec").
				VersionedParams(&v1.PodExecOptions{
					Container: pod.Spec.Containers[0].Name,
					Command:   []string{"rm", "-f", endpoint},
					Stdout:    true,
					Stderr:    true,
				}, scheme.ParameterCodec)
			cleanupLogger := klog.LoggerWithName(logger, "cleanup")
			cleanupCtx := klog.NewContext(ctx, cleanupLogger)
			if err := execute(cleanupCtx, cleanReq.URL(), tCtx.RESTConfig(), 0); err != nil {
				cleanupLogger.Error(err, "Socket removal failed")
			}
		}()
		defer func() {
			if e != nil {
				cmdCancel(e)
			}
		}()
		stopHostpathplugin := func() {
			cmdCancel(errListenerDone)
			wg.Wait()
		}

		addr := proxy.Addr{
			Namespace:     tCtx.Namespace(),
			PodName:       pod.Name,
			ContainerName: pod.Spec.Containers[0].Name,
			Port:          int(thisPort),
		}
		listener, err := proxy.Listen(ctx, tCtx.Client(), tCtx.RESTConfig(), addr)
		if err != nil {
			return nil, fmt.Errorf("listen for connections from %+v: %w", addr, err)
		}
		return &listenerWithClose{Listener: listener, close: stopHostpathplugin}, nil
	}
}

// listenerWithClose wraps Close to also shut down hostpathplugin.
type listenerWithClose struct {
	net.Listener
	close func()
}

func (l *listenerWithClose) Close() error {
	err := l.Listener.Close()
	l.close()
	return err
}

// execute runs a remote command with stdout/stderr redirected to log messages.
func execute(ctx context.Context, url *url.URL, config *rest.Config, verbosity int) error {
	stdout := pipe(context.WithoutCancel(ctx), "STDOUT", verbosity)
	stderr := pipe(context.WithoutCancel(ctx), "STDERR", verbosity)
	defer func() { _ = stdout.Close() }()
	defer func() { _ = stderr.Close() }()

	executor := exec.DefaultRemoteExecutor{}
	return executor.ExecuteWithContext(ctx, url, config, nil, stdout, stderr, false, nil)
}

// pipe creates an in-memory pipe that logs data sent through it.
func pipe(ctx context.Context, msg string, verbosity int) *io.PipeWriter {
	logger := klog.FromContext(ctx)
	reader, writer := io.Pipe()
	go func() {
		buffer := make([]byte, 10*1024)
		for {
			n, err := reader.Read(buffer)
			if n > 0 {
				logger.V(verbosity).Info(msg, "msg", string(buffer[0:n]))
			}
			if err != nil {
				if !errors.Is(err, io.EOF) {
					logger.Error(err, msg)
				}
				reader.CloseWithError(err)
				return
			}
			if ctx.Err() != nil {
				reader.CloseWithError(context.Cause(ctx))
				return
			}
		}
	}()
	return writer
}

// createFromYAML applies YAML content to the cluster, creating objects
// and registering cleanup handlers.
func (d *Driver) createFromYAML(tCtx ktesting.TContext, content []byte, namespace string) {
	discoveryCache := memory.NewMemCacheClient(tCtx.Client().Discovery())
	restMapper := restmapper.NewDeferredDiscoveryRESTMapper(discoveryCache)

	for _, content := range bytes.Split(content, []byte("---\n")) {
		if len(content) == 0 {
			continue
		}

		var obj *unstructured.Unstructured
		tCtx.ExpectNoError(yaml.UnmarshalStrict(content, &obj), fmt.Sprintf("Full YAML:\n%s\n", string(content)))

		gv, err := schema.ParseGroupVersion(obj.GetAPIVersion())
		tCtx.ExpectNoError(err, fmt.Sprintf("extract group+version from object %q", klog.KObj(obj)))
		gk := schema.GroupKind{Group: gv.Group, Kind: obj.GetKind()}

		mapping, err := restMapper.RESTMapping(gk, gv.Version)
		tCtx.ExpectNoError(err, fmt.Sprintf("map %q to resource", gk))

		resourceClient := tCtx.Dynamic().Resource(mapping.Resource)
		options := metav1.CreateOptions{FieldValidation: "Strict"}
		switch mapping.Scope.Name() {
		case meta.RESTScopeNameRoot:
			_, err = resourceClient.Create(tCtx, obj, options)
		case meta.RESTScopeNameNamespace:
			if namespace == "" {
				tCtx.Fatalf("need namespace for object type %s", gk)
			}
			_, err = resourceClient.Namespace(namespace).Create(tCtx, obj, options)
		}
		tCtx.ExpectNoError(err, "create object")
		tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
			del := resourceClient.Delete
			if mapping.Scope.Name() == meta.RESTScopeNameNamespace {
				del = resourceClient.Namespace(namespace).Delete
			}
			err := del(tCtx, obj.GetName(), metav1.DeleteOptions{})
			if !apierrors.IsNotFound(err) {
				tCtx.ExpectNoError(err, fmt.Sprintf("deleting %s.%s %s", obj.GetKind(), obj.GetAPIVersion(), klog.KObj(obj)))
			}
		})
	}
}
