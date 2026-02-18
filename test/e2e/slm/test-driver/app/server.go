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

// Package app implements the SLM test driver binary.
package app

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/featuregate"
	"k8s.io/component-base/logs"
	logsapi "k8s.io/component-base/logs/api/v1"
	"k8s.io/component-base/term"
	"k8s.io/klog/v2"
)

const (
	// DefaultKubeletPluginsDir is where DRA/SLM plugins store their
	// per-driver Unix domain sockets.
	DefaultKubeletPluginsDir = "/var/lib/kubelet/plugins"
	// DefaultKubeletRegistryDir is where the kubelet plugin watcher
	// discovers registration sockets.
	DefaultKubeletRegistryDir = "/var/lib/kubelet/plugins_registry"
)

// NewCommand creates a *cobra.Command for the SLM test driver.
func NewCommand() *cobra.Command {
	o := logsapi.NewLoggingConfiguration()
	var clientset kubernetes.Interface
	logger := klog.Background()

	cmd := &cobra.Command{
		Use:  "slm-test-driver",
		Long: "slm-test-driver implements a kubelet plugin for Specialized Lifecycle Management testing.",
	}
	sharedFlagSets := cliflag.NamedFlagSets{}
	fs := sharedFlagSets.FlagSet("logging")
	logsapi.AddFlags(o, fs)
	logs.AddFlags(fs, logs.SkipLoggingConfigurationFlags())

	fs = sharedFlagSets.FlagSet("Kubernetes client")
	kubeconfig := fs.String("kubeconfig", "", "Absolute path to the kube.config file. Either this or KUBECONFIG need to be set if the driver is being run out of cluster.")
	kubeAPIQPS := fs.Float32("kube-api-qps", 50, "QPS to use while communicating with the kubernetes apiserver.")
	kubeAPIBurst := fs.Int("kube-api-burst", 100, "Burst to use while communicating with the kubernetes apiserver.")

	fs = sharedFlagSets.FlagSet("SLM")
	driverName := fs.String("drivername", "test-driver.slm.k8s.io", "SLM driver name.")

	fs = sharedFlagSets.FlagSet("other")
	featureGate := featuregate.NewFeatureGate()
	utilruntime.Must(logsapi.AddFeatureGates(featureGate))
	featureGate.AddFlag(fs)

	fs = cmd.PersistentFlags()
	for _, f := range sharedFlagSets.FlagSets {
		fs.AddFlagSet(f)
	}

	cmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		if err := logsapi.ValidateAndApply(o, featureGate); err != nil {
			return err
		}

		kubeconfigEnv := os.Getenv("KUBECONFIG")
		if kubeconfigEnv != "" {
			logger.Info("Found KUBECONFIG environment variable set, using that..")
			*kubeconfig = kubeconfigEnv
		}

		var config *rest.Config
		var err error
		if *kubeconfig == "" {
			config, err = rest.InClusterConfig()
			if err != nil {
				return fmt.Errorf("create in-cluster client configuration: %w", err)
			}
		} else {
			config, err = clientcmd.BuildConfigFromFlags("", *kubeconfig)
			if err != nil {
				return fmt.Errorf("create out-of-cluster client configuration: %w", err)
			}
		}
		config.QPS = *kubeAPIQPS
		config.Burst = int(*kubeAPIBurst)

		clientset, err = kubernetes.NewForConfig(config)
		if err != nil {
			return fmt.Errorf("create client: %w", err)
		}

		return nil
	}

	kubeletPlugin := &cobra.Command{
		Use:   "kubelet-plugin",
		Short: "run as kubelet SLM plugin",
		Long:  "slm-test-driver kubelet-plugin runs as an SLM plugin for kubelet that publishes lifecycle transitions.",
		Args:  cobra.ExactArgs(0),
	}
	kubeletPluginFlagSets := cliflag.NamedFlagSets{}
	fs = kubeletPluginFlagSets.FlagSet("kubelet")
	kubeletRegistryDir := fs.String("plugin-registration-path", DefaultKubeletRegistryDir, "The directory where kubelet looks for plugin registration sockets.")
	kubeletPluginsDir := fs.String("datadir", DefaultKubeletPluginsDir, "The per-driver directory where the SLM Unix domain socket will be created.")
	fs = kubeletPluginFlagSets.FlagSet("SLM")
	nodeName := fs.String("node-name", "", "Name of the node that the kubelet plugin is responsible for.")
	fs = kubeletPlugin.Flags()
	for _, f := range kubeletPluginFlagSets.FlagSets {
		fs.AddFlagSet(f)
	}

	kubeletPlugin.RunE = func(cmd *cobra.Command, args []string) error {
		if *nodeName == "" {
			return errors.New("--node-name not set")
		}

		datadir := path.Join(*kubeletPluginsDir, *driverName)
		if err := os.MkdirAll(filepath.Dir(datadir), 0750); err != nil {
			return fmt.Errorf("create socket directory: %w", err)
		}

		// Build the desired transition, similar to how the DRA test driver
		// constructs ResourceSlice DriverResources.
		transitions := []TransitionSpec{
			{
				Name:     *driverName + "-" + *nodeName,
				Start:    "test-drain-started",
				End:      "test-drain-complete",
				NodeName: nodeName,
			},
		}

		plugin, err := StartPlugin(
			cmd.Context(),
			*driverName,
			clientset,
			*nodeName,
			transitions,
			&PluginOptions{
				PluginDataDirectoryPath: datadir,
				RegistrarDirectoryPath:  *kubeletRegistryDir,
			},
		)
		if err != nil {
			return fmt.Errorf("start SLM test plugin: %w", err)
		}

		sigc := make(chan os.Signal, 1)
		signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)
		logger.Info("Waiting for signal.")
		sig := <-sigc
		logger.Info("Received signal, shutting down.", "signal", sig)
		plugin.Stop()
		return nil
	}
	cmd.AddCommand(kubeletPlugin)

	externalController := &cobra.Command{
		Use:   "external-controller",
		Short: "run as centralized lifecycle controller",
		Long:  "slm-test-driver external-controller demonstrates the non-node-local controller method for driving lifecycle transitions.",
		Args:  cobra.ExactArgs(0),
	}
	externalControllerFlagSets := cliflag.NamedFlagSets{}
	fs = externalControllerFlagSets.FlagSet("SLM")
	controllerNodeName := fs.String("node-name", "", "Name of the node to target.")
	controllerTransitionName := fs.String("transition-name", "", "LifecycleTransition name. Defaults to <drivername>-<node-name>.")
	controllerEventName := fs.String("event-name", "", "LifecycleEvent name. Defaults to <transition-name>-event.")
	controllerStartState := fs.String("start", "test-drain-started", "LifecycleTransition start state.")
	controllerEndState := fs.String("end", "test-drain-complete", "LifecycleTransition end state.")
	deleteCompletedEvent := fs.Bool("delete-completed-event", true, "Delete LifecycleEvent after marking it Succeeded.")
	autoCompleteAfter := fs.Duration("auto-complete-after", 0, "If >0, automatically mark transition complete after this duration from event claim.")
	pollInterval := fs.Duration("poll-interval", time.Second, "Reconcile poll interval.")
	fs = externalController.Flags()
	for _, f := range externalControllerFlagSets.FlagSets {
		fs.AddFlagSet(f)
	}

	externalController.RunE = func(cmd *cobra.Command, args []string) error {
		if *controllerNodeName == "" {
			return errors.New("--node-name not set")
		}
		transitionName := *controllerTransitionName
		if transitionName == "" {
			transitionName = *driverName + "-" + *controllerNodeName
		}
		eventName := *controllerEventName
		if eventName == "" {
			eventName = transitionName + "-event"
		}

		return runExternalController(cmd.Context(), clientset, ExternalControllerOptions{
			DriverName:           *driverName,
			NodeName:             *controllerNodeName,
			TransitionName:       transitionName,
			EventName:            eventName,
			StartState:           *controllerStartState,
			EndState:             *controllerEndState,
			PollInterval:         *pollInterval,
			DeleteCompletedEvent: *deleteCompletedEvent,
			AutoCompleteAfter:    *autoCompleteAfter,
		})
	}
	cmd.AddCommand(externalController)

	cols, _, _ := term.TerminalSize(cmd.OutOrStdout())
	cliflag.SetUsageAndHelpFunc(cmd, sharedFlagSets, cols)
	var children []string
	for _, child := range cmd.Commands() {
		children = append(children, child.Use)
	}
	cmd.Use += " [shared flags] " + strings.Join(children, "|")
	cliflag.SetUsageAndHelpFunc(kubeletPlugin, kubeletPluginFlagSets, cols)
	cliflag.SetUsageAndHelpFunc(externalController, externalControllerFlagSets, cols)

	return cmd
}

// listen creates a Unix domain socket and returns the net.Listener.
// It removes any stale socket file first.
func listen(socketPath string) (net.Listener, error) {
	if err := os.MkdirAll(filepath.Dir(socketPath), 0750); err != nil {
		return nil, fmt.Errorf("create directory for socket %s: %w", socketPath, err)
	}
	// Remove stale socket.
	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("remove existing socket %s: %w", socketPath, err)
	}
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", socketPath, err)
	}
	return listener, nil
}
