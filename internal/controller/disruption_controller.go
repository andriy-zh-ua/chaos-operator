/*
Copyright 2026.

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

package controller

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	chaosv1 "github.com/andriy-zh-ua/chaos-operator/api/v1"
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
)

const (
	// Safety configuration constants
	DefaultDurationSeconds             = 300             // Default duration for a disruption in seconds (5 minutes)
	DefaultPodsAffected                = 1               // Default pods affected per reconciliation cycle (Scope: Per-cycle safety limit)
	DefaultPercentageAffected          = 10              // Default percentage of pods affected per reconciliation cycle (Scope: Per-cycle safety limit)
	DefaultCountLimit                  = 100             // Absolute default count value a user can specify in their spec.Count (Scope: Global safety limit)
	DefaultGracePeriodSeconds          = 300             // Default grace period in seconds (equal to default duration)
	DefaultMonitoringReconcileInterval = 5 * time.Second // Default monitoring reconcile interval (5 seconds)

	// Disruption phase constants
	PhaseCompleted = "Completed"
	PhaseFailed    = "Failed"
	PhaseRunning   = "Running"

	// PodKill kill mode constants
	KillModeFixedCount = "fixed-count"
	KillModeRandom     = "random"
	KillModeAll        = "all"
)

// Default system namespaces
var DefaultSystemNamespaces = []string{
	"kube-system",           // Cluster management namespace
	"kube-public",           // Public cluster namespace
	"kube-node-lease",       // Node lease system namespace
	"chaos-operator-system", // Chaos operator namespace (CRITICAL!)
	"gatekeeper-system",     // Security policy namespace
	"istio-system",          // Service mesh namespace
	"default",               // Default namespace
}

// Global random source for pod selection - initialized once for better performance
var globalRandSource = rand.New(rand.NewSource(time.Now().UnixNano()))

// DisruptionReconciler reconciles a Disruption object
type DisruptionReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Logger   logr.Logger
	Recorder record.EventRecorder

	defaultSafetyConfig chaosv1.SafetyConfig
	systemNamespaces    map[string]bool
}

// NewDisruptionReconciler creates a new disruption reconciler
func NewDisruptionReconciler(mgr ctrl.Manager) *DisruptionReconciler {
	// Create a new reconciler
	r := &DisruptionReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Logger:   ctrl.Log.WithName("controllers").WithName("Disruption"),
		Recorder: mgr.GetEventRecorderFor("disruption-controller"),
	}

	// Initialize default safety configuration
	r.defaultSafetyConfig = chaosv1.SafetyConfig{
		DurationSeconds:             r.parseEnvInt32("CHAOS_DEFAULT_DURATION_SECONDS", DefaultDurationSeconds),
		PodsAffected:                r.parseEnvInt32("CHAOS_DEFAULT_PODS_AFFECTED", DefaultPodsAffected),
		PercentageAffected:          r.parseEnvInt32("CHAOS_DEFAULT_PERCENTAGE_AFFECTED", DefaultPercentageAffected),
		CountLimit:                  r.parseEnvInt32("CHAOS_DEFAULT_COUNT_LIMIT", DefaultCountLimit),
		GracePeriodSeconds:          r.parseEnvInt64("CHAOS_DEFAULT_GRACE_PERIOD_SECONDS", DefaultGracePeriodSeconds),
		MonitoringReconcileInterval: time.Duration(r.parseEnvInt32("CHAOS_DEFAULT_MONITORING_RECONCILE_INTERVAL", int32(DefaultMonitoringReconcileInterval.Seconds()))) * time.Second,
	}

	// Initialize system namespaces
	r.systemNamespaces = r.parseEnvSystemNamespaces("CHAOS_SYSTEM_NAMESPACES", DefaultSystemNamespaces)

	return r
}

// +kubebuilder:rbac:groups=chaos.a2solutions.ca,resources=disruptions,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=chaos.a2solutions.ca,resources=disruptions/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=chaos.a2solutions.ca,resources=disruptions/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Disruption object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.1/pkg/reconcile
func (r *DisruptionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var disruption chaosv1.Disruption

	// Get the Disruption custom resource from the cluster
	if err := r.Get(ctx, req.NamespacedName, &disruption); err != nil {
		r.Logger.Error(err, "Unable to fetch Disruption")
		if errors.IsNotFound(err) {
			// No further reconciliation needed - disruption is finished
			return ctrl.Result{}, nil
		}
		// Return error to trigger exponential backoff and retry until the status update succeeds
		return ctrl.Result{}, err
	}

	// Skip reconciliation for completed/failed disruptions
	if disruption.Status.Phase == PhaseCompleted || disruption.Status.Phase == PhaseFailed {
		// No further reconciliation needed - disruption is finished
		return ctrl.Result{}, nil
	}

	r.Logger.Info("Reconciling Disruption", "name", disruption.Name, "namespace", disruption.Namespace)

	// Get safety configuration with defaults
	safetyConfig := r.getSafetyConfig(disruption)

	// Validate PodKill configuration
	if err := r.validatePodKill(disruption.Spec.PodKill, safetyConfig); err != nil {
		r.Logger.Error(err, "Invalid PodKill configuration")
		if err := r.markDisruptionFailed(ctx, &disruption); err != nil {
			// Return error to trigger exponential backoff and retry until the status update succeeds
			return ctrl.Result{}, err
		}
		r.Logger.Info("Disruption marked as failed due to invalid PodKill configuration")
		// No further reconciliation needed - disruption is finished
		return ctrl.Result{}, nil
	}

	// If experiment has no start time, mark as Running and set start time
	if disruption.Status.StartTime == nil {
		if err := r.markDisruptionRunning(ctx, &disruption); err != nil {
			// Return error to trigger exponential backoff and retry until the status update succeeds
			return ctrl.Result{}, err
		}
		r.Logger.Info("Disruption started")
	}

	// Check if disruption has reached its specified duration
	if completed, err := r.checkDisruptionDuration(ctx, &disruption); completed {
		return ctrl.Result{}, err
	}

	// Ensures the Disruption cannot run longer than allowed
	if safetyConfig.DurationSeconds > 0 {
		duration := time.Since(disruption.Status.StartTime.Time)

		// If the disruption has run longer than the max duration
		if duration > time.Duration(safetyConfig.DurationSeconds)*time.Second {
			if err := r.markDisruptionCompleted(ctx, &disruption); err != nil {
				// Return error to trigger exponential backoff and retry until the status update succeeds
				return ctrl.Result{}, err
			}
			r.Logger.Info("Disruption completed due to max duration")
			// No further reconciliation needed - disruption is finished
			return ctrl.Result{}, nil
		}
	}

	// Execute PodKill
	if err := r.executePodKill(ctx, &disruption, safetyConfig); err != nil {
		r.Logger.Error(err, "Failed to execute PodKill")
		// Mark disruption as failed
		if markErr := r.markDisruptionFailed(ctx, &disruption); markErr != nil {
			// Return error to trigger exponential backoff and retry until the status update succeeds
			return ctrl.Result{}, err
		}
		// Return error to trigger exponential backoff and retry until the status update succeeds
		return ctrl.Result{}, err
	}

	// Requeue to continue monitoring
	return ctrl.Result{RequeueAfter: safetyConfig.MonitoringReconcileInterval}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *DisruptionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&chaosv1.Disruption{}).
		Named("disruption").
		Complete(r)
}

// validatePodKill validates the PodKill specification
func (r *DisruptionReconciler) validatePodKill(spec *chaosv1.PodKillSpec, safetyConfig chaosv1.SafetyConfig) error {
	if spec == nil {
		return fmt.Errorf("no PodKill specification provided - nothing to disrupt")
	}

	// Validate scope
	if spec.Scope != "" && spec.Scope != chaosv1.DisruptionScopeNamespace && spec.Scope != chaosv1.DisruptionScopeCluster {
		return fmt.Errorf("invalid scope: %s. Must be 'namespace' or 'cluster'", spec.Scope)
	}

	// Check if selector is nil or empty
	if spec.Selector == nil {
		r.Logger.Info("No selector specified - disruption will affect all pods in the target namespaces")
	} else if spec.Selector.MatchLabels == nil && spec.Selector.MatchExpressions == nil {
		r.Logger.Info("Selector is empty - disruption will affect all pods in target namespaces")
	}

	// Validate duration if specified
	if spec.Duration != nil {
		// Validate duration is positive
		if spec.Duration.Duration <= 0 {
			return fmt.Errorf("podKill.duration must be positive, got %v", spec.Duration.Duration)
		}
		// Check against maximum allowed duration from safety config
		maxDuration := time.Duration(safetyConfig.DurationSeconds) * time.Second
		if spec.Duration.Duration > maxDuration {
			return fmt.Errorf("podKill.duration '%v' exceeds maximum allowed limit of %v", spec.Duration.Duration, maxDuration)
		}
	}

	// Validate count only when killMode is fixed-count
	if spec.KillMode != KillModeFixedCount && spec.Count > 0 {
		return fmt.Errorf("podKill.count is only valid when killMode is 'fixed-count', but killMode is '%s'", spec.KillMode)
	}

	// Validate count is positive when killMode is fixed-count
	if spec.KillMode == KillModeFixedCount && spec.Count <= 0 {
		return fmt.Errorf("podKill.count must be > 0 when killMode is 'fixed-count'")
	}

	// Validate count does not exceed maximum limit
	if spec.Count > safetyConfig.CountLimit {
		return fmt.Errorf("podKill.count '%d' exceeds maximum allowed limit of %d", spec.Count, safetyConfig.CountLimit)
	}

	// Validate count does not exceed safety PodsAffected
	if spec.KillMode == KillModeFixedCount && safetyConfig.PodsAffected > 0 && spec.Count > safetyConfig.PodsAffected {
		return fmt.Errorf("podKill.count '%d' exceeds safety PodsAffected limit of %d", spec.Count, safetyConfig.PodsAffected)
	}

	// Validate grace period does not exceed maximum limit
	if spec.GracePeriodSeconds != nil && safetyConfig.GracePeriodSeconds > 0 && *spec.GracePeriodSeconds > safetyConfig.GracePeriodSeconds {
		return fmt.Errorf("podKill.gracePeriodSeconds '%d' exceeds maximum allowed limit of %d", *spec.GracePeriodSeconds, safetyConfig.GracePeriodSeconds)
	}

	return nil
}

// getSafetyConfig returns the safety configuration for a disruption
func (r *DisruptionReconciler) getSafetyConfig(disruption chaosv1.Disruption) chaosv1.SafetyConfig {
	if disruption.Spec.Safety == nil {
		// Return default safety config
		return chaosv1.SafetyConfig{
			DurationSeconds:    r.defaultSafetyConfig.DurationSeconds,
			PodsAffected:       r.defaultSafetyConfig.PodsAffected,
			PercentageAffected: r.defaultSafetyConfig.PercentageAffected,
			CountLimit:         r.defaultSafetyConfig.CountLimit,
			GracePeriodSeconds: r.defaultSafetyConfig.GracePeriodSeconds,
		}
	}

	// Apply defaults for missing fields
	config := *disruption.Spec.Safety
	if config.DurationSeconds == 0 {
		config.DurationSeconds = r.defaultSafetyConfig.DurationSeconds
	}
	if config.PodsAffected == 0 {
		config.PodsAffected = r.defaultSafetyConfig.PodsAffected
	}
	if config.PercentageAffected == 0 {
		config.PercentageAffected = r.defaultSafetyConfig.PercentageAffected
	}
	if config.CountLimit == 0 {
		config.CountLimit = r.defaultSafetyConfig.CountLimit
	}
	if config.GracePeriodSeconds == 0 {
		config.GracePeriodSeconds = r.defaultSafetyConfig.GracePeriodSeconds
	}
	if config.MonitoringReconcileInterval == 0 {
		config.MonitoringReconcileInterval = r.defaultSafetyConfig.MonitoringReconcileInterval
	}
	return config
}

// checkDisruptionDuration checks if the disruption has reached its specified duration
//
// Arguments:
//   - ctx: The context for the request
//   - disruption: The disruption to check
//
// Returns:
//   - bool: true if the disruption should be completed due to duration
//   - error: error if marking disruption as completed fails
func (r *DisruptionReconciler) checkDisruptionDuration(ctx context.Context, disruption *chaosv1.Disruption) (bool, error) {
	// Check if PodKill spec exists and has a duration set
	if disruption.Spec.PodKill == nil || disruption.Spec.PodKill.Duration == nil {
		return false, nil
	}

	// Calculate elapsed time since disruption started
	elapsed := time.Since(disruption.Status.StartTime.Time)

	// Check if elapsed time meets or exceeds the specified duration
	if elapsed >= disruption.Spec.PodKill.Duration.Duration {
		if err := r.markDisruptionCompleted(ctx, disruption); err != nil {
			return true, fmt.Errorf("failed to mark disruption as completed: %w", err)
		}
		r.Logger.Info("Disruption completed due to user-specified duration",
			"duration", disruption.Spec.PodKill.Duration.Duration,
			"elapsed", elapsed)
		return true, nil
	}

	return false, nil
}

// parseEnvInt32 loads and parses an environment variable as an int32 with a default value
//
// Arguments:
//   - key: The environment variable key to read
//   - defaultValue: The default value to use if the environment variable is not set or invalid
//
// Returns:
//   - The parsed int32 value or the default value
//
// base: 10 = Decimal (0-9),
//
//	2 = Binary (0-1),
//	8 = Octal (0-7),
//	16 = Hexadecimal (0-9, A-F)
//
// bitSize: 0 = Int (platform-dependent, usually 64-bit),
//
//	8 = 8-bit (range: -128 to 127),
//	16 = 16-bit (range: -32,768 to 32,767),
//	32 = 32-bit (range: -2,147,483,648 to 2,147,483,647),
//	64 = 64-bit (huge range)
func (r *DisruptionReconciler) parseEnvInt32(key string, defaultValue int32) int32 {
	if value := os.Getenv(key); value != "" {
		if parsed, err := strconv.ParseInt(value, 10, 32); err == nil {
			r.Logger.Info("Parsed environment variable",
				"key", key,
				"value", parsed)
			return int32(parsed)
		}
	}
	// Log that we're using default value
	r.Logger.Info("Using default value:",
		"key", key,
		"value", defaultValue,
		"reason", "environment variable not set or invalid")
	return defaultValue
}

// parseEnvInt64 loads and parses an environment variable as an int64 with a default value
//
// Arguments:
//   - key: The environment variable key to read
//   - defaultValue: The default value to use if the environment variable is not set or invalid
//
// Returns:
//   - The parsed int64 value or the default value
func (r *DisruptionReconciler) parseEnvInt64(key string, defaultValue int64) int64 {
	if value := os.Getenv(key); value != "" {
		if parsed, err := strconv.ParseInt(value, 10, 64); err == nil {
			r.Logger.Info("Parsed environment variable",
				"key", key,
				"value", parsed)
			return parsed
		}
	}
	// Log that we're using default value
	r.Logger.Info("Using default value",
		"key", key,
		"value", defaultValue,
		"reason", "environment variable not set or invalid")
	return defaultValue
}

// parseEnvSystemNamespaces loads and parses the system namespaces from environment variable
//
// Arguments:
//   - key: The environment variable key to read
//   - defaultSystemNamespaces: The default system namespaces to use if the environment variable is not set
//
// Returns:
//   - A map of system namespace names to true
func (r *DisruptionReconciler) parseEnvSystemNamespaces(key string, defaultSystemNamespaces []string) map[string]bool {
	// Get from environment variable or use defaults
	systemNamespacesStr := os.Getenv(key)
	// Users shouldn't be required to set environment variables
	if systemNamespacesStr == "" {
		// Default critical system namespaces
		systemNamespacesStr = strings.Join(defaultSystemNamespaces, ",")
	}

	// Parse comma-separated list into map
	systemNamespaces := make(map[string]bool)
	for ns := range strings.SplitSeq(systemNamespacesStr, ",") {
		systemNamespaces[strings.TrimSpace(ns)] = true
	}

	// Sort namespaces for consistent logging
	namespaces := make([]string, 0, len(systemNamespaces))
	for ns := range systemNamespaces {
		namespaces = append(namespaces, ns)
	}
	sort.Strings(namespaces)

	r.Logger.Info("Loaded system namespace protection",
		"namespaces", namespaces,
		"count", len(systemNamespaces))

	return systemNamespaces
}

// isSystemNamespace checks if a namespace is protected from chaos
//
// Arguments:
//   - namespace: The namespace to check
//
// Returns:
//   - true if the namespace is protected, false otherwise
func (r *DisruptionReconciler) isSystemNamespace(namespace string) bool {
	return r.systemNamespaces[namespace]
}

// updateDisruptionStatus updates the disruption status and handles errors
func (r *DisruptionReconciler) updateDisruptionStatus(ctx context.Context, disruption *chaosv1.Disruption, phase string) error {
	disruption.Status.Phase = phase

	now := metav1.Now()

	switch phase {
	case PhaseRunning:
		disruption.Status.StartTime = &now

		r.setCondition(
			disruption,
			"Progressing",
			metav1.ConditionTrue,
			"DisruptionStarted",
			"Chaos disruption started",
		)

		r.Recorder.Eventf(
			disruption,
			corev1.EventTypeNormal,
			"DisruptionStarted",
			"Chaos disruption started",
		)
	case PhaseCompleted:
		disruption.Status.EndTime = &now

		r.setCondition(
			disruption,
			"Available",
			metav1.ConditionFalse,
			"DisruptionCompleted",
			"Chaos disruption completed successfully",
		)

		r.Recorder.Eventf(
			disruption,
			corev1.EventTypeNormal,
			"DisruptionCompleted",
			"Chaos disruption completed successfully",
		)
	case PhaseFailed:
		disruption.Status.EndTime = &now

		r.setCondition(
			disruption,
			"Degraded",
			metav1.ConditionTrue,
			"DisruptionFailed",
			"Chaos disruption failed",
		)

		r.Recorder.Eventf(
			disruption,
			corev1.EventTypeWarning,
			"DisruptionFailed",
			"Chaos disruption failed",
		)
	}

	if err := r.Status().Update(ctx, disruption); err != nil {
		r.Logger.Error(err, "Failed to update disruption status")
		return err
	}

	return nil
}

// markDisruptionRunning marks disruption as running
func (r *DisruptionReconciler) markDisruptionRunning(ctx context.Context, disruption *chaosv1.Disruption) error {
	return r.updateDisruptionStatus(ctx, disruption, PhaseRunning)
}

// markDisruptionCompleted marks disruption as completed
func (r *DisruptionReconciler) markDisruptionCompleted(ctx context.Context, disruption *chaosv1.Disruption) error {
	return r.updateDisruptionStatus(ctx, disruption, PhaseCompleted)
}

// markDisruptionFailed marks disruption as failed
func (r *DisruptionReconciler) markDisruptionFailed(ctx context.Context, disruption *chaosv1.Disruption) error {
	return r.updateDisruptionStatus(ctx, disruption, PhaseFailed)
}

// executePodKill is the core chaos execution logic
func (r *DisruptionReconciler) executePodKill(ctx context.Context, disruption *chaosv1.Disruption, safetyConfig chaosv1.SafetyConfig) error {
	spec := disruption.Spec.PodKill
	if spec == nil {
		return fmt.Errorf("podKill spec is nil")
	}

	// Update last execution timestamp
	now := metav1.Now()
	disruption.Status.LastExecution = &now

	// Get running pods
	pods, err := r.getTargetPods(ctx, disruption)
	if err != nil {
		return fmt.Errorf("failed to get target pods: %w", err)
	}

	if len(pods) == 0 {
		r.Logger.Info("No running pods found matching selector", "namespace", disruption.Namespace)

		r.setCondition(
			disruption,
			"Progressing",
			metav1.ConditionTrue,
			"NoTargetPods",
			"No running pods found matching selector",
		)

		r.Recorder.Eventf(
			disruption,
			corev1.EventTypeNormal,
			"NoTargetPods",
			"No running pods found matching selector",
		)

		return r.Status().Update(ctx, disruption)
	}

	// Calculate how many pods we can kill this cycle
	allowed := r.calculateAllowedKillsPerCycle(safetyConfig, len(pods))
	if allowed <= 0 {
		r.Logger.Info("Safety limits reached - no pods will be killed this cycle")

		r.setCondition(
			disruption,
			"Progressing",
			metav1.ConditionTrue,
			"SafetyLimitReached",
			"Safety limits prevented pod disruption",
		)

		r.Recorder.Eventf(
			disruption,
			corev1.EventTypeNormal,
			"SafetyLimitReached",
			"Safety limits prevented pod disruption",
		)

		return r.Status().Update(ctx, disruption)
	}

	// Select which pods to kill (respects KillMode + randomness)
	targetPods := r.selectPodsToKill(pods, disruption.Spec.PodKill, allowed)

	// Kill the selected pods
	actualKilled, err := r.killPods(ctx, targetPods, disruption)
	if err != nil {
		return err
	}

	// Accumulate total affected pods
	disruption.Status.PodsAffected += int32(actualKilled)

	r.Logger.Info("PodKill cycle completed",
		"killedThisCycle", actualKilled,
		"totalAffected", disruption.Status.PodsAffected,
		"allowed", allowed,
		"available", len(pods))

	return r.Status().Update(ctx, disruption)
}

// getTargetPods returns running pods matching the disruption's selector criteria, respecting scope and safety filters.
// It builds a label selector from the pod kill spec and lists matching pods.
//
// Arguments:
//   - ctx: The context for the request
//   - disruption: The disruption resource containing selector criteria
//
// Example label selector:
//
//	selector := &andSet{
//		requirements: []labels.Requirement{
//			{key:"app", operator:"Equals", values:[]string{"myapp"}},
//			{key:"tier", operator:"Equals", values:[]string{"frontend"}},
//		}
//	}
//
// Example list options:
//
//	listOpts := []client.ListOption{
//		&InNamespaceOption{
//			namespace: "production",
//		},
//		&MatchingLabelsSelectorOption{
//			selector: &andSet{
//				requirements: []labels.Requirement{
//					{key:"app", operator:"Equals", values:["myapp"]},
//					{key:"tier", operator:"Equals", values:["frontend"]},
//				},
//			},
//		},
//	}
//
// Example CLI command:
//
//	kubectl get pods --namespace=default --selector="app=myapp,tier=frontend"
//
// Returns:
//   - A slice of matching pods
//   - An error if the operation failed
func (r *DisruptionReconciler) getTargetPods(ctx context.Context, disruption *chaosv1.Disruption) ([]corev1.Pod, error) {
	podKillSpec := disruption.Spec.PodKill

	// Builds label selector (nil selector = match everything, as per k8s convention)
	selector, err := metav1.LabelSelectorAsSelector(podKillSpec.Selector)
	if err != nil {
		return nil, fmt.Errorf("invalid label selector: %w", err)
	}

	var targetNamespaces []string

	switch podKillSpec.Scope {
	case chaosv1.DisruptionScopeNamespace, "": // Treat empty as namespace (safe default)
		// Check if the namespace is protected
		if r.isSystemNamespace(disruption.Namespace) {
			return nil, fmt.Errorf("cannot run chaos in protected system namespace: %s", disruption.Namespace)
		}
		targetNamespaces = []string{disruption.Namespace}

	case chaosv1.DisruptionScopeCluster:
		if len(podKillSpec.Namespaces) > 0 {
			// Use the specified namespaces potentially could include system namespaces
			targetNamespaces = podKillSpec.Namespaces
		} else {
			// No namespaces specified -> target ALL except system namespaces ones
			nsList := &corev1.NamespaceList{}
			// List all namespaces
			if err := r.List(ctx, nsList); err != nil {
				return nil, fmt.Errorf("failed to list namespaces for cluster scope: %w", err)
			}
			// Filter out system namespaces
			for _, ns := range nsList.Items {
				if !r.isSystemNamespace(ns.Name) {
					targetNamespaces = append(targetNamespaces, ns.Name)
				}
			}
		}
	default:
		return nil, fmt.Errorf("unknown scope: %s", podKillSpec.Scope)
	}

	// Loop through each target namespace and list pods
	var runningPods []corev1.Pod
	for _, ns := range targetNamespaces {
		// Skip system namespaces
		if r.isSystemNamespace(ns) {
			r.Logger.Info("Skipping protected system namespace", "namespace", ns)
			continue
		}

		// List pods in each target namespace
		podList := &corev1.PodList{}
		if err := r.List(ctx, podList, client.InNamespace(ns), client.MatchingLabelsSelector{Selector: selector}); err != nil {
			r.Logger.Error(err, "Failed to list pods in namespace", "namespace", ns)
			continue
		}

		// Filter running pods
		for _, pod := range podList.Items {
			if pod.Status.Phase == corev1.PodRunning {
				runningPods = append(runningPods, pod)
			}
		}
	}

	return runningPods, nil
}

// calculateAllowedKillsPerCycle returns the maximum number of pods we may kill in the current reconciliation cycle.
// It considers the safety configuration and the number of available pods.
//
// Arguments:
// - safetyConfig: The safety configuration to apply
// - available: The number of available pods
//
// Returns:
// - The maximum number of pods we may kill in the current reconciliation cycle
func (r *DisruptionReconciler) calculateAllowedKillsPerCycle(safetyConfig chaosv1.SafetyConfig, available int) int {
	maxAllowed := available

	// Enforce PodsAffected against the currently available pods
	if safetyConfig.PodsAffected > 0 && int(safetyConfig.PodsAffected) < maxAllowed {
		maxAllowed = int(safetyConfig.PodsAffected)
	}

	// Enforce PercentageAffected against the currently available pods
	if safetyConfig.PercentageAffected > 0 {
		maxFromPercent := int(float64(available) * float64(safetyConfig.PercentageAffected) / 100.0)
		if maxFromPercent < maxAllowed {
			maxAllowed = maxFromPercent
		}
	}

	return maxAllowed
}

// selectPodsToKill returns the subset of pods that should be killed this cycle,
// respecting KillMode and the safety cap (allowed).
//
// The controller never gets invalid values because:
// - Kubernetes API rejects them first
// - Controller only receives validated objects
//
// Arguments:
// - pods: The pods to select from
// - spec: The PodKillSpec to apply
// - allowed: The maximum number of pods to select
//
// Returns:
// - The selected pods
func (r *DisruptionReconciler) selectPodsToKill(pods []corev1.Pod, spec *chaosv1.PodKillSpec, allowed int) []corev1.Pod {
	if allowed <= 0 || len(pods) == 0 {
		return []corev1.Pod{}
	}

	switch spec.KillMode {
	case KillModeAll:
		return pods[:min(len(pods), allowed)]

	case KillModeFixedCount:
		count := min(int(spec.Count), allowed, len(pods))
		return pods[:count]

	default:
		return r.getRandomPods(pods, allowed)
	}
}

// getRandomPods returns a random subset of pods, respecting the allowed limit
//
// Arguments:
// - pods: The pods to select from
// - allowed: The maximum number of pods to select (safety limit)
//
// Returns:
// - The selected pods
func (r *DisruptionReconciler) getRandomPods(pods []corev1.Pod, allowed int) []corev1.Pod {
	if len(pods) == 0 || allowed <= 0 {
		return []corev1.Pod{}
	}

	// Create a copy to avoid modifying the original slice
	shuffled := make([]corev1.Pod, len(pods))
	copy(shuffled, pods)

	// Use the global random source (better performance than creating new one each time)
	globalRandSource.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})

	// Return the first 'allowed' pods (respects safety limits)
	return shuffled[:min(len(shuffled), allowed)]
}

// killPods deletes the specified pods and records events
//
// Arguments:
// - ctx: The context for the request
// - pods: The pods to kill
// - disruption: The disruption object to update
//
// Returns:
// - The number of pods killed
// - An error if the operation fails
func (r *DisruptionReconciler) killPods(ctx context.Context, pods []corev1.Pod, disruption *chaosv1.Disruption) (int, error) {
	if len(pods) == 0 {
		return 0, nil
	}

	gracePeriodSeconds := disruption.Spec.PodKill.GracePeriodSeconds
	gracePeriod := int64(0)
	if gracePeriodSeconds != nil {
		gracePeriod = *gracePeriodSeconds
	}

	// Set how gracefully Kubernetes terminates pods during chaos experiments
	deleteOpts := &client.DeleteOptions{GracePeriodSeconds: &gracePeriod}

	killed := 0
	for _, pod := range pods {
		if err := r.Delete(ctx, &pod, deleteOpts); err != nil {
			if !errors.IsNotFound(err) {
				r.Recorder.Eventf(
					disruption,
					corev1.EventTypeWarning,
					"PodKillFailed",
					"Failed to kill pod %s in namespace %s: %v",
					pod.Name,
					pod.Namespace,
					err,
				)
				r.Logger.Error(err, "Failed to kill pod", "pod", pod.Name, "namespace", pod.Namespace)
			} else {
				r.Recorder.Eventf(
					disruption,
					corev1.EventTypeNormal,
					"PodAlreadyKilled",
					"Pod %s in namespace %s was already killed",
					pod.Name,
					pod.Namespace,
				)
				r.Logger.Info("Pod already killed", "pod", pod.Name, "namespace", pod.Namespace)
			}
			continue
		}
		r.Logger.Info("Successfully killed pod", "pod", pod.Name, "namespace", pod.Namespace)
		r.Recorder.Eventf(
			disruption,
			corev1.EventTypeNormal,
			"PodKilled",
			"Killed pod %s in namespace %s",
			pod.Name,
			pod.Namespace,
		)
		killed++
	}

	return killed, nil
}

// setCondition sets a condition on the disruption status
//
// Arguments:
// - disruption: The disruption object to update
// - condType: The type of condition
// - status: The status of the condition
// - reason: The reason for the condition
// - message: The message for the condition
func (r *DisruptionReconciler) setCondition(
	disruption *chaosv1.Disruption,
	condType string,
	status metav1.ConditionStatus,
	reason string,
	message string,
) {
	meta.SetStatusCondition(&disruption.Status.Conditions, metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: disruption.Generation,
		LastTransitionTime: metav1.Now(),
	})
}
