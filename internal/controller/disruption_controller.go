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
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	chaosv1 "github.com/andriy-zh-ua/chaos-operator/api/v1"
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/errors"
)

const (
	// Safety configuration constants
	DefaultDurationSeconds       = 300 // 5 minutes per disruption cycle
	DefaultMaxPodsAffected       = 1   // 1 pod per reconciliation cycle
	DefaultMaxPercentageAffected = 10  // 10% per reconciliation cycle

	// Reconciliation interval constants
	MonitoringRequeueInterval = 30 * time.Second

	// Validation limits constants
	MaxCountLimit         = 100 // Maximum allowed count for fixed-count mode
	MaxGracePeriodSeconds = 300 // Maximum allowed grace period in seconds (equal to default duration)

	// Disruption phase constants
	PhaseCompleted = "Completed"
	PhaseFailed    = "Failed"
	PhaseRunning   = "Running"
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

// DisruptionReconciler reconciles a Disruption object
type DisruptionReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Logger logr.Logger

	defaultSafetyConfig   chaosv1.SafetyConfig
	monitoringInterval    time.Duration
	maxCountLimit         int32
	maxGracePeriodSeconds *int64
	systemNamespaces      map[string]bool // Cache of protected system namespaces
}

// NewDisruptionReconciler creates a new disruption reconciler
func NewDisruptionReconciler(mgr ctrl.Manager) *DisruptionReconciler {
	// Create a new reconciler
	r := &DisruptionReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Logger: ctrl.Log.WithName("controllers").WithName("Disruption"),
	}

	// Initialize default safety configuration
	r.defaultSafetyConfig = chaosv1.SafetyConfig{
		MaxDurationSeconds:    r.parseEnvInt32("CHAOS_DEFAULT_DURATION_SECONDS", DefaultDurationSeconds),
		MaxPodsAffected:       r.parseEnvInt32("CHAOS_DEFAULT_MAX_PODS", DefaultMaxPodsAffected),
		MaxPercentageAffected: r.parseEnvInt32("CHAOS_DEFAULT_MAX_PERCENTAGE", DefaultMaxPercentageAffected),
	}

	// Initialize default monitoring interval
	r.monitoringInterval = time.Duration(r.parseEnvInt32("CHAOS_MONITORING_REQUEUE_INTERVAL", int32(MonitoringRequeueInterval.Seconds()))) * time.Second

	// Initialize max count limit
	r.maxCountLimit = r.parseEnvInt32("CHAOS_MAX_COUNT_LIMIT", MaxCountLimit)

	// Initialize max grace period limit
	maxGracePeriod := r.parseEnvInt64("CHAOS_MAX_GRACE_PERIOD_SECONDS", MaxGracePeriodSeconds)
	r.maxGracePeriodSeconds = &maxGracePeriod

	// Initialize system namespaces
	r.systemNamespaces = r.parseEnvSystemNamespaces("CHAOS_SYSTEM_NAMESPACES", DefaultSystemNamespaces)

	return r
}

// +kubebuilder:rbac:groups=chaos.a2solutions.ca,resources=disruptions,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=chaos.a2solutions.ca,resources=disruptions/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=chaos.a2solutions.ca,resources=disruptions/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;delete

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

	// Validate PodKill configuration
	if err := r.validatePodKill(disruption.Spec.PodKill); err != nil {
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

	// Get safety configuration with defaults
	safetyConfig := r.getSafetyConfig(disruption)

	// Ensures the Disruption cannot run longer than allowed
	if safetyConfig.MaxDurationSeconds > 0 {
		duration := time.Since(disruption.Status.StartTime.Time)

		// If the disruption has run longer than the max duration
		if duration > time.Duration(safetyConfig.MaxDurationSeconds)*time.Second {
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
	return ctrl.Result{RequeueAfter: MonitoringRequeueInterval}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *DisruptionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&chaosv1.Disruption{}).
		Named("disruption").
		Complete(r)
}

// validatePodKill validates the PodKill specification
func (r *DisruptionReconciler) validatePodKill(spec *chaosv1.PodKillSpec) error {
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
		maxDuration := time.Duration(r.defaultSafetyConfig.MaxDurationSeconds) * time.Second
		if spec.Duration.Duration > maxDuration {
			return fmt.Errorf("podKill.duration '%v' exceeds maximum allowed limit of %v", spec.Duration.Duration, maxDuration)
		}
	}

	// Validate count only when killMode is fixed-count
	if spec.KillMode != "fixed-count" && spec.Count > 0 {
		return fmt.Errorf("podKill.count is only valid when killMode is 'fixed-count', but killMode is '%s'", spec.KillMode)
	}

	// Validate count is positive when killMode is fixed-count
	if spec.KillMode == "fixed-count" && spec.Count <= 0 {
		return fmt.Errorf("podKill.count must be > 0 when killMode is 'fixed-count'")
	}

	// Validate count does not exceed maximum limit
	if spec.Count > r.maxCountLimit {
		return fmt.Errorf("podKill.count '%d' exceeds maximum allowed limit of %d", spec.Count, r.maxCountLimit)
	}

	// Validate grace period does not exceed maximum limit
	if spec.GracePeriodSeconds != nil && r.maxGracePeriodSeconds != nil && *spec.GracePeriodSeconds > *r.maxGracePeriodSeconds {
		return fmt.Errorf("podKill.gracePeriodSeconds '%d' exceeds maximum allowed limit of %d", *spec.GracePeriodSeconds, *r.maxGracePeriodSeconds)
	}

	return nil
}

// getSafetyConfig returns the safety configuration for a disruption
func (r *DisruptionReconciler) getSafetyConfig(disruption chaosv1.Disruption) chaosv1.SafetyConfig {
	if disruption.Spec.Safety == nil {
		// Return default safety config
		return chaosv1.SafetyConfig{
			MaxDurationSeconds:    r.defaultSafetyConfig.MaxDurationSeconds,
			MaxPodsAffected:       r.defaultSafetyConfig.MaxPodsAffected,
			MaxPercentageAffected: r.defaultSafetyConfig.MaxPercentageAffected,
		}
	}

	// Apply defaults for missing fields
	config := *disruption.Spec.Safety
	if config.MaxDurationSeconds == 0 {
		config.MaxDurationSeconds = r.defaultSafetyConfig.MaxDurationSeconds
	}
	if config.MaxPodsAffected == 0 {
		config.MaxPodsAffected = r.defaultSafetyConfig.MaxPodsAffected
	}
	if config.MaxPercentageAffected == 0 {
		config.MaxPercentageAffected = r.defaultSafetyConfig.MaxPercentageAffected
	}
	return config
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
	case PhaseCompleted, PhaseFailed:
		disruption.Status.EndTime = &now
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

// updateDisruptionStatusWithPodsAffected updates the disruption status with pods affected information
func (r *DisruptionReconciler) updateDisruptionStatusWithPodsAffected(ctx context.Context, disruption *chaosv1.Disruption, podsAffected int32) error {
	// Update the status fields
	disruption.Status.PodsAffected = podsAffected

	// Update the status in Kubernetes
	if err := r.Status().Update(ctx, disruption); err != nil {
		r.Logger.Error(err, "Failed to update disruption status")
		return fmt.Errorf("failed to update disruption status: %w", err)
	}

	r.Logger.Info("Updated disruption status", "podsAffected", podsAffected)
	return nil
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
		// Update status to show no pods affected
		return r.updateDisruptionStatusWithPodsAffected(ctx, disruption, 0)
	}

	// Calculate how many pods we can kill this cycle
	allowed := r.calculateAllowedKillsPerCycle(safetyConfig, len(pods))
	if allowed <= 0 {
		r.Logger.Info("Safety limits reached - no pods will be killed this cycle")
		// Update status to show no pods affected due to safety limits
		return r.updateDisruptionStatusWithPodsAffected(ctx, disruption, 0)
	}

	r.Logger.Info("Target pods", "count", len(pods))

	// TODO: Actually kill pods here
	// For now, just update status to show how many pods would be affected
	return r.updateDisruptionStatusWithPodsAffected(ctx, disruption, int32(allowed))
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

	// Enforce MaxPodsAffected against the currently available pods
	if safetyConfig.MaxPodsAffected > 0 && int(safetyConfig.MaxPodsAffected) < maxAllowed {
		maxAllowed = int(safetyConfig.MaxPodsAffected)
	}

	// Enforce MaxPercentageAffected against the currently available pods
	if safetyConfig.MaxPercentageAffected > 0 {
		maxFromPercent := int(float64(available) * float64(safetyConfig.MaxPercentageAffected) / 100.0)
		if maxFromPercent < maxAllowed {
			maxAllowed = maxFromPercent
		}
	}

	return maxAllowed
}
