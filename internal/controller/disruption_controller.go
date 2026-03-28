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
	"strconv"
	"time"

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
	DefaultDurationSeconds       = 300 // 5 minutes
	DefaultMaxPodsAffected       = 1   // 1 pod
	DefaultMaxPercentageAffected = 10  // 10%

	// Reconciliation intervals
	MonitoringRequeueInterval = 30 * time.Second // Regular monitoring

	// Validation limits
	MaxCountLimit = 100 // Maximum allowed count for fixed-count mode

	// Grace period limits
	MaxGracePeriodSeconds = DefaultDurationSeconds // Maximum allowed grace period in seconds (equal to default duration)
)

// DisruptionReconciler reconciles a Disruption object
type DisruptionReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Logger logr.Logger

	defaultSafetyConfig   chaosv1.SafetyConfig
	monitoringInterval    time.Duration
	maxCountLimit         int32
	maxGracePeriodSeconds int64
}

// NewDisruptionReconciler creates a new disruption reconciler
func NewDisruptionReconciler(mgr ctrl.Manager) *DisruptionReconciler {
	// Create a new reconciler
	r := &DisruptionReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Logger: ctrl.Log.WithName("controllers").WithName("Disruption"),
	}

	// Initialize defaults
	r.defaultSafetyConfig = chaosv1.SafetyConfig{
		MaxDurationSeconds:    r.getInt32Env("CHAOS_DEFAULT_DURATION_SECONDS", DefaultDurationSeconds),
		MaxPodsAffected:       r.getInt32Env("CHAOS_DEFAULT_MAX_PODS", DefaultMaxPodsAffected),
		MaxPercentageAffected: r.getInt32Env("CHAOS_DEFAULT_MAX_PERCENTAGE", DefaultMaxPercentageAffected),
	}

	// Set monitoring interval
	r.monitoringInterval = time.Duration(r.getInt32Env("CHAOS_MONITORING_REQUEUE_INTERVAL", int32(MonitoringRequeueInterval.Seconds()))) * time.Second

	// Set max count limit
	r.maxCountLimit = r.getInt32Env("CHAOS_MAX_COUNT_LIMIT", MaxCountLimit)

	// Set max grace period limit
	r.maxGracePeriodSeconds = r.getInt64Env("CHAOS_MAX_GRACE_PERIOD_SECONDS", MaxGracePeriodSeconds)

	return r
}

// +kubebuilder:rbac:groups=chaos.a2solutions.ca,resources=disruptions,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=chaos.a2solutions.ca,resources=disruptions/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=chaos.a2solutions.ca,resources=disruptions/finalizers,verbs=update

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
	if disruption.Status.Phase == "Completed" || disruption.Status.Phase == "Failed" {
		// No further reconciliation needed - disruption is finished
		return ctrl.Result{}, nil
	}

	r.Logger.Info("Reconciling Disruption", "name", disruption.Name, "namespace", disruption.Namespace)

	// Validate PodKill configuration
	if err := r.validatePodKill(disruption.Spec.PodKill); err != nil {
		r.Logger.Error(err, "Invalid PodKill configuration")
		if err := r.markDisruptionFailed(ctx, &disruption, r.Logger); err != nil {
			// Return error to trigger exponential backoff and retry until the status update succeeds
			return ctrl.Result{}, err
		}
		r.Logger.Info("Disruption marked as failed due to invalid PodKill configuration")
		// No further reconciliation needed - disruption is finished
		return ctrl.Result{}, nil
	}

	// If experiment has no start time, mark as Running and set start time
	if disruption.Status.StartTime == nil {
		if err := r.markDisruptionRunning(ctx, &disruption, r.Logger); err != nil {
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
			if err := r.markDisruptionCompleted(ctx, &disruption, r.Logger); err != nil {
				// Return error to trigger exponential backoff and retry until the status update succeeds
				return ctrl.Result{}, err
			}
			r.Logger.Info("Disruption completed due to max duration")
			// No further reconciliation needed - disruption is finished
			return ctrl.Result{}, nil
		}
	}

	// Execute PodKill
	if err := r.executePodKill(ctx, &disruption); err != nil {
		r.Logger.Error(err, "Failed to execute PodKill")
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
	if spec.GracePeriodSeconds > r.maxGracePeriodSeconds {
		return fmt.Errorf("podKill.gracePeriodSeconds '%d' exceeds maximum allowed limit of %d", spec.GracePeriodSeconds, r.maxGracePeriodSeconds)
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

// getInt32Env parses an environment variable as an int32 with a default value
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
func (r *DisruptionReconciler) getInt32Env(key string, defaultValue int32) int32 {
	if value := os.Getenv(key); value != "" {
		if parsed, err := strconv.ParseInt(value, 10, 32); err == nil {
			return int32(parsed)
		}
	}
	// Log that we're using default value
	r.Logger.Info("Using default value for %s: %d (environment variable not set or invalid)", key, defaultValue)
	return defaultValue
}

// getInt64Env parses an environment variable as an int64 with a default value
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
func (r *DisruptionReconciler) getInt64Env(key string, defaultValue int64) int64 {
	if value := os.Getenv(key); value != "" {
		if parsed, err := strconv.ParseInt(value, 10, 64); err == nil {
			return parsed
		}
		r.Logger.Info("Invalid %s, using default %d", key, defaultValue)
	}
	// Log that we're using default value
	r.Logger.Info("Using default value for %s: %d (environment variable not set or invalid)", key, defaultValue)
	return defaultValue
}

// updateDisruptionStatus updates the disruption status and handles errors
func (r *DisruptionReconciler) updateDisruptionStatus(ctx context.Context, disruption *chaosv1.Disruption, phase string, logger logr.Logger) error {
	disruption.Status.Phase = phase

	now := metav1.Now()

	switch phase {
	case "Running":
		disruption.Status.StartTime = &now
	case "Completed", "Failed":
		disruption.Status.EndTime = &now
	}

	if err := r.Status().Update(ctx, disruption); err != nil {
		logger.Error(err, "Failed to update disruption status")
		return err
	}

	return nil
}

// markDisruptionRunning marks disruption as running
func (r *DisruptionReconciler) markDisruptionRunning(ctx context.Context, disruption *chaosv1.Disruption, logger logr.Logger) error {
	return r.updateDisruptionStatus(ctx, disruption, "Running", logger)
}

// markDisruptionCompleted marks disruption as completed
func (r *DisruptionReconciler) markDisruptionCompleted(ctx context.Context, disruption *chaosv1.Disruption, logger logr.Logger) error {
	return r.updateDisruptionStatus(ctx, disruption, "Completed", logger)
}

// markDisruptionFailed marks disruption as failed
func (r *DisruptionReconciler) markDisruptionFailed(ctx context.Context, disruption *chaosv1.Disruption, logger logr.Logger) error {
	return r.updateDisruptionStatus(ctx, disruption, "Failed", logger)
}

// executePodKill performs the actual pod killing logic
func (r *DisruptionReconciler) executePodKill(ctx context.Context, disruption *chaosv1.Disruption) error {
	r.Logger.Info("Executing PodKill for disruption:", "name", disruption.Name)
	return nil
}
