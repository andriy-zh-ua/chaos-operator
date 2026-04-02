package controller

import (
	"context"
	"fmt"
	"os"
	"testing"

	chaosv1 "github.com/andriy-zh-ua/chaos-operator/api/v1"
	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type mockStatusWriter struct {
	err error
}

func (m *mockStatusWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	return m.err
}

func (m *mockStatusWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	return m.err
}

func (m *mockStatusWriter) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
	return m.err
}

func (m *mockStatusWriter) Create(ctx context.Context, obj client.Object, obj2 client.Object, opts ...client.SubResourceCreateOption) error {
	return m.err
}

type mockErrorClient struct {
	client.Client
	statusError error
}

func (m *mockErrorClient) Status() client.StatusWriter {
	return &mockStatusWriter{err: m.statusError}
}

// newMockErrorClient creates a mock client that returns an error when updating status
func newMockErrorClient(statusError error) *mockErrorClient {
	scheme := runtime.NewScheme()
	_ = chaosv1.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	return &mockErrorClient{Client: fakeClient, statusError: statusError}
}

// Helper function to create int64 pointer
func int64Ptr(i int64) *int64 {
	return &i
}

func TestNewDisruptionReconciler(t *testing.T) {
	// Note: We can't easily test NewDisruptionReconciler initialization without a full ctrl.Manager
	// This test only verifies basic function callability and error handling
	// Full testing of field assignments and environment variable handling would require envtest/integration tests

	t.Run("function is callable", func(t *testing.T) {
		// Create a test scheme
		scheme := runtime.NewScheme()
		_ = chaosv1.AddToScheme(scheme)

		// This will panic because we're passing nil, but that's expected
		// The important thing is that the function exists and is callable
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("Expected panic when calling NewDisruptionReconciler with nil manager")
			}
		}()

		_ = NewDisruptionReconciler(nil)
	})
}

func TestSetupWithManager(t *testing.T) {
	// This test verifies that SetupWithManager has the correct signature and basic behavior
	// Full integration testing of controller setup is typically done in envtest/integration tests

	t.Run("basic function signature test", func(t *testing.T) {
		// Create a test scheme
		scheme := runtime.NewScheme()
		_ = chaosv1.AddToScheme(scheme)

		// Create reconciler
		r := &DisruptionReconciler{
			Scheme: scheme,
		}

		err := r.SetupWithManager(nil)

		// When called with nil manager, it should return an error rather than panic
		if err == nil {
			t.Errorf("Expected error when calling SetupWithManager with nil manager, got nil")
		}
	})
}

func TestValidatePodKill(t *testing.T) {
	tests := []struct {
		name        string
		disruption  chaosv1.Disruption
		expectError bool
		errorMsg    string
	}{
		{
			name: "nil podKill spec - should return error",
			disruption: chaosv1.Disruption{
				Spec: chaosv1.DisruptionSpec{
					PodKill: nil,
				},
			},
			expectError: true,
			errorMsg:    "no PodKill specification provided - nothing to disrupt",
		},
		{
			name: "invalid scope name - should return error",
			disruption: chaosv1.Disruption{
				Spec: chaosv1.DisruptionSpec{
					PodKill: &chaosv1.PodKillSpec{
						Scope: "invalid_scope_name",
					},
				},
			},
			expectError: true,
			errorMsg:    "invalid scope: invalid_scope_name. Must be 'namespace' or 'cluster'",
		},
		{
			name: "nil selector - should not return error",
			disruption: chaosv1.Disruption{
				Spec: chaosv1.DisruptionSpec{
					PodKill: &chaosv1.PodKillSpec{
						Selector: nil,
					},
				},
			},
			expectError: false,
		},
		{
			name: "empty selector (exists but no MatchLabels or MatchExpressions) - should not return error",
			disruption: chaosv1.Disruption{
				Spec: chaosv1.DisruptionSpec{
					PodKill: &chaosv1.PodKillSpec{
						Selector: &metav1.LabelSelector{
							// Empty selector - no MatchLabels or MatchExpressions
						},
					},
				},
			},
			expectError: false,
		},
		{
			name: "negative duration - should return error",
			disruption: chaosv1.Disruption{
				Spec: chaosv1.DisruptionSpec{
					PodKill: &chaosv1.PodKillSpec{
						// time.Duration(-1)      // -1ns (nanoseconds)
						// time.Duration(-1000)   // -1µs (microseconds)
						// time.Duration(-1000000) // -1ms (milliseconds)
						// time.Duration(-1000000000) // -1s (seconds)
						Duration: &metav1.Duration{Duration: -1},
					},
				},
			},
			expectError: true,
			errorMsg:    "podKill.duration must be positive, got -1ns",
		},
		{
			name: "zero duration - should return error",
			disruption: chaosv1.Disruption{
				Spec: chaosv1.DisruptionSpec{
					PodKill: &chaosv1.PodKillSpec{
						Duration: &metav1.Duration{Duration: 0},
					},
				},
			},
			expectError: true,
			errorMsg:    "podKill.duration must be positive, got 0s",
		},
		{
			name: "duration exceeding safety limit - should return error",
			disruption: chaosv1.Disruption{
				Spec: chaosv1.DisruptionSpec{
					PodKill: &chaosv1.PodKillSpec{
						Duration: &metav1.Duration{Duration: 600000000000}, // 10 minutes (exceeds 5min default)
					},
				},
			},
			expectError: true,
			errorMsg:    "podKill.duration 10m0s exceeds maximum allowed limit of 5m0s",
		},
		{
			name: "count with non-fixed-count killMode - should return error",
			disruption: chaosv1.Disruption{
				Spec: chaosv1.DisruptionSpec{
					PodKill: &chaosv1.PodKillSpec{
						KillMode: "random",
						Count:    5,
					},
				},
			},
			expectError: true,
			errorMsg:    "podKill.count is only valid when killMode is 'fixed-count', but killMode is 'random'",
		},
		{
			name: "zero count with fixed-count killMode - should return error",
			disruption: chaosv1.Disruption{
				Spec: chaosv1.DisruptionSpec{
					PodKill: &chaosv1.PodKillSpec{
						KillMode: KillModeFixedCount,
						Count:    0,
					},
				},
			},
			expectError: true,
			errorMsg:    "podKill.count must be > 0 when killMode is 'fixed-count'",
		},
		{
			name: "count exceeding max limit - should return error",
			disruption: chaosv1.Disruption{
				Spec: chaosv1.DisruptionSpec{
					PodKill: &chaosv1.PodKillSpec{
						KillMode: KillModeFixedCount,
						Count:    101,
					},
				},
			},
			expectError: true,
			errorMsg:    "podKill.count '101' exceeds maximum allowed limit of 100",
		},
		{
			name: "fixed-count killMode with count exceeding PodsAffected - should return error",
			disruption: chaosv1.Disruption{
				Spec: chaosv1.DisruptionSpec{
					PodKill: &chaosv1.PodKillSpec{
						KillMode: KillModeFixedCount,
						Count:    10,
					},
					Safety: &chaosv1.SafetyConfig{
						PodsAffected: 5,
						CountLimit:   20,
					},
				},
			},
			expectError: true,
			errorMsg:    "podKill.count 10 exceeds safety PodsAffected limit of 5",
		},
		{
			name: "gracePeriod exceeding max limit - should return error",
			disruption: chaosv1.Disruption{
				Spec: chaosv1.DisruptionSpec{
					PodKill: &chaosv1.PodKillSpec{
						GracePeriodSeconds: int64Ptr(301),
					},
				},
			},
			expectError: true,
			errorMsg:    "podKill.gracePeriodSeconds 301 exceeds maximum allowed limit of 300",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Create reconciler with default limits
			r := &DisruptionReconciler{
				defaultSafetyConfig: chaosv1.SafetyConfig{
					DurationSeconds:    300, // 5 minutes
					PodsAffected:       5,
					PercentageAffected: 20,
					CountLimit:         100,
					GracePeriodSeconds: int64(300),
				},
			}

			// Get safety config (same logic as main code)
			safetyConfig := test.disruption.Spec.Safety
			if safetyConfig == nil {
				safetyConfig = &chaosv1.SafetyConfig{
					DurationSeconds:    300, // 5 minutes
					PodsAffected:       5,
					PercentageAffected: 20,
					CountLimit:         100,
					GracePeriodSeconds: int64(300),
				}
			}

			err := r.validatePodKill(test.disruption.Spec.PodKill, *safetyConfig)

			if test.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				} else if err.Error() != test.errorMsg {
					t.Errorf("Expected error message '%s', got '%s'", test.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error but got: %v", err)
				}
			}
		})
	}
}

func TestGetSafetyConfig(t *testing.T) {
	tests := []struct {
		name           string
		disruption     chaosv1.Disruption
		defaultConfig  chaosv1.SafetyConfig
		expectedResult chaosv1.SafetyConfig
	}{
		{
			name: "nil safety config - should return defaults",
			disruption: chaosv1.Disruption{
				Spec: chaosv1.DisruptionSpec{
					Safety: nil,
				},
			},
			defaultConfig: chaosv1.SafetyConfig{
				DurationSeconds:    300,
				PodsAffected:       5,
				PercentageAffected: 20,
			},
			expectedResult: chaosv1.SafetyConfig{
				DurationSeconds:    300,
				PodsAffected:       5,
				PercentageAffected: 20,
			},
		},
		{
			name: "complete safety config - should return as-is",
			disruption: chaosv1.Disruption{
				Spec: chaosv1.DisruptionSpec{
					Safety: &chaosv1.SafetyConfig{
						DurationSeconds:    600,
						PodsAffected:       10,
						PercentageAffected: 50,
					},
				},
			},
			defaultConfig: chaosv1.SafetyConfig{
				DurationSeconds:    300,
				PodsAffected:       5,
				PercentageAffected: 20,
			},
			expectedResult: chaosv1.SafetyConfig{
				DurationSeconds:    600,
				PodsAffected:       10,
				PercentageAffected: 50,
			},
		},
		{
			name: "partial safety config - missing duration should use default",
			disruption: chaosv1.Disruption{
				Spec: chaosv1.DisruptionSpec{
					Safety: &chaosv1.SafetyConfig{
						DurationSeconds:    0, // Missing
						PodsAffected:       8,
						PercentageAffected: 30,
					},
				},
			},
			defaultConfig: chaosv1.SafetyConfig{
				DurationSeconds:    300,
				PodsAffected:       5,
				PercentageAffected: 20,
			},
			expectedResult: chaosv1.SafetyConfig{
				DurationSeconds:    300, // From default
				PodsAffected:       8,   // From disruption
				PercentageAffected: 30,  // From disruption
			},
		},
		{
			name: "partial safety config - missing pods should use default",
			disruption: chaosv1.Disruption{
				Spec: chaosv1.DisruptionSpec{
					Safety: &chaosv1.SafetyConfig{
						DurationSeconds:    400,
						PodsAffected:       0, // Missing
						PercentageAffected: 40,
					},
				},
			},
			defaultConfig: chaosv1.SafetyConfig{
				DurationSeconds:    300,
				PodsAffected:       5,
				PercentageAffected: 20,
			},
			expectedResult: chaosv1.SafetyConfig{
				DurationSeconds:    400, // From disruption
				PodsAffected:       5,   // From default
				PercentageAffected: 40,  // From disruption
			},
		},
		{
			name: "partial safety config - missing percentage should use default",
			disruption: chaosv1.Disruption{
				Spec: chaosv1.DisruptionSpec{
					Safety: &chaosv1.SafetyConfig{
						DurationSeconds:    400,
						PodsAffected:       8,
						PercentageAffected: 0, // Missing
					},
				},
			},
			defaultConfig: chaosv1.SafetyConfig{
				DurationSeconds:    300,
				PodsAffected:       5,
				PercentageAffected: 20,
			},
			expectedResult: chaosv1.SafetyConfig{
				DurationSeconds:    400, // From disruption
				PodsAffected:       8,   // From disruption
				PercentageAffected: 20,  // From default
			},
		},
		{
			name: "all zero values - should use all defaults",
			disruption: chaosv1.Disruption{
				Spec: chaosv1.DisruptionSpec{
					Safety: &chaosv1.SafetyConfig{
						DurationSeconds:    0,
						PodsAffected:       0,
						PercentageAffected: 0,
					},
				},
			},
			defaultConfig: chaosv1.SafetyConfig{
				DurationSeconds:    300,
				PodsAffected:       5,
				PercentageAffected: 20,
			},
			expectedResult: chaosv1.SafetyConfig{
				DurationSeconds:    300,
				PodsAffected:       5,
				PercentageAffected: 20,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Create reconciler with specific default config
			r := &DisruptionReconciler{
				defaultSafetyConfig: test.defaultConfig,
			}

			result := r.getSafetyConfig(test.disruption)

			// Assert if max duration seconds is as expected
			if result.DurationSeconds != test.expectedResult.DurationSeconds {
				t.Errorf("Expected DurationSeconds %d, got %d",
					test.expectedResult.DurationSeconds, result.DurationSeconds)
			}

			// Assert if max pods affected is as expected
			if result.PodsAffected != test.expectedResult.PodsAffected {
				t.Errorf("Expected PodsAffected %d, got %d",
					test.expectedResult.PodsAffected, result.PodsAffected)
			}

			// Assert if max percentage affected is as expected
			if result.PercentageAffected != test.expectedResult.PercentageAffected {
				t.Errorf("Expected PercentageAffected %d, got %d",
					test.expectedResult.PercentageAffected, result.PercentageAffected)
			}
		})
	}
}

func TestParseEnvInt32(t *testing.T) {
	// Create reconciler
	r := &DisruptionReconciler{}

	tests := []struct {
		name         string
		envKey       string
		envValue     string
		defaultValue int32
		expected     int32
		shouldUnset  bool
	}{
		{
			name:         "valid integer",
			envKey:       "TEST_INT_VALID",
			envValue:     "42",
			defaultValue: 0,
			expected:     42,
			shouldUnset:  true,
		},
		{
			name:         "invalid integer",
			envKey:       "TEST_INT_INVALID",
			envValue:     "not_a_number",
			defaultValue: 99,
			expected:     99,
			shouldUnset:  true,
		},
		{
			name:         "non-existent environment variable",
			envKey:       "NON_EXISTENT_VAR",
			envValue:     "",
			defaultValue: 100,
			expected:     100,
			shouldUnset:  false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Set up environment variable if needed
			if test.envValue != "" {
				if err := os.Setenv(test.envKey, test.envValue); err != nil {
					t.Fatalf("Failed to set environment variable %s: %v", test.envKey, err)
				}
				if test.shouldUnset {
					defer func() {
						if err := os.Unsetenv(test.envKey); err != nil {
							t.Fatalf("Failed to unset environment variable %s: %v", test.envKey, err)
						}
					}()
				}
			}

			value := r.parseEnvInt32(test.envKey, test.defaultValue)

			// Assert if value is not as expected
			if value != test.expected {
				t.Errorf("Test %s: parseEnvInt32(%s, %d) = %d, want %d",
					test.name, test.envKey, test.defaultValue, value, test.expected)
			}
		})
	}
}

func TestParseEnvInt64(t *testing.T) {
	// Create reconciler
	r := &DisruptionReconciler{}

	tests := []struct {
		name         string
		envKey       string
		envValue     string
		defaultValue int64
		expected     int64
		shouldUnset  bool
	}{
		{
			name:         "valid integer",
			envKey:       "TEST_INT_VALID",
			envValue:     "42",
			defaultValue: 0,
			expected:     42,
			shouldUnset:  true,
		},
		{
			name:         "invalid integer",
			envKey:       "TEST_INT_INVALID",
			envValue:     "not_a_number",
			defaultValue: 99,
			expected:     99,
			shouldUnset:  true,
		},
		{
			name:         "non-existent environment variable",
			envKey:       "NON_EXISTENT_VAR",
			envValue:     "",
			defaultValue: 100,
			expected:     100,
			shouldUnset:  false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Set up environment variable if needed
			if test.envValue != "" {
				if err := os.Setenv(test.envKey, test.envValue); err != nil {
					t.Fatalf("Failed to set environment variable %s: %v", test.envKey, err)
				}
				if test.shouldUnset {
					defer func() {
						if err := os.Unsetenv(test.envKey); err != nil {
							t.Fatalf("Failed to unset environment variable %s: %v", test.envKey, err)
						}
					}()
				}
			}

			value := r.parseEnvInt64(test.envKey, test.defaultValue)

			// Assert if value is not as expected
			if value != test.expected {
				t.Errorf("Test %s: parseEnvInt64(%s, %d) = %d, want %d",
					test.name, test.envKey, test.defaultValue, value, test.expected)
			}
		})
	}
}

func TestUpdateDisruptionStatus(t *testing.T) {
	tests := []struct {
		name              string
		initialPhase      string
		targetPhase       string
		statusUpdateError error
	}{
		{
			name:              "mark disruption as running",
			initialPhase:      "",
			targetPhase:       "Running",
			statusUpdateError: nil,
		},
		{
			name:              "mark disruption as completed",
			initialPhase:      "Running",
			targetPhase:       "Completed",
			statusUpdateError: nil,
		},
		{
			name:              "mark disruption as failed",
			initialPhase:      "Running",
			targetPhase:       "Failed",
			statusUpdateError: nil,
		},

		// Error cases
		{
			name:              "status update fails - error",
			initialPhase:      "",
			targetPhase:       "Running",
			statusUpdateError: fmt.Errorf("Failed to update disruption status"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Create reconciler with mock client
			r := &DisruptionReconciler{
				Client:   newMockErrorClient(test.statusUpdateError),
				Recorder: &record.FakeRecorder{},
			}

			// Create disruption with proper initial state
			disruption := &chaosv1.Disruption{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-disruption",
				},
				Status: chaosv1.DisruptionStatus{
					Phase: test.initialPhase,
				},
			}

			// Create the resource in the fake client so that it exists for the status update
			if err := r.Create(context.Background(), disruption); err != nil {
				t.Fatalf("Test %s: failed to create disruption: %v", test.name, err)
			}

			err := r.updateDisruptionStatus(context.Background(), disruption, test.targetPhase)

			// Assert no error
			if err != nil && err.Error() != "Failed to update disruption status" {
				t.Errorf("Test %s: Expected error message 'Failed to update disruption status', got '%s'", test.name, err.Error())
			}

			// Assert phase was updated
			if disruption.Status.Phase != test.targetPhase {
				t.Errorf("Test %s: Expected phase %s, got %s", test.name, test.targetPhase, disruption.Status.Phase)
			}

			// Assert start time was set correctly
			if disruption.Status.Phase == "Running" {
				if disruption.Status.StartTime == nil {
					t.Errorf("Test %s: Expected startTime to be set when phase is Running", test.name)
				}
			}

			// Assert end time was set correctly
			if disruption.Status.Phase == "Completed" || disruption.Status.Phase == "Failed" {
				if disruption.Status.EndTime == nil {
					t.Errorf("Test %s: Expected endTime to be set when phase is Completed or Failed", test.name)
				}
			}
		})
	}
}

func TestMarkDisruptionRunning(t *testing.T) {
	// Create reconciler with mock client
	r := &DisruptionReconciler{
		Client:   newMockErrorClient(nil),
		Logger:   logr.Discard(),
		Recorder: &record.FakeRecorder{},
	}

	// Create disruption
	disruption := &chaosv1.Disruption{}

	err := r.markDisruptionRunning(context.Background(), disruption)

	// Assert if error occurred
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

func TestMarkDisruptionFailed(t *testing.T) {
	// Create reconciler with mock client
	r := &DisruptionReconciler{
		Client:   newMockErrorClient(nil),
		Logger:   logr.Discard(),
		Recorder: &record.FakeRecorder{},
	}

	// Create disruption
	disruption := &chaosv1.Disruption{}

	err := r.markDisruptionFailed(context.Background(), disruption)

	// Assert if error occurred
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

func TestMarkDisruptionCompleted(t *testing.T) {
	// Create reconciler with mock client
	r := &DisruptionReconciler{
		Client:   newMockErrorClient(nil),
		Logger:   logr.Discard(),
		Recorder: &record.FakeRecorder{},
	}

	// Create disruption
	disruption := &chaosv1.Disruption{}

	err := r.markDisruptionCompleted(context.Background(), disruption)

	// Assert if error occurred
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}
