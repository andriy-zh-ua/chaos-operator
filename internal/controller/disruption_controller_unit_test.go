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
	return &mockErrorClient{
		Client:      fakeClient,
		statusError: statusError,
	}
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
			name: "empty selector - should not return error",
			disruption: chaosv1.Disruption{
				Spec: chaosv1.DisruptionSpec{
					PodKill: &chaosv1.PodKillSpec{
						Selector: &metav1.LabelSelector{},
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
						KillMode: "fixed-count",
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
						KillMode: "fixed-count",
						Count:    101,
					},
				},
			},
			expectError: true,
			errorMsg:    "podKill.count '101' exceeds maximum allowed limit of 100",
		},
		{
			name: "gracePeriod exceeding max limit - should return error",
			disruption: chaosv1.Disruption{
				Spec: chaosv1.DisruptionSpec{
					PodKill: &chaosv1.PodKillSpec{
						GracePeriodSeconds: 301,
					},
				},
			},
			expectError: true,
			errorMsg:    "podKill.gracePeriodSeconds '301' exceeds maximum allowed limit of 300",
		},
		{
			name: "valid configuration - should not return error",
			disruption: chaosv1.Disruption{
				Spec: chaosv1.DisruptionSpec{
					PodKill: &chaosv1.PodKillSpec{
						KillMode:           "fixed-count",
						Count:              5,
						GracePeriodSeconds: 30,
					},
				},
			},
			expectError: false,
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
			name: "valid positive duration - should not return error",
			disruption: chaosv1.Disruption{
				Spec: chaosv1.DisruptionSpec{
					PodKill: &chaosv1.PodKillSpec{
						Duration: &metav1.Duration{Duration: 30000000000}, // 30 seconds
					},
				},
			},
			expectError: false,
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
			errorMsg:    "podKill.duration '10m0s' exceeds maximum allowed limit of 5m0s",
		},
		{
			name: "random killMode without count - should not return error",
			disruption: chaosv1.Disruption{
				Spec: chaosv1.DisruptionSpec{
					PodKill: &chaosv1.PodKillSpec{
						KillMode: "random",
						// Count is 0 by default, which is fine for random mode
					},
				},
			},
			expectError: false,
		},
		{
			name: "all killMode without count - should not return error",
			disruption: chaosv1.Disruption{
				Spec: chaosv1.DisruptionSpec{
					PodKill: &chaosv1.PodKillSpec{
						KillMode: "all",
						// Count is 0 by default, which is correct for 'all' mode
					},
				},
			},
			expectError: false,
		},
		{
			name: "all killMode with count - should return error (count only valid with fixed-count)",
			disruption: chaosv1.Disruption{
				Spec: chaosv1.DisruptionSpec{
					PodKill: &chaosv1.PodKillSpec{
						KillMode: "all",
						Count:    5,
					},
				},
			},
			expectError: true,
			errorMsg:    "podKill.count is only valid when killMode is 'fixed-count', but killMode is 'all'",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Create reconciler with default limits
			r := &DisruptionReconciler{
				maxCountLimit:         100,
				maxGracePeriodSeconds: 300,
				defaultSafetyConfig: chaosv1.SafetyConfig{
					MaxDurationSeconds:    300, // 5 minutes
					MaxPodsAffected:       5,
					MaxPercentageAffected: 20,
				},
			}

			err := r.validatePodKill(test.disruption.Spec.PodKill)

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
				MaxDurationSeconds:    300,
				MaxPodsAffected:       5,
				MaxPercentageAffected: 20,
			},
			expectedResult: chaosv1.SafetyConfig{
				MaxDurationSeconds:    300,
				MaxPodsAffected:       5,
				MaxPercentageAffected: 20,
			},
		},
		{
			name: "complete safety config - should return as-is",
			disruption: chaosv1.Disruption{
				Spec: chaosv1.DisruptionSpec{
					Safety: &chaosv1.SafetyConfig{
						MaxDurationSeconds:    600,
						MaxPodsAffected:       10,
						MaxPercentageAffected: 50,
					},
				},
			},
			defaultConfig: chaosv1.SafetyConfig{
				MaxDurationSeconds:    300,
				MaxPodsAffected:       5,
				MaxPercentageAffected: 20,
			},
			expectedResult: chaosv1.SafetyConfig{
				MaxDurationSeconds:    600,
				MaxPodsAffected:       10,
				MaxPercentageAffected: 50,
			},
		},
		{
			name: "partial safety config - missing duration should use default",
			disruption: chaosv1.Disruption{
				Spec: chaosv1.DisruptionSpec{
					Safety: &chaosv1.SafetyConfig{
						MaxDurationSeconds:    0, // Missing
						MaxPodsAffected:       8,
						MaxPercentageAffected: 30,
					},
				},
			},
			defaultConfig: chaosv1.SafetyConfig{
				MaxDurationSeconds:    300,
				MaxPodsAffected:       5,
				MaxPercentageAffected: 20,
			},
			expectedResult: chaosv1.SafetyConfig{
				MaxDurationSeconds:    300, // From default
				MaxPodsAffected:       8,   // From disruption
				MaxPercentageAffected: 30,  // From disruption
			},
		},
		{
			name: "partial safety config - missing pods should use default",
			disruption: chaosv1.Disruption{
				Spec: chaosv1.DisruptionSpec{
					Safety: &chaosv1.SafetyConfig{
						MaxDurationSeconds:    400,
						MaxPodsAffected:       0, // Missing
						MaxPercentageAffected: 40,
					},
				},
			},
			defaultConfig: chaosv1.SafetyConfig{
				MaxDurationSeconds:    300,
				MaxPodsAffected:       5,
				MaxPercentageAffected: 20,
			},
			expectedResult: chaosv1.SafetyConfig{
				MaxDurationSeconds:    400, // From disruption
				MaxPodsAffected:       5,   // From default
				MaxPercentageAffected: 40,  // From disruption
			},
		},
		{
			name: "partial safety config - missing percentage should use default",
			disruption: chaosv1.Disruption{
				Spec: chaosv1.DisruptionSpec{
					Safety: &chaosv1.SafetyConfig{
						MaxDurationSeconds:    400,
						MaxPodsAffected:       8,
						MaxPercentageAffected: 0, // Missing
					},
				},
			},
			defaultConfig: chaosv1.SafetyConfig{
				MaxDurationSeconds:    300,
				MaxPodsAffected:       5,
				MaxPercentageAffected: 20,
			},
			expectedResult: chaosv1.SafetyConfig{
				MaxDurationSeconds:    400, // From disruption
				MaxPodsAffected:       8,   // From disruption
				MaxPercentageAffected: 20,  // From default
			},
		},
		{
			name: "all zero values - should use all defaults",
			disruption: chaosv1.Disruption{
				Spec: chaosv1.DisruptionSpec{
					Safety: &chaosv1.SafetyConfig{
						MaxDurationSeconds:    0,
						MaxPodsAffected:       0,
						MaxPercentageAffected: 0,
					},
				},
			},
			defaultConfig: chaosv1.SafetyConfig{
				MaxDurationSeconds:    300,
				MaxPodsAffected:       5,
				MaxPercentageAffected: 20,
			},
			expectedResult: chaosv1.SafetyConfig{
				MaxDurationSeconds:    300,
				MaxPodsAffected:       5,
				MaxPercentageAffected: 20,
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
			if result.MaxDurationSeconds != test.expectedResult.MaxDurationSeconds {
				t.Errorf("Expected MaxDurationSeconds %d, got %d",
					test.expectedResult.MaxDurationSeconds, result.MaxDurationSeconds)
			}

			// Assert if max pods affected is as expected
			if result.MaxPodsAffected != test.expectedResult.MaxPodsAffected {
				t.Errorf("Expected MaxPodsAffected %d, got %d",
					test.expectedResult.MaxPodsAffected, result.MaxPodsAffected)
			}

			// Assert if max percentage affected is as expected
			if result.MaxPercentageAffected != test.expectedResult.MaxPercentageAffected {
				t.Errorf("Expected MaxPercentageAffected %d, got %d",
					test.expectedResult.MaxPercentageAffected, result.MaxPercentageAffected)
			}
		})
	}
}

func TestGetInt32Env(t *testing.T) {
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
				os.Setenv(test.envKey, test.envValue)
				if test.shouldUnset {
					defer os.Unsetenv(test.envKey)
				}
			}

			value := r.getInt32Env(test.envKey, test.defaultValue)

			// Assert if value is not as expected
			if value != test.expected {
				t.Errorf("Test %s: getInt32Env(%s, %d) = %d, want %d",
					test.name, test.envKey, test.defaultValue, value, test.expected)
			}
		})
	}
}

func TestGetInt64Env(t *testing.T) {
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
				os.Setenv(test.envKey, test.envValue)
				if test.shouldUnset {
					defer os.Unsetenv(test.envKey)
				}
			}

			value := r.getInt64Env(test.envKey, test.defaultValue)

			// Assert if value is not as expected
			if value != test.expected {
				t.Errorf("Test %s: getInt64Env(%s, %d) = %d, want %d",
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
				Client: newMockErrorClient(test.statusUpdateError),
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

			err := r.updateDisruptionStatus(context.Background(), disruption, test.targetPhase, logr.Discard())

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
		Client: newMockErrorClient(nil),
		Logger: logr.Discard(),
	}

	// Create disruption
	disruption := &chaosv1.Disruption{}

	err := r.markDisruptionRunning(context.Background(), disruption, r.Logger)

	// Assert if error occurred
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

func TestMarkDisruptionFailed(t *testing.T) {
	// Create reconciler with mock client
	r := &DisruptionReconciler{
		Client: newMockErrorClient(nil),
		Logger: logr.Discard(),
	}

	// Create disruption
	disruption := &chaosv1.Disruption{}

	err := r.markDisruptionFailed(context.Background(), disruption, r.Logger)

	// Assert if error occurred
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

func TestMarkDisruptionCompleted(t *testing.T) {
	// Create reconciler with mock client
	r := &DisruptionReconciler{
		Client: newMockErrorClient(nil),
		Logger: logr.Discard(),
	}

	// Create disruption
	disruption := &chaosv1.Disruption{}

	err := r.markDisruptionCompleted(context.Background(), disruption, r.Logger)

	// Assert if error occurred
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

// // Test individual functions in isolation
// func TestValidatePodKill(t *testing.T) {
// 	panic("unimplemented")
// }
