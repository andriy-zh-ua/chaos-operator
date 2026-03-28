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

func TestGetInt32Env(t *testing.T) {
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
		// Set up environment variable if needed
		if test.envValue != "" {
			os.Setenv(test.envKey, test.envValue)
			if test.shouldUnset {
				defer os.Unsetenv(test.envKey)
			}
		}

		value := r.getInt32Env(test.envKey, test.defaultValue)

		if value != test.expected {
			t.Errorf("Test %s: getInt32Env(%s, %d) = %d, want %d",
				test.name, test.envKey, test.defaultValue, value, test.expected)
		}
	}
}

func TestGetInt64Env(t *testing.T) {
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
		// Set up environment variable if needed
		if test.envValue != "" {
			os.Setenv(test.envKey, test.envValue)
			if test.shouldUnset {
				defer os.Unsetenv(test.envKey)
			}
		}

		value := r.getInt64Env(test.envKey, test.defaultValue)

		if value != test.expected {
			t.Errorf("Test %s: getInt64Env(%s, %d) = %d, want %d",
				test.name, test.envKey, test.defaultValue, value, test.expected)
		}
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
		// Initialize reconciler with mock client
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
	}
}

func TestMarkDisruptionRunning(t *testing.T) {
	// Setup mock client (as before)
	r := &DisruptionReconciler{Client: newMockErrorClient(nil), Logger: logr.Discard()}
	disruption := &chaosv1.Disruption{}

	err := r.markDisruptionRunning(context.Background(), disruption, r.Logger)

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

func TestMarkDisruptionFailed(t *testing.T) {
	// Setup mock client (as before)
	r := &DisruptionReconciler{Client: newMockErrorClient(nil), Logger: logr.Discard()}
	disruption := &chaosv1.Disruption{}

	err := r.markDisruptionFailed(context.Background(), disruption, r.Logger)

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

func TestMarkDisruptionCompleted(t *testing.T) {
	// Setup mock client (as before)
	r := &DisruptionReconciler{Client: newMockErrorClient(nil), Logger: logr.Discard()}
	disruption := &chaosv1.Disruption{}

	err := r.markDisruptionCompleted(context.Background(), disruption, r.Logger)

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

// // Test individual functions in isolation
// func TestValidatePodKill(t *testing.T) {
// 	panic("unimplemented")
// }

// func TestGetSafetyConfig(t *testing.T) {
// 	panic("unimplemented")
// }
