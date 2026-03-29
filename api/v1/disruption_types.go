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

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// DisruptionSpec defines the desired state of Disruption
type DisruptionSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	// The following markers will use OpenAPI v3 schema to validate the value
	// More info: https://book.kubebuilder.io/reference/markers/crd-validation.html

	// Safety configuration to prevent excessive disruption
	// +optional
	Safety *SafetyConfig `json:"safety,omitempty" yaml:"safety,omitempty"`

	// PodKill configuration for chaos experiments
	// +optional
	PodKill *PodKillSpec `json:"podKill,omitempty" yaml:"podKill,omitempty"`
}

// SafetyConfig defines safety limits for chaos experiments
type SafetyConfig struct {
	MaxDurationSeconds    int32 `json:"maxDurationSeconds,omitempty" yaml:"maxDurationSeconds,omitempty"`       // Maximum time disruption can run
	MaxPodsAffected       int32 `json:"maxPodsAffected,omitempty" yaml:"maxPodsAffected,omitempty"`             // Maximum number of pods that can be affected
	MaxPercentageAffected int32 `json:"maxPercentageAffected,omitempty" yaml:"maxPercentageAffected,omitempty"` // Maximum percentage of pods that can be affected (0-100)
}

// DisruptionScope defines the scope of the disruption
type DisruptionScope string

const (
	DisruptionScopeNamespace DisruptionScope = "namespace"
	DisruptionScopeCluster   DisruptionScope = "cluster"
)

// PodKillSpec defines the configuration for pod killing disruptions
type PodKillSpec struct {
	// Scope defines the scope of the disruption (namespace or cluster)
	// +kubebuilder:default="namespace"
	Scope DisruptionScope `json:"scope" yaml:"scope"`

	// Namespaces is only respected when Scope=cluster.
	// If empty, targets all non-system namespaces.
	// +optional
	Namespaces []string `json:"namespaces,omitempty" yaml:"namespaces,omitempty"`

	// Selector defines the label selector to identify which pods to target
	// +optional
	Selector *metav1.LabelSelector `json:"selector,omitempty" yaml:"selector,omitempty"`

	// Duration for which the disruption should last (e.g., 5m, 30s)
	// +optional
	Duration *metav1.Duration `json:"duration,omitempty" yaml:"duration,omitempty"`

	// KillMode specifies how to select pods for killing
	// +kubebuilder:validation:Enum=random;all;fixed-count
	// +kubebuilder:default="random"
	KillMode string `json:"killMode" yaml:"killMode"`

	// Count is only respected when KillMode=fixed-count.
	// Count specifies the number of pods to kill.
	// +kubebuilder:validation:Minimum=1
	// +optional
	Count int32 `json:"count,omitempty" yaml:"count,omitempty"`

	// GracePeriodSeconds specifies the grace period for pod before termination
	// (OpenAPI validation rule rejects any value less than 0)
	// +kubebuilder:validation:Minimum=0
	// +optional
	GracePeriodSeconds *int64 `json:"gracePeriodSeconds,omitempty" yaml:"gracePeriodSeconds,omitempty"`
}

// DisruptionStatus defines the observed state of Disruption.
type DisruptionStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// For Kubernetes API conventions, see:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#typical-status-properties

	// conditions represent the current state of the Disruption resource.
	// Each condition has a unique type and reflects the status of a specific aspect of the resource.
	//
	// Standard condition types include:
	// - "Available": the resource is fully functional
	// - "Progressing": the resource is being created or updated
	// - "Degraded": the resource failed to reach or maintain its desired state
	//
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty" yaml:"conditions,omitempty"`

	// Phase represents the current phase of the disruption
	Phase string `json:"phase,omitempty" yaml:"phase,omitempty"` // Pending, Running, Completed, Failed

	// StartTime is when the disruption started execution
	StartTime *metav1.Time `json:"startTime,omitempty" yaml:"startTime,omitempty"`

	// EndTime is when the disruption completed or failed
	EndTime *metav1.Time `json:"endTime,omitempty" yaml:"endTime,omitempty"`

	// PodsAffected is the number of pods affected by the disruption
	PodsAffected int32 `json:"podsAffected,omitempty" yaml:"podsAffected,omitempty"`

	// LastExecution is the timestamp of the last execution attempt
	LastExecution *metav1.Time `json:"lastExecution,omitempty" yaml:"lastExecution,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// Disruption is the Schema for the disruptions API
type Disruption struct {
	metav1.TypeMeta `json:",inline" yaml:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" yaml:"metadata,omitempty"`

	// spec defines the desired state of Disruption
	// +required
	Spec DisruptionSpec `json:"spec" yaml:"spec"`

	// status defines the observed state of Disruption
	// +optional
	Status DisruptionStatus `json:"status,omitempty" yaml:"status,omitempty"`
}

// +kubebuilder:object:root=true

// DisruptionList contains a list of Disruption
type DisruptionList struct {
	metav1.TypeMeta `json:",inline" yaml:",inline"`
	metav1.ListMeta `json:"metadata,omitempty" yaml:"metadata,omitempty"`
	Items           []Disruption `json:"items" yaml:"items"`
}

func init() {
	SchemeBuilder.Register(&Disruption{}, &DisruptionList{})
}
