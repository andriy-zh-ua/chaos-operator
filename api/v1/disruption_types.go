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

	// Safety configuration defines constraints to limit disruption impact and prevent system-wide failures
	// +optional
	Safety *SafetyConfig `json:"safety,omitempty"`

	// PodKill defines the configuration for pod killing disruptions
	// +optional
	PodKill *PodKillSpec `json:"podKill,omitempty"`
}

type SafetyConfig struct {
	MaxDurationSeconds    int32 `json:"maxDurationSeconds,omitempty"`    // Maximum time disruption can run
	MaxPodsAffected       int32 `json:"maxPodsAffected,omitempty"`       // Maximum number of pods that can be affected
	MaxPercentageAffected int32 `json:"maxPercentageAffected,omitempty"` // Maximum percentage of pods that can be affected (0-100)
}

// PodKillSpec defines the configuration for pod killing disruptions
type PodKillSpec struct {
	// Selector defines the label selector to identify which pods to target
	// +optional
	Selector *metav1.LabelSelector `json:"selector,omitempty"`

	// Duration for which the disruption should last (e.g., 5m, 30s)
	// +optional
	Duration *metav1.Duration `json:"duration,omitempty"`

	// KillMode specifies how to select pods for killing
	// +kubebuilder:validation:Enum=random;all;fixed-count
	// +kubebuilder:default="random"
	KillMode string `json:"killMode"`

	// Count specifies the number of pods to kill (only used when KillMode is "fixed-count")
	// +kubebuilder:validation:Minimum=1
	// +optional
	Count int32 `json:"count,omitempty"`

	// GracePeriodSeconds specifies the grace period for pod before termination
	// +kubebuilder:validation:Minimum=0
	// +optional
	GracePeriodSeconds int64 `json:"gracePeriodSeconds,omitempty"`
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
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	Phase     string       `json:"phase"` // Pending, Running, Completed, Failed
	StartTime *metav1.Time `json:"startTime,omitempty"`
	EndTime   *metav1.Time `json:"endTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// Disruption is the Schema for the disruptions API
type Disruption struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Disruption
	// +required
	Spec DisruptionSpec `json:"spec"`

	// status defines the observed state of Disruption
	// +optional
	Status DisruptionStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// DisruptionList contains a list of Disruption
type DisruptionList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Disruption `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Disruption{}, &DisruptionList{})
}
