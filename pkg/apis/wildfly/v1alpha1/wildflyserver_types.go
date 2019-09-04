package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// WildFlyServerSpec defines the desired state of WildFlyServer
// +k8s:openapi-gen=true
type WildFlyServerSpec struct {
	// ApplicationImage is the name of the application image to be deployed
	ApplicationImage string `json:"applicationImage"`
	Size             int32  `json:"size"`
	//SessionAffinity defines if connections from the same client ip are passed to the same WildFlyServer instance/pod each time
	SessionAffinity     bool                     `json:"sessionAffinity,omitempty"` // TODO: how to use this?
	DisableHTTPRoute    bool                     `json:"disableHTTPRoute,omitempty"`
	StandaloneConfigMap *StandaloneConfigMapSpec `json:"standaloneConfigMap,omitempty"`
	Storage             *StorageSpec             `json:"storage,omitempty"`
	ServiceAccountName  string                   `json:"serviceAccountName,omitempty"`
	EnvFrom             []corev1.EnvFromSource   `json:"envFrom,omitempty"`
	// Env contains environment variables for the containers running the WildFlyServer application
	Env []corev1.EnvVar `json:"env,omitempty"`
	// JBossHome defines place where jboss distribution is located at the docker image defined by ApplicationImage
	JbossHome string `json:"jbossHome,omitempty"`
}

// StandaloneConfigMapSpec defines the desired configMap configuration to obtain the standalone configuration for WildFlyServer
// +k8s:openapi-gen=true
type StandaloneConfigMapSpec struct {
	Name string `json:"name"`
	Key  string `json:"key,omitempty"`
}

// StorageSpec defines the desired storage for WildFlyServer
// +k8s:openapi-gen=true
type StorageSpec struct {
	EmptyDir            *corev1.EmptyDirVolumeSource `json:"emptyDir,omitempty"`
	VolumeClaimTemplate corev1.PersistentVolumeClaim `json:"volumeClaimTemplate,omitempty"`
}

// WildFlyServerStatus defines the observed state of WildFlyServer
// +k8s:openapi-gen=true
type WildFlyServerStatus struct {
	Pods  []PodStatus `json:"pods,omitempty"`
	Hosts []string    `json:"hosts,omitempty"`
	// Represents the number of pods which are in scaledown process
	// what particular pod is scaling down can be verified by PodStatus
	//
	// Read-only.
	ScalingdownPods int32 `json:"scalingdownPods"`
}

const (
	// PodStateActive represents PodStatus.State when pod is active to serve requests
	// it's connected in the Service load balancer
	PodStateActive = "ACTIVE"
	// PodStateScalingDownRecoveryInvestigation represents the PodStatus.State when pod is not active to serve requests,
	// it's in state of scaling down and is to be verified if it's dirty and if recovery is needed
	// as the pod is under recovery verification it can't be immediatelly removed
	// and it needs to be wait until it's marked as clean to be removed
	PodStateScalingDownRecoveryInvestigation = "SCALING_DOWN_RECOVERY_INVESTIGATION"
	// PodStateScalingDownRecoveryInProgress represents the PodStatus.State when pod is not active to serve requests,
	// and it was marked as recovery is needed - there are some in-doubt transactions.
	// The app server was restarted with the recovery properties to speed-up recovery nad it's needed to wait
	// until all ind-doubt transactions are processed.
	PodStateScalingDownRecoveryInProgress = "SCALING_DOWN_RECOVERY_INPROGRESS"
	// PodStateScalingDownClean represents the PodStatus.State when pod is not active to serve requests
	// it's in state of scaling down and it's clean
	// 'clean' means it's ready to be removed from the kubernetes cluster
	PodStateScalingDownClean = "SCALING_DOWN_CLEAN"
)

// PodStatus defines the observed state of pods running the WildFlyServer application
// +k8s:openapi-gen=true
type PodStatus struct {
	Name  string `json:"name"`
	PodIP string `json:"podIP"`
	// Represent the state of the Pod, it's used especially during scale down
	// the expected values are represented by the PodState* constants
	//
	// Read-only.
	State string `json:"state"`
}

// WildFlyServer is the Schema for the wildflyservers API
// +k8s:openapi-gen=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:subresource:status
type WildFlyServer struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   WildFlyServerSpec   `json:"spec,omitempty"`
	Status WildFlyServerStatus `json:"status,omitempty"`
}

// WildFlyServerList contains a list of WildFlyServer
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type WildFlyServerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []WildFlyServer `json:"items"`
}

func init() {
	SchemeBuilder.Register(&WildFlyServer{}, &WildFlyServerList{})
}
