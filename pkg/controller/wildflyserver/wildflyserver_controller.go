package wildflyserver

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/go-logr/logr"
	wildflyv1alpha1 "github.com/wildfly/wildfly-operator/pkg/apis/wildfly/v1alpha1"
	wildflyutil "github.com/wildfly/wildfly-operator/pkg/controller/util"

	routev1 "github.com/openshift/api/route/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	logf "sigs.k8s.io/controller-runtime/pkg/runtime/log"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var log = logf.Log.WithName("controller_wildflyserver")

const (
	httpApplicationPort            int32 = 8080
	httpManagementPort             int32 = 9990
	defaultRecoveryPort            int32 = 4712
	wildflyServerFinalizer               = "finalizer.wildfly.org"
	wildflyHome                          = "/wildfly"
	standaloneServerDataDirSuffix        = "/standalone/data"                // data directory where runtime data is saved
	wftcDataDirName                      = "ejb-xa-recovery"                 // data directory where WFTC stores transaction runtime data
	labelMarkerOperatedByService         = "wildfly.org/operated-by-service" // label used to remove a pod from receiving load from service during transaction recovery
	labelMarkerScaleDownProcessing       = "under-scale-down-processing"     // label value for pod that's in scaledown process
	txnRecoveryScanCommand               = "SCAN"                            // Narayana socket command to force recovery
	reloadRetryCount                     = 10                                // number of retries when waiting for container reload is done
)

var (
	recoveryErrorRegExp = regexp.MustCompile("ERROR.*Periodic Recovery")
)

// Add creates a new WildFlyServer Controller and adds it to the Manager. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	return add(mgr, newReconciler(mgr))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager) reconcile.Reconciler {
	return &ReconcileWildFlyServer{client: mgr.GetClient(), scheme: mgr.GetScheme()}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New("wildflyserver-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to primary resource WildFlyServer
	err = c.Watch(&source.Kind{Type: &wildflyv1alpha1.WildFlyServer{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// Watch for changes to secondary resources and requeue the owner WildFlyServer
	enqueueRequestForOwner := handler.EnqueueRequestForOwner{
		IsController: true,
		OwnerType:    &wildflyv1alpha1.WildFlyServer{},
	}
	for _, obj := range [3]runtime.Object{&appsv1.StatefulSet{}, &corev1.Service{}, &routev1.Route{}} {
		if err = c.Watch(&source.Kind{Type: obj}, &enqueueRequestForOwner); err != nil {
			return err
		}
	}
	return nil
}

var _ reconcile.Reconciler = &ReconcileWildFlyServer{}

// ReconcileWildFlyServer reconciles a WildFlyServer object
type ReconcileWildFlyServer struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	client client.Client
	scheme *runtime.Scheme
}

// Reconcile reads that state of the cluster for a WildFlyServer object and makes changes based on the state read
// and what is in the WildFlyServer.Spec
// TODO(user): Modify this Reconcile function to implement your Controller logic.  This example creates
// a Pod as an example
// Note:
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (r *ReconcileWildFlyServer) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	reqLogger := log.WithValues("Request.Namespace", request.Namespace, "Request.Name", request.Name)
	reqLogger.Info("Reconciling WildFlyServer")

	// Fetch the WildFlyServer instance
	wildflyServer := &wildflyv1alpha1.WildFlyServer{}
	err := r.client.Get(context.TODO(), request.NamespacedName, wildflyServer)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}

	// Check if the statefulSet already exists, if not create a new one
	foundStatefulSet := &appsv1.StatefulSet{}
	err = r.client.Get(context.TODO(), types.NamespacedName{Name: wildflyServer.Name, Namespace: wildflyServer.Namespace}, foundStatefulSet)
	if err != nil && errors.IsNotFound(err) {
		// Define a new statefulSet
		statefulSet := r.statefulSetForWildFly(wildflyServer)
		reqLogger.Info("Creating a new StatefulSet.", "StatefulSet.Namespace", statefulSet.Namespace, "StatefulSet.Name", statefulSet.Name)
		err = r.client.Create(context.TODO(), statefulSet)
		if err != nil {
			reqLogger.Error(err, "Failed to create new StatefulSet.", "StatefulSet.Namespace", statefulSet.Namespace, "StatefulSet.Name", statefulSet.Name)
			return reconcile.Result{}, err
		}
		// StatefulSet created successfully - return and requeue
		return reconcile.Result{Requeue: true}, nil
	} else if err != nil {
		reqLogger.Error(err, "Failed to get StatefulSet.")
		return reconcile.Result{}, err
	}

	// Check if the WildFly server instance is marked to be deleted, which is
	// indicated by the deletion timestamp being set.
	isWildflyServerMarkedToBeDeleted := wildflyServer.GetDeletionTimestamp() != nil
	if isWildflyServerMarkedToBeDeleted {
		if wildflyutil.ContainsInList(wildflyServer.GetFinalizers(), wildflyServerFinalizer) {
			// Run finalization logic for WildflyServer. If fails do not remove.
			//   this will be retried at next reconciliation.
			if err := r.finalizeWildflyServer(reqLogger, wildflyServer); err != nil {
				return reconcile.Result{}, err
			}

			// Remove WildflyServer. Once all finalizers have been removed, the object will be deleted.
			wildflyServer.SetFinalizers(wildflyutil.RemoveFromList(wildflyServer.GetFinalizers(), wildflyServerFinalizer))
			err := r.client.Update(context.TODO(), wildflyServer)
			if err != nil {
				return reconcile.Result{}, err
			}
		}
		return reconcile.Result{}, nil
	}
	// Add finalizer for this CR
	if !wildflyutil.ContainsInList(wildflyServer.GetFinalizers(), wildflyServerFinalizer) {
		if err := r.addWildflyServerFinalizer(reqLogger, wildflyServer); err != nil {
			return reconcile.Result{}, err
		}
	}

	// Checking if  WildFlyServerSpec is about to be scaled down
	podList, err := r.getPodsForWildFly(wildflyServer)
	if err != nil {
		reqLogger.Error(err, "Failed to list pods.", "WildFlyServer.Namespace", wildflyServer.Namespace, "WildFlyServer.Name", wildflyServer.Name)
		return reconcile.Result{}, err
	}
	wildflyServerSpecSize := wildflyServer.Spec.Size
	wildflyServerNumberOfPods := len(podList.Items)

	// We need to ensure that the pod was not deleted by user during the time the scale down is in progress
	//   when the WildFly spec size and the Stateful replica size is in sync but the number of pods does not match
	//   we need to wait for StatefulSet to make the counts in sync
	if *foundStatefulSet.Spec.Replicas == wildflyServerSpecSize && int32(wildflyServerNumberOfPods) != wildflyServerSpecSize {
		reqLogger.Info("Number of pods does not match the WildFly server specification. Waiting to get numbers in sync.",
			"WildflyServer specification", wildflyServer.Name, "Expected number of pods", wildflyServerSpecSize, "Current number of existing pods", wildflyServerNumberOfPods)
		return reconcile.Result{}, nil
	}

	numberOfPodsToScaleDown := wildflyServerNumberOfPods - int(wildflyServerSpecSize) // difference between desired pod count and the current number of pods
	wasRecoverySuccesful, err := r.processTransactionRecoveryScaleDown(reqLogger, wildflyServer, numberOfPodsToScaleDown, podList)
	if err != nil {
		// error during processing transaction recovery, requeue
		return reconcile.Result{}, err
	} else if !wasRecoverySuccesful {
		// recovery processed without failures but there are some unfinished transaction in pods, need to repeat processing, requeue
		return reconcile.Result{}, nil
	}

	// Check if the stateful set is up to date with the WildFlyServerSpec
	if checkUpdate(&wildflyServer.Spec, foundStatefulSet) {
		err = r.client.Update(context.TODO(), foundStatefulSet)
		if err != nil {
			reqLogger.Error(err, "Failed to update StatefulSet.", "StatefulSet.Namespace", foundStatefulSet.Namespace, "StatefulSet.Name", foundStatefulSet.Name)
			return reconcile.Result{}, err
		}

		// Spec updated - return and requeue
		return reconcile.Result{Requeue: true}, nil
	}

	// Check if the loadbalancer already exists, if not create a new one
	foundLoadBalancer := &corev1.Service{}
	loadBalancerName := loadBalancerServiceName(wildflyServer)
	err = r.client.Get(context.TODO(), types.NamespacedName{Name: loadBalancerName, Namespace: wildflyServer.Namespace}, foundLoadBalancer)
	if err != nil && errors.IsNotFound(err) {
		// Define a new loadbalancer
		loadBalancer := r.loadBalancerForWildFly(wildflyServer)
		reqLogger.Info("Creating a new LoadBalancer.", "LoadBalancer.Namespace", loadBalancer.Namespace, "LoadBalancer.Name", loadBalancer.Name)
		err = r.client.Create(context.TODO(), loadBalancer)
		if err != nil {
			reqLogger.Error(err, "Failed to create new LoadBalancer.", "LoadBalancer.Namespace", loadBalancer.Namespace, "LoadBalancer.Name", loadBalancer.Name)
			return reconcile.Result{}, err
		}
		// loadbalancer created successfully - return and requeue
		return reconcile.Result{Requeue: true}, nil
	} else if err != nil {
		reqLogger.Error(err, "Failed to get LoadBalancer.")
		return reconcile.Result{}, err
	}

	// Check if the HTTP route must be created.
	foundRoute := &routev1.Route{}
	if !wildflyServer.Spec.DisableHTTPRoute {
		err = r.client.Get(context.TODO(), types.NamespacedName{Name: wildflyServer.Name, Namespace: wildflyServer.Namespace}, foundRoute)
		if err != nil && errors.IsNotFound(err) {
			// Define a new Route
			route := r.routeForWildFly(wildflyServer)
			reqLogger.Info("Creating a new Route.", "Route.Namespace", route.Namespace, "Route.Name", route.Name)
			err = r.client.Create(context.TODO(), route)
			if err != nil {
				reqLogger.Error(err, "Failed to create new Route.", "Route.Namespace", route.Namespace, "Route.Name", route.Name)
				return reconcile.Result{}, err
			}
			// Route created successfully - return and requeue
			return reconcile.Result{Requeue: true}, nil
		} else if err != nil && errorIsNoMatchesForKind(err, "Route", "route.openshift.io/v1") {
			// if the operator runs on k8s, Route resource does not exist and the route creation must be skipped.
			reqLogger.Info("Routes are not supported, skip creation of the HTTP route")
			wildflyServer.Spec.DisableHTTPRoute = true
			if err = r.client.Update(context.TODO(), wildflyServer); err != nil {
				reqLogger.Error(err, "Failed to update WildFlyServerSpec to disable HTTP Route.", "WildFlyServer.Namespace", wildflyServer.Namespace, "WildFlyServer.Name", wildflyServer.Name)
				return reconcile.Result{}, err
			}
			return reconcile.Result{Requeue: true}, nil
		} else if err != nil {
			reqLogger.Error(err, "Failed to get Route.")
			return reconcile.Result{}, err
		}
	}

	// Requeue until the pod list matches the spec's size
	if len(podList.Items) != int(wildflyServerSpecSize) {
		reqLogger.Info("Number of pods does not match the desired size", "PodList.Size", len(podList.Items), "Size", wildflyServerSpecSize)
		return reconcile.Result{Requeue: true}, nil
	}

	// Update WildFly Server host status
	update := false
	if !wildflyServer.Spec.DisableHTTPRoute {
		hosts := make([]string, len(foundRoute.Status.Ingress))
		for i, ingress := range foundRoute.Status.Ingress {
			hosts[i] = ingress.Host
		}
		if !reflect.DeepEqual(hosts, wildflyServer.Status.Hosts) {
			update = true
			wildflyServer.Status.Hosts = hosts
			reqLogger.Info("Updating hosts", "WildFlyServer", wildflyServer)
		}
	}

	// Update Wildfly Server scale down processing info
	if wildflyServer.Status.ScalingdownPods != int32(numberOfPodsToScaleDown) {
		update = true
		wildflyServer.Status.ScalingdownPods = int32(numberOfPodsToScaleDown)
	}

	// Update WildFly Server pod status
	requeue, podsStatus := getPodStatus(podList.Items, wildflyServer.Status.Pods)
	if !reflect.DeepEqual(podsStatus, wildflyServer.Status.Pods) {
		update = true
		wildflyServer.Status.Pods = podsStatus
	}

	if update {
		if err := r.client.Status().Update(context.Background(), wildflyServer); err != nil {
			reqLogger.Error(err, "Failed to update pods in WildFlyServer status.")
			return reconcile.Result{}, err
		}
	}
	if requeue {
		return reconcile.Result{Requeue: true}, nil
	}

	return reconcile.Result{}, nil
}

// listing pods which belongs to the WildFly server
//   the pods are differentiated based on the selectors
func (r *ReconcileWildFlyServer) getPodsForWildFly(w *wildflyv1alpha1.WildFlyServer) (*corev1.PodList, error) {
	podList := &corev1.PodList{}
	labelSelector := labels.SelectorFromSet(labelsForWildFly(w))
	listOps := &client.ListOptions{
		Namespace:     w.Namespace,
		LabelSelector: labelSelector,
	}
	err := r.client.List(context.TODO(), listOps, podList)

	// sorting pods by number in the name
	if err == nil {
		pattern := regexp.MustCompile(`[0-9]+$`)
		sort.SliceStable(podList.Items, func(i, j int) bool {
			reOut1 := pattern.FindStringSubmatch(podList.Items[i].ObjectMeta.Name)
			if reOut1 == nil {
				return false
			}
			number1, err := strconv.Atoi(reOut1[0])
			if err != nil {
				return false
			}
			reOut2 := pattern.FindStringSubmatch(podList.Items[j].ObjectMeta.Name)
			if reOut2 == nil {
				return false
			}
			number2, err := strconv.Atoi(reOut2[0])
			if err != nil {
				return false
			}

			return number1 < number2
		})
	}
	return podList, err
}

// statefulSetForWildFly returns a wildfly StatefulSet object
func (r *ReconcileWildFlyServer) statefulSetForWildFly(w *wildflyv1alpha1.WildFlyServer) *appsv1.StatefulSet {
	replicas := w.Spec.Size
	applicationImage := w.Spec.ApplicationImage
	volumeName := w.Name + "-volume"
	labesForActiveWildflyPod := labelsForWildFly(w)
	labesForActiveWildflyPod[labelMarkerOperatedByService] = "active"

	statefulSet := &appsv1.StatefulSet{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "apps/v1",
			Kind:       "StatefulSet",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      w.Name,
			Namespace: w.Namespace,
			Labels:    labelsForWildFly(w),
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:            &replicas,
			ServiceName:         loadBalancerServiceName(w),
			PodManagementPolicy: appsv1.ParallelPodManagement,
			Selector: &metav1.LabelSelector{
				MatchLabels: labelsForWildFly(w),
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labesForActiveWildflyPod,
				},
				Spec: corev1.PodSpec{
					// TODO: provide this to s2i scripts before server startup
					//       operator should consider changing the recovery properties of narayana backoff period
					/*InitContainers: []corev1.Container{{
						Name:  "app-server-init",
						Image: "busybox",
						Command: []string{"sh", "-c", fmt.Sprintf(`
						    if [ -e /etc/podinfo/annotation.scale_down ]; then
								echo JAVA_OPTS='$JAVA_OPTS -Dcom.arjuna.ats.arjuna.common.RecoveryEnvironmentBean.recoveryBackoffPeriod=1 -Dcom.arjuna.ats.arjuna.common.RecoveryEnvironmentBean.periodicRecoveryPeriod=1 -Dcom.arjuna.ats.jta.common.JTAEnvironmentBean.orphanSafetyInterval=1' >> %s/bin/standalone.conf
							fi`, getJbossHome()),
						},
						VolumeMounts: []corev1.VolumeMount{{
							Name:      "podinfo",
							MountPath: "/etc/podinfo",
							ReadOnly:  false,
						}},
					}},*/
					Containers: []corev1.Container{{
						Name:  w.Name,
						Image: applicationImage,
						Ports: []corev1.ContainerPort{
							{
								ContainerPort: httpApplicationPort,
								Name:          "http",
							},
							{
								ContainerPort: httpManagementPort,
								Name:          "admin",
							},
						},
						LivenessProbe: createLivenessProbe(),
						// Readiness Probe is options
						ReadinessProbe: createReadinessProbe(),
						VolumeMounts: []corev1.VolumeMount{{
							Name:      volumeName,
							MountPath: getJbossHome() + standaloneServerDataDirSuffix,
						}, {
							Name:      "podinfo",
							MountPath: "/etc/podinfo",
							ReadOnly:  false,
						}},
						// TODO the KUBERNETES_NAMESPACE and KUBERNETES_LABELS env should only be set if
						// the application uses clustering and KUBE_PING.
						Env: []corev1.EnvVar{
							{
								Name: "KUBERNETES_NAMESPACE",
								ValueFrom: &corev1.EnvVarSource{
									FieldRef: &corev1.ObjectFieldSelector{
										FieldPath: "metadata.namespace",
									},
								},
							},
							{
								Name:  "KUBERNETES_LABELS",
								Value: labels.SelectorFromSet(labesForActiveWildflyPod).String(),
							},
						},
					}},
					ServiceAccountName: w.Spec.ServiceAccountName,
					Volumes: []v1.Volume{{
						Name: "podinfo",
						VolumeSource: v1.VolumeSource{
							DownwardAPI: &v1.DownwardAPIVolumeSource{
								Items: []v1.DownwardAPIVolumeFile{{
									Path: "annotation.scale_down",
									FieldRef: &v1.ObjectFieldSelector{
										FieldPath: "metadata.annotations['" + labelMarkerScaleDownProcessing + "']",
									},
								}},
							},
						},
					}},
				},
			},
		},
	}

	if len(w.Spec.EnvFrom) > 0 {
		statefulSet.Spec.Template.Spec.Containers[0].EnvFrom = append(statefulSet.Spec.Template.Spec.Containers[0].EnvFrom, w.Spec.EnvFrom...)
	}

	if len(w.Spec.Env) > 0 {
		statefulSet.Spec.Template.Spec.Containers[0].Env = append(statefulSet.Spec.Template.Spec.Containers[0].Env, w.Spec.Env...)
	}

	storageSpec := w.Spec.Storage

	if storageSpec == nil {
		statefulSet.Spec.Template.Spec.Volumes = append(statefulSet.Spec.Template.Spec.Volumes, corev1.Volume{
			Name: volumeName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		})
	} else if storageSpec.EmptyDir != nil {
		emptyDir := storageSpec.EmptyDir
		statefulSet.Spec.Template.Spec.Volumes = append(statefulSet.Spec.Template.Spec.Volumes, corev1.Volume{
			Name: volumeName,
			VolumeSource: v1.VolumeSource{
				EmptyDir: emptyDir,
			},
		})
	} else {
		pvcTemplate := storageSpec.VolumeClaimTemplate
		if pvcTemplate.Name == "" {
			pvcTemplate.Name = volumeName
		}
		pvcTemplate.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
		pvcTemplate.Spec.Resources = storageSpec.VolumeClaimTemplate.Spec.Resources
		pvcTemplate.Spec.Selector = storageSpec.VolumeClaimTemplate.Spec.Selector
		statefulSet.Spec.VolumeClaimTemplates = append(statefulSet.Spec.VolumeClaimTemplates, pvcTemplate)
	}

	standaloneConfigMap := w.Spec.StandaloneConfigMap
	if standaloneConfigMap != nil {
		configMapName := standaloneConfigMap.Name
		configMapKey := standaloneConfigMap.Key
		if configMapKey == "" {
			configMapKey = "standalone.xml"
		}
		log.Info("Reading standalone configuration from configmap", "StandaloneConfigMap.Name", configMapName, "StandaloneConfigMap.Key", configMapKey)

		statefulSet.Spec.Template.Spec.Volumes = append(statefulSet.Spec.Template.Spec.Volumes, corev1.Volume{
			Name: "standalone-config-volume",
			VolumeSource: v1.VolumeSource{
				ConfigMap: &v1.ConfigMapVolumeSource{
					LocalObjectReference: v1.LocalObjectReference{
						Name: configMapName,
					},
					Items: []v1.KeyToPath{
						{
							Key:  configMapKey,
							Path: "standalone.xml",
						},
					},
				},
			},
		})
		statefulSet.Spec.Template.Spec.Containers[0].VolumeMounts = append(statefulSet.Spec.Template.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{
			Name:      "standalone-config-volume",
			MountPath: getJbossHome() + "/standalone/configuration/standalone.xml",
			SubPath:   "standalone.xml",
		})
	}

	// Set WildFlyServer instance as the owner and controller
	controllerutil.SetControllerReference(w, statefulSet, r.scheme)
	return statefulSet
}

// loadBalancerForWildFly returns a loadBalancer service
func (r *ReconcileWildFlyServer) loadBalancerForWildFly(w *wildflyv1alpha1.WildFlyServer) *corev1.Service {
	labels := labelsForWildFly(w)
	labels[labelMarkerOperatedByService] = "active" // managing only active pods, ones which are not in scaledown process
	loadBalancer := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      loadBalancerServiceName(w),
			Namespace: w.Namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: labels,
			// SessionAffinity: sessionAffinity,
			ClusterIP: corev1.ClusterIPNone,
			Ports: []corev1.ServicePort{
				{
					Name: "http",
					Port: httpApplicationPort,
				},
			},
		},
	}
	// Set WildFlyServer instance as the owner and controller
	controllerutil.SetControllerReference(w, loadBalancer, r.scheme)
	return loadBalancer
}

func (r *ReconcileWildFlyServer) routeForWildFly(w *wildflyv1alpha1.WildFlyServer) *routev1.Route {
	weight := int32(100)

	route := &routev1.Route{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "route.openshift.io/v1",
			Kind:       "Route",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      w.Name,
			Namespace: w.Namespace,
			Labels:    labelsForWildFly(w),
		},
		Spec: routev1.RouteSpec{
			To: routev1.RouteTargetReference{
				Kind:   "Service",
				Name:   loadBalancerServiceName(w),
				Weight: &weight,
			},
			Port: &routev1.RoutePort{
				TargetPort: intstr.FromString("http"),
			},
		},
	}
	// Set WildFlyServer instance as the owner and controller
	controllerutil.SetControllerReference(w, route, r.scheme)

	return route
}

func (r *ReconcileWildFlyServer) finalizeWildflyServer(reqLogger logr.Logger, w *wildflyv1alpha1.WildFlyServer) error {
	podList, err := r.getPodsForWildFly(w)
	if err != nil {
		return fmt.Errorf("Finalizer processing: failed to list pods for WildflyServer %v:%v name Error: %v", w.Namespace, w.Name, err)
	}
	wasRecoverySuccesful, err := r.processTransactionRecoveryScaleDown(reqLogger, w, len(podList.Items), podList)
	if err != nil {
		// error during processing transaction recovery, error from finalizer
		return fmt.Errorf("Finalizer processing: failed transaction recovery for WildflyServer %v:%v name Error: %v", w.Namespace, w.Name, err)
	} else if !wasRecoverySuccesful {
		// recovery processed without failures but there are some unfinished transaction, error from finalizer
		return fmt.Errorf("Finalizer processing: transaction recovery processed with unfinished transactions: WildflyServer %v:%v", w.Namespace, w.Name)
	}
	reqLogger.Info("Finalizer finished succesfully", "WildflyServer Namespace", w.Namespace, "WildflyServer Name", w.Name)
	return nil
}

func (r *ReconcileWildFlyServer) addWildflyServerFinalizer(reqLogger logr.Logger, w *wildflyv1alpha1.WildFlyServer) error {
	reqLogger.Info("Adding Finalizer for the WildflyServer")
	w.SetFinalizers(append(w.GetFinalizers(), wildflyServerFinalizer))

	// Update CR WildflyServer
	err := r.client.Update(context.TODO(), w)
	if err != nil {
		reqLogger.Error(err, "Failed to update WildflyServer with finalizer")
		return err
	}
	return nil
}

// processTransactionRecoveryScaleDown runs transaction recovery on provided number of pods
//   it returns true if all processing finished with success, then error is nil
//   it retunrs false if there is some unfinished transactions in pod or an error occured
func (r *ReconcileWildFlyServer) processTransactionRecoveryScaleDown(reqLogger logr.Logger, w *wildflyv1alpha1.WildFlyServer, numberOfPodsToScaleDown int, podList *corev1.PodList) (bool, error) {
	wildflyServerNumberOfPods := len(podList.Items)
	scaleDownPodsState := make(map[string]string) // map referring to: pod name - pod state
	scaleDownErrors := []error{}                  // errors occured during processing the scaledown for the pods
	for scaleDownIndex := 1; scaleDownIndex <= numberOfPodsToScaleDown; scaleDownIndex++ {
		// Scaledown scenario, need to handle transction recovery
		scaleDownPod := podList.Items[wildflyServerNumberOfPods-scaleDownIndex]
		scaleDownPodName := scaleDownPod.ObjectMeta.Name
		scaleDownPodIP := scaleDownPod.Status.PodIP
		if strings.Contains(scaleDownPodIP, ":") && !strings.HasPrefix(scaleDownPodIP, "[") {
			scaleDownPodIP = "[" + scaleDownPodIP + "]" // for IPv6
		}

		// Setting-up the pod status - status is used to decide if the pod could be scaled (aka. removed from the statefulset)
		wildflyServerSpecPodStatus := getWildflyServerPodStatusByName(w, scaleDownPodName)
		if wildflyServerSpecPodStatus == nil {
			scaleDownErrors = append(scaleDownErrors,
				fmt.Errorf("Cannot find pod name '%v' in the list of the active pods for the WildflyServer operator: %v", scaleDownPodName, w))
			break
		}
		scaleDownPodsState[scaleDownPodName] = wildflyServerSpecPodStatus.State
		if wildflyServerSpecPodStatus.State != wildflyv1alpha1.PodStateScalingDownClean {
			// When state is not already processed with recovery and all data was cleaned then we mark pod to be dirty
			scaleDownPodsState[scaleDownPodName] = wildflyv1alpha1.PodStateScalingDownDirty
		}

		if wildflyServerSpecPodStatus.State != wildflyv1alpha1.PodStateScalingDownClean {
			// Removing the pod from the Service handling and deleting it to be booted-up with recovery configuration
			if scaleDownPod.ObjectMeta.Labels[labelMarkerOperatedByService] != labelMarkerScaleDownProcessing &&
				scaleDownPod.Annotations[labelMarkerScaleDownProcessing] == "" {

				scaleDownPod.ObjectMeta.Labels[labelMarkerOperatedByService] = labelMarkerScaleDownProcessing
				scaleDownPod.Annotations[labelMarkerOperatedByService] = labelMarkerScaleDownProcessing
				if err := r.client.Update(context.TODO(), &scaleDownPod); err != nil {
					scaleDownErrors = append(scaleDownErrors,
						fmt.Errorf("Failed to update pod labels, pod name %v, labels %v, error: %v", scaleDownPodName, scaleDownPod.ObjectMeta.Labels, err))
					break
				}
				// deleting pod, the purpose of the pod deletion is that pod will start with properties fitting faster recovery
				//   if this fails we don't care too much as it means only that the recovery can take a bit more time
				if err := wildflyutil.DeletePod(&scaleDownPod); err != nil {
					reqLogger.Info("Not possible to delete pod", "Pod Name", scaleDownPodName, "Error", err)
				}
				break
			}

			reqLogger.Info("Going for scaledown with pod. Processing transaction recovery consistency checks",
				"Pod Name", scaleDownPodName, "IP Address", scaleDownPodIP)

			success, message, err := checkRecovery(reqLogger, &scaleDownPod)
			if err != nil {
				scaleDownErrors = append(scaleDownErrors, err)
				break
			}
			if success {
				// Recovery was processed with success, the pod is clean to go
				scaleDownPodsState[scaleDownPodName] = wildflyv1alpha1.PodStateScalingDownClean
			} else if message != "" {
				// some in-doubt transaction left in store, the pod is still dirty
				reqLogger.Info(message)
				scaleDownPodsState[scaleDownPodName] = wildflyv1alpha1.PodStateScalingDownDirty
			}
		}
	}
	// Updating the pod state based on the recovery processing when a scale down is in progress
	if numberOfPodsToScaleDown > 0 {
		for podsIndex, v := range w.Status.Pods {
			if podStateValue, exist := scaleDownPodsState[v.Name]; exist {
				w.Status.Pods[podsIndex].State = podStateValue
			}
		}
		w.Status.ScalingdownPods = int32(numberOfPodsToScaleDown)
		if err := r.client.Status().Update(context.Background(), w); err != nil && !strings.Contains(err.Error(), "object has been modified") {
			reqLogger.Error(err, "Failed to update pods in WildFlyServer status during transaction recovery scale down processing")
		}
	}
	// Error happened during recovery processing, report and requeue
	if len(scaleDownErrors) > 0 {
		var errStrings = ""
		for _, v := range scaleDownErrors {
			errStrings += " [[" + v.Error() + "]],"
		}
		return false, fmt.Errorf("Found %v errors:\n%s", len(scaleDownErrors), errStrings)
	}
	// Transaction scale down processing did not recover all in-doubt transactions, requeue
	if wildflyutil.ContainsInMap(scaleDownPodsState, wildflyv1alpha1.PodStateScalingDownDirty) {
		reqLogger.Info("Some pods are in process of scale down. Some of them contain unfinished transactions. Retrying process of recovery.",
			"Pods in scale down process with state", scaleDownPodsState)
		return false, nil
	}
	return true, nil
}

func getJbossHome() string {
	jbossHome, wasFound := os.LookupEnv("JBOSS_HOME")
	if !wasFound {
		jbossHome = wildflyHome
	}
	return jbossHome
}

// check if the statefulset resource is up to date with the WildFlyServerSpec
func checkUpdate(spec *wildflyv1alpha1.WildFlyServerSpec, statefuleSet *appsv1.StatefulSet) bool {
	var update bool
	// Ensure the application image is up to date
	applicationImage := spec.ApplicationImage
	if statefuleSet.Spec.Template.Spec.Containers[0].Image != applicationImage {
		log.Info("Updating application image to "+applicationImage, "StatefulSet.Namespace", statefuleSet.Namespace, "StatefulSet.Name", statefuleSet.Name)
		statefuleSet.Spec.Template.Spec.Containers[0].Image = applicationImage
		update = true
	}
	// Ensure the statefulset replicas is up to date
	size := spec.Size
	if *statefuleSet.Spec.Replicas != size {
		log.Info("Updating replica size to "+strconv.Itoa(int(size)), "StatefulSet.Namespace", statefuleSet.Namespace, "StatefulSet.Name", statefuleSet.Name)
		statefuleSet.Spec.Replicas = &size
		update = true
	}
	// Ensure the env variables are up to date
	for _, env := range spec.Env {
		if !matches(&statefuleSet.Spec.Template.Spec.Containers[0], env) {
			log.Info("Updated statefulset env", "StatefulSet.Namespace", statefuleSet.Namespace, "StatefulSet.Name", statefuleSet.Name, "Env", env)
			update = true
		}
	}
	// Ensure the envFrom variables are up to date
	envFrom := spec.EnvFrom
	if !reflect.DeepEqual(statefuleSet.Spec.Template.Spec.Containers[0].EnvFrom, envFrom) {
		log.Info("Updating envFrom", "StatefulSet.Namespace", statefuleSet.Namespace, "StatefulSet.Name", statefuleSet.Name)
		statefuleSet.Spec.Template.Spec.Containers[0].EnvFrom = envFrom
		update = true
	}

	return update
}

func checkRecovery(reqLogger logr.Logger, scaleDownPod *corev1.Pod) (bool, string, error) {
	scaleDownPodName := scaleDownPod.ObjectMeta.Name
	scaleDownPodIP := scaleDownPod.Status.PodIP
	jbossHome := getJbossHome()

	// Reading timestamp for the latest log record
	scaleDownPodLogTimestampAtStart, err := wildflyutil.ObtainLogLatestTimestamp(scaleDownPod)
	if err != nil {
		return false, "", fmt.Errorf("Failed to read log from scaling down pod '%v', error: %v", scaleDownPodName, err)
	}

	// TODO: consider if we can get rid of the recovery listener enablement to speed up the scale down processing
	// Enabling recovery listener to speed up recovery processing
	jsonResult, err := wildflyutil.ExecuteMgmtOp(scaleDownPod, jbossHome, wildflyutil.MgmtOpTxnEnableRecoveryListener)
	if err != nil {
		return false, "", fmt.Errorf("Cannot enable transaction recovery listener for scaling down pod %v, error: %v", scaleDownPodName, err)
	}
	if !wildflyutil.IsMgmtOutcomeSuccesful(jsonResult) {
		return false, "", fmt.Errorf("Failed to enable transaction recovery listener for scaling down pod %v. Scaledown processing cannot trigger recovery. "+
			"Management command: %v, JSON response: %v", scaleDownPodName, wildflyutil.MgmtOpTxnEnableRecoveryListener, jsonResult)
	}
	// Enabling the recovery listner may require the server being reloaded
	isReloadRequired := wildflyutil.ReadJSONDataByIndex(jsonResult["response-headers"], "operation-requires-reload")
	if isReloadRequiredBool, _ := strconv.ParseBool(isReloadRequired); isReloadRequiredBool {
		jsonResult, err = wildflyutil.ExecuteMgmtOp(scaleDownPod, jbossHome, wildflyutil.MgmtOpReload)
		if err != nil {
			return false, "", fmt.Errorf("Cannot reload application container for scaling down pod %v, error: %v", scaleDownPodName, err)
		}
		for serverStateCheckCounter := 0; err != nil && serverStateCheckCounter < reloadRetryCount; serverStateCheckCounter++ {
			jsonResult, err = wildflyutil.ExecuteMgmtOp(scaleDownPod, jbossHome, wildflyutil.MgmtOpServerStateRead)
		}
		if err != nil { // reload did not yet finished and server is not properly running
			return false, "", fmt.Errorf("Failed waiting for server to be reloaded for the scaling down pod %v, error: %v", scaleDownPodName, err)
		}
	}
	// Reading recovery port from the app server with management port
	scaleDownPodRecoveryPort, err := wildflyutil.GetTransactionRecoveryPort(scaleDownPod, jbossHome)
	if err != nil || scaleDownPodRecoveryPort == 0 {
		scaleDownPodRecoveryPort = defaultRecoveryPort
	}
	if err != nil {
		reqLogger.Error(err, "Error on reading transaction recovery port with management command")
	}
	// With enabled recovery listener and the port, let's start the recovery scan
	reqLogger.Info("Executing recovery scan", "Pod name", scaleDownPodName, "Pod IP", scaleDownPodIP, "Recovery port", scaleDownPodRecoveryPort)
	_, err = wildflyutil.SocketConnect(scaleDownPodIP, scaleDownPodRecoveryPort, txnRecoveryScanCommand)
	if err != nil {
		return false, "", fmt.Errorf("Failed to run transaction recovery scan for scaling down pod %v, error: %v", scaleDownPodName, err)
	}

	// no error on recovery scan => all the registered resources were accessible and could be rolled-back if that was necessary
	foundLogLine, err := wildflyutil.VerifyLogContainsRegexp(scaleDownPod, scaleDownPodLogTimestampAtStart, recoveryErrorRegExp)
	if err != nil {
		return false, "", fmt.Errorf("Cannot parse log from scaling down pod %v, error: %v", scaleDownPodName, err)
	}
	if foundLogLine != "" {
		retString := fmt.Sprintf("Scale down transaction recovery processing contains errors in log. The recovery will be retried."+
			"Pod name: %v, log line with error '%v'", scaleDownPod, foundLogLine)
		return false, retString, nil
	}
	// Probing transaction log to verify there is not in-doubt transaction in the log
	_, err = wildflyutil.ExecuteMgmtOp(scaleDownPod, jbossHome, wildflyutil.MgmtOpTxnProbe)
	if err != nil {
		return false, "", fmt.Errorf("Error in probing transaction log for scaling down pod %v, error: %v", scaleDownPodName, err)
	}
	// Transaction log was probed, now we read the set of transactions which are in-doubt
	jsonResult, err = wildflyutil.ExecuteMgmtOp(scaleDownPod, jbossHome, wildflyutil.MgmtOpTxnRead)
	if err != nil {
		return false, "", fmt.Errorf("Cannot read transactions from the transaction log for pod scaling down %v, error: %v", scaleDownPodName, err)
	}
	if !wildflyutil.IsMgmtOutcomeSuccesful(jsonResult) {
		return false, "", fmt.Errorf("Cannot get list of the in-doubt transactions at pod %v for transaction scaledown", scaleDownPodName)
	}
	// Is the number of in-doubt transactions equal to zero?
	transactions := jsonResult["result"]
	txnMap, isMap := transactions.(map[string]interface{}) // typing the variable to be a map of interfaces
	if isMap && len(txnMap) > 0 {
		retString := fmt.Sprintf("Recovery scan to be invoked as the transaction log storage is not empty for pod scaling down pod %v, "+
			"transaction list: %v", scaleDownPodName, txnMap)
		return false, retString, nil
	}
	// Verification of the unfinished data of the WildFly transaction client (verification of the directory content)
	lsCommand := fmt.Sprintf(`ls %s/%s/ 2> /dev/null || true`, jbossHome+standaloneServerDataDirSuffix, wftcDataDirName)
	commandResult, err := wildflyutil.ExecRemote(scaleDownPod, lsCommand)
	if err != nil {
		return false, "", fmt.Errorf("Cannot query filesystem to check existing remote transactions for pod scaling down %v. "+
			"Exec command %v", scaleDownPodName, lsCommand)
	}
	if commandResult != "" {
		retString := fmt.Sprintf("WFTC data dir is not empty and scaling down of the pod '%v' will be retried."+
			"Wildfly Transacton Client data dir path '%v', output listing: %v",
			scaleDownPodName, jbossHome+standaloneServerDataDirSuffix+"/"+wftcDataDirName, commandResult)
		return false, retString, nil
	}
	return true, "", nil
}

// matches checks if the envVar from the WildFlyServerSpec matches the same env var from the container.
// If it does not match, it updates the container EnvVar with the fields from the WildFlyServerSpec and return false.
func matches(container *v1.Container, envVar corev1.EnvVar) bool {
	for index, e := range container.Env {
		if envVar.Name == e.Name {
			if !reflect.DeepEqual(envVar, e) {
				container.Env[index] = envVar
				return false
			}
			return true
		}
	}
	//append new spec env to container's env var
	container.Env = append(container.Env, envVar)
	return false
}

func getWildflyServerPodStatusByName(w *wildflyv1alpha1.WildFlyServer, podName string) *wildflyv1alpha1.PodStatus {
	for _, podStatus := range w.Status.Pods {
		if podName == podStatus.Name {
			return &podStatus
		}
	}
	return nil
}

// getPodStatus returns the pod names of the array of pods passed in
func getPodStatus(pods []corev1.Pod, originalPodStatuses []wildflyv1alpha1.PodStatus) (bool, []wildflyv1alpha1.PodStatus) {
	var requeue = false
	var podStatuses []wildflyv1alpha1.PodStatus
	podStatusesOriginalMap := make(map[string]wildflyv1alpha1.PodStatus)
	for _, v := range originalPodStatuses {
		podStatusesOriginalMap[v.Name] = v
	}
	for _, pod := range pods {
		podState := wildflyv1alpha1.PodStateActive
		if value, exists := podStatusesOriginalMap[pod.Name]; exists {
			podState = value.State
		}
		podStatuses = append(podStatuses, wildflyv1alpha1.PodStatus{
			Name:  pod.Name,
			PodIP: pod.Status.PodIP,
			State: podState,
		})
		if pod.Status.PodIP == "" {
			requeue = true
		}
	}
	return requeue, podStatuses
}

// createLivenessProbe create a Exec probe if the SERVER_LIVENESS_SCRIPT env var is present.
// Otherwise, it creates a HTTPGet probe that checks the /health endpoint on the admin port.
//
// If defined, the SERVER_LIVENESS_SCRIPT env var must be the path of a shell script that
// complies to the Kuberenetes probes requirements.
func createLivenessProbe() *corev1.Probe {
	livenessProbeScript, defined := os.LookupEnv("SERVER_LIVENESS_SCRIPT")
	if defined {
		return &corev1.Probe{
			Handler: corev1.Handler{
				Exec: &v1.ExecAction{
					Command: []string{"/bin/bash", "-c", livenessProbeScript},
				},
			},
			InitialDelaySeconds: 60,
		}
	}
	return &corev1.Probe{
		Handler: corev1.Handler{
			HTTPGet: &v1.HTTPGetAction{
				Path: "/health",
				Port: intstr.FromString("admin"),
			},
		},
		InitialDelaySeconds: 60,
	}
}

// createReadinessProbe create a Exec probe if the SERVER_READINESS_SCRIPT env var is present.
// Otherwise, it returns nil (i.e. no readiness probe is configured).
//
// If defined, the SERVER_READINESS_SCRIPT env var must be the path of a shell script that
// complies to the Kuberenetes probes requirements.
func createReadinessProbe() *corev1.Probe {
	readinessProbeScript, defined := os.LookupEnv("SERVER_READINESS_SCRIPT")
	if defined {
		return &corev1.Probe{
			Handler: corev1.Handler{
				Exec: &v1.ExecAction{
					Command: []string{"/bin/bash", "-c", readinessProbeScript},
				},
			},
		}
	}
	return nil
}

func labelsForWildFly(w *wildflyv1alpha1.WildFlyServer) map[string]string {
	labels := make(map[string]string)
	labels["app.kubernetes.io/name"] = w.Name
	labels["app.kubernetes.io/managed-by"] = os.Getenv("LABEL_APP_MANAGED_BY")
	labels["app.openshift.io/runtime"] = os.Getenv("LABEL_APP_RUNTIME")
	if w.Labels != nil {
		for labelKey, labelValue := range w.Labels {
			labels[labelKey] = labelValue
		}
	}
	return labels
}

func loadBalancerServiceName(w *wildflyv1alpha1.WildFlyServer) string {
	return w.Name + "-loadbalancer"
}

func errorIsNoMatchesForKind(err error, kind string, version string) bool {
	return strings.HasPrefix(err.Error(), fmt.Sprintf("no matches for kind \"%s\" in version \"%s\"", kind, version))
}
