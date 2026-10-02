// Package kube keeps Kubernetes access behind a small runtime interface.
package kube

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sort"
	"sync/atomic"
	"time"

	"agent-workspace/internal/control"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	appslisters "k8s.io/client-go/listers/apps/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/ptr"
)

const (
	managedByLabel       = "app.kubernetes.io/managed-by"
	managedByValue       = "agent-workspace"
	workspaceLabel       = "agent-workspace/workspace"
	restartedAtLabel     = "agent-workspace/restartedAt"
	specHashAnnotation   = "agent-workspace/spec-hash"
	defaultClusterDomain = "cluster.local"
)

type Runtime struct {
	Namespace     string
	ClusterDomain string
	Client        kubernetes.Interface
	HTTPClient    *http.Client
	ProbeFunc     func(context.Context) error
	// APIRequests counts every call to the API server. It is wired into the
	// rest.Config transport by NewRuntime; tests on the fake client bypass it.
	APIRequests *APIMetrics

	events      chan string
	deployments appslisters.DeploymentLister
	pods        corelisters.PodLister
	synced      atomic.Bool
}

// NewRuntime builds a Kubernetes client from in-cluster credentials or the
// current kubeconfig context.
func NewRuntime(namespace, kubeContext string) (*Runtime, error) {
	cfg, err := loadRESTConfig(kubeContext)
	if err != nil {
		return nil, err
	}
	metrics := NewAPIMetrics()
	cfg.WrapTransport = metrics.Wrap
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("create kubernetes client: %w", err)
	}
	return &Runtime{Namespace: namespace, Client: client, APIRequests: metrics}, nil
}

func loadRESTConfig(kubeContext string) (*rest.Config, error) {
	if os.Getenv("KUBERNETES_SERVICE_HOST") != "" {
		cfg, err := rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("load in-cluster config: %w", err)
		}
		return cfg, nil
	}
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	overrides := &clientcmd.ConfigOverrides{CurrentContext: kubeContext}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides).ClientConfig()
}

func (r *Runtime) client() (kubernetes.Interface, error) {
	if r.Client == nil {
		return nil, fmt.Errorf("kubernetes client is required")
	}
	return r.Client, nil
}

func name(w control.Workspace) string { return "nc-" + w.ID }

func workspaceLabels(w control.Workspace) map[string]string {
	return map[string]string{managedByLabel: managedByValue, workspaceLabel: w.ID}
}

func owns(itemLabels map[string]string, w control.Workspace) bool {
	return itemLabels[managedByLabel] == managedByValue && itemLabels[workspaceLabel] == w.ID
}

func (r *Runtime) Observe(ctx context.Context, w control.Workspace, p control.Profile) (control.Observation, error) {
	d, err := r.getDeployment(ctx, name(w))
	if apierrors.IsNotFound(err) {
		return control.Observation{}, nil
	}
	if err != nil {
		return control.Observation{}, err
	}
	if !owns(d.Labels, w) {
		return control.Observation{}, fmt.Errorf("refusing unmanaged deployment %s", name(w))
	}
	domain := r.ClusterDomain
	if domain == "" {
		domain = defaultClusterDomain
	}
	target := url.URL{Scheme: "http", Host: fmt.Sprintf("%s.%s.svc.%s:%d", name(w), r.Namespace, domain, p.Port)}
	obs := control.Observation{Exists: true, Endpoint: target.String()}
	if d.Spec.Replicas == nil || *d.Spec.Replicas == 0 || d.Status.ReadyReplicas == 0 {
		return obs, nil
	}

	obs.Image = rolledOutImage(d)

	health := target
	health.Path = p.HealthPath
	probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, health.String(), nil)
	if err != nil {
		return obs, err
	}
	httpClient := r.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		// A missing or unreachable Service is a normal "not ready yet" state.
		return obs, nil
	}
	defer resp.Body.Close()
	obs.Ready = resp.StatusCode >= 200 && resp.StatusCode < 300
	return obs, nil
}

// rolledOutImage returns the workload image once every replica is the updated
// one and ready, and "" while a rollout is still in progress. ReadyReplicas
// alone is not enough: right after the spec changes, the old pod is still
// ready and would look like a successful upgrade.
func rolledOutImage(d *appsv1.Deployment) string {
	if d.Spec.Replicas == nil || len(d.Spec.Template.Spec.Containers) == 0 {
		return ""
	}
	want := *d.Spec.Replicas
	st := d.Status
	if want == 0 || st.ObservedGeneration < d.Generation || st.UpdatedReplicas < want ||
		st.ReadyReplicas < want || st.Replicas != st.UpdatedReplicas {
		return ""
	}
	return d.Spec.Template.Spec.Containers[0].Image
}

func (r *Runtime) Ensure(ctx context.Context, w control.Workspace, p control.Profile) error {
	pvc, deployment, service, err := BuildObjects(w, p, r.Namespace)
	if err != nil {
		return err
	}
	if err := r.ensurePVC(ctx, pvc, w); err != nil {
		return err
	}
	if err := r.ensureDeployment(ctx, deployment, w); err != nil {
		return err
	}
	return r.ensureService(ctx, service, w)
}

func (r *Runtime) ensurePVC(ctx context.Context, desired *corev1.PersistentVolumeClaim, w control.Workspace) error {
	client, err := r.client()
	if err != nil {
		return err
	}
	current, err := client.CoreV1().PersistentVolumeClaims(r.Namespace).Get(ctx, desired.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = client.CoreV1().PersistentVolumeClaims(r.Namespace).Create(ctx, desired, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	if !owns(current.Labels, w) {
		return fmt.Errorf("refusing unmanaged pvc %s", desired.Name)
	}
	return nil
}

func (r *Runtime) ensureDeployment(ctx context.Context, desired *appsv1.Deployment, w control.Workspace) error {
	client, err := r.client()
	if err != nil {
		return err
	}
	current, err := r.getDeployment(ctx, desired.Name)
	if apierrors.IsNotFound(err) {
		if _, err = client.AppsV1().Deployments(r.Namespace).Create(ctx, desired, metav1.CreateOptions{}); err == nil {
			return nil
		}
		if !apierrors.IsAlreadyExists(err) {
			return err
		}
		// The cache lags behind the API server: the create raced with a read
		// that did not see the object yet. Read it directly and continue.
		if current, err = client.AppsV1().Deployments(r.Namespace).Get(ctx, desired.Name, metav1.GetOptions{}); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	if !owns(current.Labels, w) {
		return fmt.Errorf("refusing unmanaged deployment %s", desired.Name)
	}
	// A lister returns the shared cache object; never mutate it in place.
	current = current.DeepCopy()
	hash := desired.Annotations[specHashAnnotation]
	if current.Annotations[specHashAnnotation] == hash {
		// The template is already what we want. Ensure runs on every poll while a
		// workspace starts, so rewriting an unchanged spec would only add API
		// writes; scaling back from zero is the one field that may still differ.
		if current.Spec.Replicas != nil && *current.Spec.Replicas == *desired.Spec.Replicas {
			return nil
		}
		current.Spec.Replicas = desired.Spec.Replicas
		_, err = client.AppsV1().Deployments(r.Namespace).Update(ctx, current, metav1.UpdateOptions{})
		return err
	}
	// A restart is recorded on the live template. Replacing the spec without it
	// would change the template again and trigger a second rollout.
	if at := current.Spec.Template.Annotations[restartedAtLabel]; at != "" {
		if desired.Spec.Template.Annotations == nil {
			desired.Spec.Template.Annotations = map[string]string{}
		}
		desired.Spec.Template.Annotations[restartedAtLabel] = at
	}
	current.Spec = desired.Spec
	if current.Annotations == nil {
		current.Annotations = map[string]string{}
	}
	current.Annotations[specHashAnnotation] = hash
	_, err = client.AppsV1().Deployments(r.Namespace).Update(ctx, current, metav1.UpdateOptions{})
	return err
}

func (r *Runtime) ensureService(ctx context.Context, desired *corev1.Service, w control.Workspace) error {
	client, err := r.client()
	if err != nil {
		return err
	}
	current, err := client.CoreV1().Services(r.Namespace).Get(ctx, desired.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = client.CoreV1().Services(r.Namespace).Create(ctx, desired, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	if !owns(current.Labels, w) {
		return fmt.Errorf("refusing unmanaged service %s", desired.Name)
	}
	if sameServiceShape(current, desired) {
		return nil
	}
	current.Spec.Ports = desired.Spec.Ports
	current.Spec.Selector = desired.Spec.Selector
	_, err = client.CoreV1().Services(r.Namespace).Update(ctx, current, metav1.UpdateOptions{})
	return err
}

// sameServiceShape compares only the fields this controller sets, because the
// API server fills in defaults such as protocol and cluster IP.
func sameServiceShape(current, desired *corev1.Service) bool {
	if len(current.Spec.Ports) != len(desired.Spec.Ports) || len(current.Spec.Selector) != len(desired.Spec.Selector) {
		return false
	}
	for k, v := range desired.Spec.Selector {
		if current.Spec.Selector[k] != v {
			return false
		}
	}
	for i, want := range desired.Spec.Ports {
		got := current.Spec.Ports[i]
		if got.Name != want.Name || got.Port != want.Port || got.TargetPort != want.TargetPort {
			return false
		}
	}
	return true
}

func (r *Runtime) Restart(ctx context.Context, w control.Workspace) error {
	client, err := r.client()
	if err != nil {
		return err
	}
	d, err := r.getDeployment(ctx, name(w))
	if err != nil {
		return err
	}
	if !owns(d.Labels, w) {
		return fmt.Errorf("refusing unmanaged deployment %s", name(w))
	}
	d = d.DeepCopy()
	if d.Spec.Template.Annotations == nil {
		d.Spec.Template.Annotations = map[string]string{}
	}
	d.Spec.Template.Annotations[restartedAtLabel] = time.Now().UTC().Format(time.RFC3339Nano)
	_, err = client.AppsV1().Deployments(r.Namespace).Update(ctx, d, metav1.UpdateOptions{})
	return err
}

func (r *Runtime) Stop(ctx context.Context, w control.Workspace) error {
	client, err := r.client()
	if err != nil {
		return err
	}
	d, err := r.getDeployment(ctx, name(w))
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !owns(d.Labels, w) {
		return fmt.Errorf("refusing unmanaged deployment %s", name(w))
	}
	if d.Spec.Replicas == nil || *d.Spec.Replicas != 0 {
		d = d.DeepCopy()
		d.Spec.Replicas = ptr.To(int32(0))
		if _, err := client.AppsV1().Deployments(r.Namespace).Update(ctx, d, metav1.UpdateOptions{}); err != nil {
			return err
		}
	}
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		// With the cache synced this poll is memory-only; without it the poll
		// still goes to the API server, exactly as before.
		current, err := r.getDeployment(ctx, name(w))
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if !owns(current.Labels, w) {
			return fmt.Errorf("refusing unmanaged deployment %s", name(w))
		}
		if current.Status.Replicas == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (r *Runtime) Delete(ctx context.Context, w control.Workspace) error {
	if err := r.checkOwnership(ctx, w); err != nil {
		return err
	}
	// PVCs mounted by a running Pod can remain Terminating, so scale down first.
	if err := r.Stop(ctx, w); err != nil {
		return err
	}
	client, err := r.client()
	if err != nil {
		return err
	}
	foreground := metav1.DeletePropagationForeground
	if err := client.AppsV1().Deployments(r.Namespace).Delete(ctx, name(w), metav1.DeleteOptions{PropagationPolicy: &foreground}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if err := client.CoreV1().Services(r.Namespace).Delete(ctx, name(w), metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if err := client.CoreV1().PersistentVolumeClaims(r.Namespace).Delete(ctx, name(w), metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	// Credentials go last and only here: Stop and Restart keep them, like the
	// PVC, so a resumed workspace still has its keys.
	if err := client.CoreV1().Secrets(r.Namespace).Delete(ctx, credentialSecretName(w), metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

func (r *Runtime) Probe(ctx context.Context) error {
	if r.ProbeFunc != nil {
		return r.ProbeFunc(ctx)
	}
	client, err := r.client()
	if err != nil {
		return err
	}
	_, err = client.Discovery().ServerVersion()
	return err
}

func (r *Runtime) checkOwnership(ctx context.Context, w control.Workspace) error {
	client, err := r.client()
	if err != nil {
		return err
	}
	if d, err := r.getDeployment(ctx, name(w)); err == nil {
		if !owns(d.Labels, w) {
			return fmt.Errorf("refusing unmanaged deployment %s", name(w))
		}
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	if s, err := client.CoreV1().Services(r.Namespace).Get(ctx, name(w), metav1.GetOptions{}); err == nil {
		if !owns(s.Labels, w) {
			return fmt.Errorf("refusing unmanaged service %s", name(w))
		}
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	if p, err := client.CoreV1().PersistentVolumeClaims(r.Namespace).Get(ctx, name(w), metav1.GetOptions{}); err == nil {
		if !owns(p.Labels, w) {
			return fmt.Errorf("refusing unmanaged pvc %s", name(w))
		}
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	if sec, err := client.CoreV1().Secrets(r.Namespace).Get(ctx, credentialSecretName(w), metav1.GetOptions{}); err == nil {
		if !owns(sec.Labels, w) {
			return fmt.Errorf("refusing unmanaged secret %s", credentialSecretName(w))
		}
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// BuildObjects builds the Kubernetes objects managed for one workspace.
func BuildObjects(w control.Workspace, p control.Profile, namespace string) (*corev1.PersistentVolumeClaim, *appsv1.Deployment, *corev1.Service, error) {
	if !control.ValidName(w.ID) || !control.ValidName(namespace) {
		return nil, nil, nil, control.ErrInvalid
	}
	if err := ValidateProfile(p); err != nil {
		return nil, nil, nil, err
	}
	q := quantities(p)
	meta := metav1.ObjectMeta{Name: name(w), Namespace: namespace, Labels: workspaceLabels(w)}
	storage := q.storage
	pvc := &corev1.PersistentVolumeClaim{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolumeClaim"},
		ObjectMeta: meta,
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: storage}},
		},
	}

	// Map iteration order is random. An unsorted env list would change the pod
	// template on every build, and every template change is a rollout.
	keys := make([]string, 0, len(p.Env))
	for k := range p.Env {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	env := make([]corev1.EnvVar, 0, len(p.Env)+1)
	for _, k := range keys {
		env = append(env, corev1.EnvVar{Name: k, Value: p.Env[k]})
	}
	env = append(env, corev1.EnvVar{Name: "WORKSPACE_ID", Value: w.ID})
	mounts := []corev1.VolumeMount{{Name: "workspace", MountPath: p.MountPath}}
	volumes := []corev1.Volume{{
		Name:         "workspace",
		VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: name(w)}},
	}}
	if p.ConfigSecret != "" {
		mounts = append(mounts, corev1.VolumeMount{Name: "config", MountPath: p.ConfigPath, ReadOnly: true})
		volumes = append(volumes, corev1.Volume{
			Name:         "config",
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: p.ConfigSecret}},
		})
	}
	if p.CredentialPath != "" {
		mounts = append(mounts, corev1.VolumeMount{Name: "credentials", MountPath: p.CredentialPath, ReadOnly: true})
		volumes = append(volumes, credentialVolume(w))
	}
	container := corev1.Container{
		Name:            "agent",
		Image:           p.Image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Ports:           []corev1.ContainerPort{{Name: "http", ContainerPort: int32(p.Port)}},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("100m"),
				corev1.ResourceMemory: resource.MustParse("128Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    q.cpu,
				corev1.ResourceMemory: q.memory,
			},
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler:   corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: p.HealthPath, Port: intstr.FromString("http")}},
			PeriodSeconds:  2,
			TimeoutSeconds: 2,
		},
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: ptr.To(false),
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		},
		Command:      p.Command,
		Args:         p.Args,
		Env:          env,
		VolumeMounts: mounts,
	}
	if p.EnvSecret != "" {
		container.EnvFrom = []corev1.EnvFromSource{{
			SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: p.EnvSecret}},
		}}
	}
	deployment := &appsv1.Deployment{
		TypeMeta:   metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
		ObjectMeta: meta,
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To(int32(1)),
			Strategy: appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType},
			Selector: &metav1.LabelSelector{MatchLabels: workspaceLabels(w)},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: workspaceLabels(w)},
				Spec: corev1.PodSpec{
					AutomountServiceAccountToken: ptr.To(false),
					// Enough for a namespace to enforce the "restricted" Pod Security
					// profile. runAsNonRoot makes the kubelet refuse an image whose
					// USER is root or not numeric instead of running it as root.
					SecurityContext: &corev1.PodSecurityContext{
						FSGroup:        ptr.To(int64(1000)),
						RunAsNonRoot:   ptr.To(true),
						SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
					},
					Containers: []corev1.Container{container},
					Volumes:    volumes,
				},
			},
		},
	}
	hash, err := specHash(deployment.Spec)
	if err != nil {
		return nil, nil, nil, err
	}
	deployment.ObjectMeta.Annotations = map[string]string{specHashAnnotation: hash}
	service := &corev1.Service{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: meta,
		Spec: corev1.ServiceSpec{
			Selector: workspaceLabels(w),
			Ports:    []corev1.ServicePort{{Name: "http", Port: int32(p.Port), TargetPort: intstr.FromString("http")}},
		},
	}
	return pvc, deployment, service, nil
}

type profileQuantities struct{ storage, cpu, memory resource.Quantity }

// quantities must only be called after ValidateProfile succeeded.
func quantities(p control.Profile) profileQuantities {
	return profileQuantities{
		storage: resource.MustParse(p.Storage),
		cpu:     resource.MustParse(p.CPU),
		memory:  resource.MustParse(p.Memory),
	}
}

// ValidateProfile adds the Kubernetes-specific checks to Profile.Validate. It
// runs at startup so a malformed quantity fails fast instead of panicking
// inside a reconcile goroutine.
func ValidateProfile(p control.Profile) error {
	if err := p.Validate(); err != nil {
		return err
	}
	for field, raw := range map[string]string{"storage": p.Storage, "cpu": p.CPU, "memory": p.Memory} {
		if _, err := resource.ParseQuantity(raw); err != nil {
			return fmt.Errorf("%w: %s %q is not a Kubernetes quantity", control.ErrInvalid, field, raw)
		}
	}
	return nil
}

// specHash fingerprints the replica-independent part of the Deployment spec.
// It is stored on the Deployment itself, not on the pod template, so recording
// it never causes a rollout.
func specHash(spec appsv1.DeploymentSpec) (string, error) {
	spec.Replicas = nil
	b, err := json.Marshal(spec)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:8]), nil
}

// Manifest returns the same objects as a Kubernetes List for diagnostics.
func Manifest(w control.Workspace, p control.Profile, namespace string) ([]byte, error) {
	pvc, deployment, service, err := BuildObjects(w, p, namespace)
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{
		"apiVersion": "v1",
		"kind":       "List",
		"items":      []any{pvc, deployment, service},
	})
}
