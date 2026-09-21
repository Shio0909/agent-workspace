// Package kube keeps Kubernetes access behind a small runtime interface.
package kube

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"time"

	"agent-workspace/internal/control"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/ptr"
)

const (
	managedByLabel       = "app.kubernetes.io/managed-by"
	managedByValue       = "agent-workspace"
	workspaceLabel       = "agent-workspace/workspace"
	restartedAtLabel     = "agent-workspace/restartedAt"
	defaultClusterDomain = "cluster.local"
)

type Runtime struct {
	Namespace     string
	ClusterDomain string
	Client        kubernetes.Interface
	HTTPClient    *http.Client
	ProbeFunc     func(context.Context) error
}

// NewRuntime builds a Kubernetes client from in-cluster credentials or the
// current kubeconfig context.
func NewRuntime(namespace, kubeContext string) (*Runtime, error) {
	cfg, err := loadRESTConfig(kubeContext)
	if err != nil {
		return nil, err
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("create kubernetes client: %w", err)
	}
	return &Runtime{Namespace: namespace, Client: client}, nil
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

func labels(w control.Workspace) map[string]string {
	return map[string]string{managedByLabel: managedByValue, workspaceLabel: w.ID}
}

func owns(itemLabels map[string]string, w control.Workspace) bool {
	return itemLabels[managedByLabel] == managedByValue && itemLabels[workspaceLabel] == w.ID
}

func (r *Runtime) Observe(ctx context.Context, w control.Workspace, p control.Profile) (control.Observation, error) {
	client, err := r.client()
	if err != nil {
		return control.Observation{}, err
	}
	d, err := client.AppsV1().Deployments(r.Namespace).Get(ctx, name(w), metav1.GetOptions{})
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
	current, err := client.AppsV1().Deployments(r.Namespace).Get(ctx, desired.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = client.AppsV1().Deployments(r.Namespace).Create(ctx, desired, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	if !owns(current.Labels, w) {
		return fmt.Errorf("refusing unmanaged deployment %s", desired.Name)
	}
	current.Spec = desired.Spec
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
	current.Spec.Ports = desired.Spec.Ports
	current.Spec.Selector = desired.Spec.Selector
	_, err = client.CoreV1().Services(r.Namespace).Update(ctx, current, metav1.UpdateOptions{})
	return err
}

func (r *Runtime) Restart(ctx context.Context, w control.Workspace) error {
	client, err := r.client()
	if err != nil {
		return err
	}
	d, err := client.AppsV1().Deployments(r.Namespace).Get(ctx, name(w), metav1.GetOptions{})
	if err != nil {
		return err
	}
	if !owns(d.Labels, w) {
		return fmt.Errorf("refusing unmanaged deployment %s", name(w))
	}
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
	d, err := client.AppsV1().Deployments(r.Namespace).Get(ctx, name(w), metav1.GetOptions{})
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
		d.Spec.Replicas = ptr.To(int32(0))
		if _, err := client.AppsV1().Deployments(r.Namespace).Update(ctx, d, metav1.UpdateOptions{}); err != nil {
			return err
		}
	}
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		current, err := client.AppsV1().Deployments(r.Namespace).Get(ctx, name(w), metav1.GetOptions{})
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
	if d, err := client.AppsV1().Deployments(r.Namespace).Get(ctx, name(w), metav1.GetOptions{}); err == nil {
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
	return nil
}

// BuildObjects builds the Kubernetes objects managed for one workspace.
func BuildObjects(w control.Workspace, p control.Profile, namespace string) (*corev1.PersistentVolumeClaim, *appsv1.Deployment, *corev1.Service, error) {
	if !control.ValidName(w.ID) || !control.ValidName(namespace) {
		return nil, nil, nil, control.ErrInvalid
	}
	if err := p.Validate(); err != nil {
		return nil, nil, nil, err
	}
	meta := metav1.ObjectMeta{Name: name(w), Namespace: namespace, Labels: labels(w)}
	storage := resource.MustParse(p.Storage)
	pvc := &corev1.PersistentVolumeClaim{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolumeClaim"},
		ObjectMeta: meta,
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: storage}},
		},
	}

	env := make([]corev1.EnvVar, 0, len(p.Env)+1)
	for k, v := range p.Env {
		env = append(env, corev1.EnvVar{Name: k, Value: v})
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
				corev1.ResourceCPU:    resource.MustParse(p.CPU),
				corev1.ResourceMemory: resource.MustParse(p.Memory),
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
			Selector: &metav1.LabelSelector{MatchLabels: labels(w)},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels(w)},
				Spec: corev1.PodSpec{
					AutomountServiceAccountToken: ptr.To(false),
					SecurityContext:              &corev1.PodSecurityContext{FSGroup: ptr.To(int64(1000))},
					Containers:                   []corev1.Container{container},
					Volumes:                      volumes,
				},
			},
		},
	}
	service := &corev1.Service{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: meta,
		Spec: corev1.ServiceSpec{
			Selector: labels(w),
			Ports:    []corev1.ServicePort{{Name: "http", Port: int32(p.Port), TargetPort: intstr.FromString("http")}},
		},
	}
	return pvc, deployment, service, nil
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
