package kube

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"agent-workspace/internal/control"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
)

type probeTransport func(*http.Request) (*http.Response, error)

func (f probeTransport) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestObserveWaitsForService(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		err    error
		ready  bool
	}{
		{"connection refused", 0, errors.New("connection refused"), false},
		{"unhealthy", 503, nil, false},
		{"healthy", 200, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := control.Workspace{ID: "demo"}
			deployment := ownedDeployment(w, 1, 1)
			r := Runtime{
				Namespace: "agent-workspace",
				Client:    fake.NewSimpleClientset(deployment),
				HTTPClient: &http.Client{Transport: probeTransport(func(req *http.Request) (*http.Response, error) {
					if req.URL.String() != "http://nc-demo.agent-workspace.svc.cluster.local:8080/health" {
						t.Fatalf("unexpected probe URL: %s", req.URL)
					}
					if tc.err != nil {
						return nil, tc.err
					}
					return &http.Response{StatusCode: tc.status, Body: io.NopCloser(strings.NewReader("ok")), Header: make(http.Header)}, nil
				})},
			}
			obs, err := r.Observe(context.Background(), w, profile())
			if err != nil || !obs.Exists || obs.Ready != tc.ready {
				t.Fatalf("observation=%+v err=%v", obs, err)
			}
		})
	}
}

func profile() control.Profile {
	return control.Profile{Image: "demo:local", Port: 8080, HealthPath: "/health", MountPath: "/workspace", Storage: "1Gi", CPU: "1", Memory: "512Mi"}
}

func ownedDeployment(w control.Workspace, replicas, ready int32) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name(w), Namespace: "agent-workspace", Labels: labels(w)},
		Spec:       appsv1.DeploymentSpec{Replicas: ptr.To(replicas)},
		Status:     appsv1.DeploymentStatus{Replicas: 0, ReadyReplicas: ready},
	}
}

func TestManifestPreservesStorageAndSeparatesCredentials(t *testing.T) {
	p := profile()
	p.EnvSecret = "runtime-env"
	p.ConfigSecret = "runtime-config"
	p.ConfigPath = "/app/configs"
	b, err := Manifest(control.Workspace{ID: "demo"}, p, "agent-workspace")
	if err != nil {
		t.Fatal(err)
	}
	var list struct {
		Items []map[string]any `json:"items"`
	}
	if err := json.Unmarshal(b, &list); err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 3 {
		t.Fatal("expected PVC, Deployment and Service")
	}
	if list.Items[0]["kind"] != "PersistentVolumeClaim" {
		t.Fatal("storage must be created first")
	}
	if strings.Contains(string(b), "ownerReferences") {
		t.Fatal("scale/delete workload must not implicitly delete PVC")
	}
	spec := list.Items[1]["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
	if spec["automountServiceAccountToken"] != false {
		t.Fatal("agent received control-plane credential")
	}
	if len(spec["volumes"].([]any)) != 2 {
		t.Fatal("missing workspace or config volume")
	}
}

func TestStopScalesToZeroAndKeepsPVC(t *testing.T) {
	w := control.Workspace{ID: "demo"}
	deployment := ownedDeployment(w, 1, 0)
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: name(w), Namespace: "agent-workspace", Labels: labels(w)}}
	r := Runtime{Namespace: "agent-workspace", Client: fake.NewSimpleClientset(deployment, pvc)}
	if err := r.Stop(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	got, err := r.Client.AppsV1().Deployments(r.Namespace).Get(context.Background(), name(w), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Spec.Replicas == nil || *got.Spec.Replicas != 0 {
		t.Fatalf("replicas=%v, want 0", got.Spec.Replicas)
	}
	if _, err := r.Client.CoreV1().PersistentVolumeClaims(r.Namespace).Get(context.Background(), name(w), metav1.GetOptions{}); err != nil {
		t.Fatalf("PVC was removed by Stop: %v", err)
	}
}

func TestEnsureRefusesForeignResources(t *testing.T) {
	w := control.Workspace{ID: "demo"}
	foreign := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name(w), Namespace: "agent-workspace", Labels: map[string]string{"app.kubernetes.io/managed-by": "someone-else"}}}
	r := Runtime{Namespace: "agent-workspace", Client: fake.NewSimpleClientset(foreign)}
	if err := r.Ensure(context.Background(), w, profile()); err == nil {
		t.Fatal("accepted foreign deployment")
	}
}

func TestRestartChangesPodTemplateAndPreservesReplicasAndPVC(t *testing.T) {
	w := control.Workspace{ID: "demo"}
	deployment := ownedDeployment(w, 1, 1)
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: name(w), Namespace: "agent-workspace", Labels: labels(w)}}
	r := Runtime{Namespace: "agent-workspace", Client: fake.NewSimpleClientset(deployment, pvc)}
	if err := r.Restart(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	got, err := r.Client.AppsV1().Deployments(r.Namespace).Get(context.Background(), name(w), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Spec.Template.Annotations[restartedAtLabel] == "" {
		t.Fatal("restart annotation was not written")
	}
	if got.Spec.Replicas == nil || *got.Spec.Replicas != 1 {
		t.Fatalf("replicas=%v, want 1", got.Spec.Replicas)
	}
	if _, err := r.Client.CoreV1().PersistentVolumeClaims(r.Namespace).Get(context.Background(), name(w), metav1.GetOptions{}); err != nil {
		t.Fatalf("PVC was removed by Restart: %v", err)
	}
}

func TestRestartRefusesForeignDeployment(t *testing.T) {
	w := control.Workspace{ID: "demo"}
	foreign := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name(w), Namespace: "agent-workspace", Labels: map[string]string{"app.kubernetes.io/managed-by": "someone-else"}}}
	r := Runtime{Namespace: "agent-workspace", Client: fake.NewSimpleClientset(foreign)}
	if err := r.Restart(context.Background(), w); err == nil {
		t.Fatal("accepted foreign deployment")
	}
}

func TestProbeUsesAPIServer(t *testing.T) {
	if err := (&Runtime{ProbeFunc: func(context.Context) error { return nil }}).Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	failing := &Runtime{ProbeFunc: func(context.Context) error { return errors.New("cluster unreachable") }}
	if err := failing.Probe(context.Background()); err == nil {
		t.Fatal("unreachable API server reported ready")
	}
}
