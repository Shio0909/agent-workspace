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
		ObjectMeta: metav1.ObjectMeta{Name: name(w), Namespace: "agent-workspace", Labels: workspaceLabels(w)},
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
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: name(w), Namespace: "agent-workspace", Labels: workspaceLabels(w)}}
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
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: name(w), Namespace: "agent-workspace", Labels: workspaceLabels(w)}}
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

func deploymentActions(client *fake.Clientset, verb string) int {
	n := 0
	for _, a := range client.Actions() {
		if a.GetVerb() == verb && a.GetResource().Resource == "deployments" {
			n++
		}
	}
	return n
}

func TestEnsureAfterRestartKeepsTheRestartAndSkipsUnchangedSpecs(t *testing.T) {
	w := control.Workspace{ID: "demo"}
	client := fake.NewSimpleClientset()
	r := Runtime{Namespace: "agent-workspace", Client: client}
	ctx := context.Background()
	if err := r.Ensure(ctx, w, profile()); err != nil {
		t.Fatal(err)
	}
	if err := r.Restart(ctx, w); err != nil {
		t.Fatal(err)
	}
	restarted, _ := client.AppsV1().Deployments(r.Namespace).Get(ctx, name(w), metav1.GetOptions{})
	at := restarted.Spec.Template.Annotations[restartedAtLabel]
	updates := deploymentActions(client, "update")
	for i := 0; i < 5; i++ {
		if err := r.Ensure(ctx, w, profile()); err != nil {
			t.Fatal(err)
		}
	}
	got, _ := client.AppsV1().Deployments(r.Namespace).Get(ctx, name(w), metav1.GetOptions{})
	if got.Spec.Template.Annotations[restartedAtLabel] != at {
		t.Fatalf("Ensure undid the restart: %q -> %q", at, got.Spec.Template.Annotations[restartedAtLabel])
	}
	if n := deploymentActions(client, "update") - updates; n != 0 {
		t.Fatalf("unchanged spec was rewritten %d times", n)
	}
}

func TestEnsureScalesStoppedWorkspaceBackUpWithoutTouchingTheTemplate(t *testing.T) {
	w := control.Workspace{ID: "demo"}
	client := fake.NewSimpleClientset()
	r := Runtime{Namespace: "agent-workspace", Client: client}
	ctx := context.Background()
	if err := r.Ensure(ctx, w, profile()); err != nil {
		t.Fatal(err)
	}
	if err := r.Restart(ctx, w); err != nil {
		t.Fatal(err)
	}
	if err := r.Stop(ctx, w); err != nil {
		t.Fatal(err)
	}
	before, _ := client.AppsV1().Deployments(r.Namespace).Get(ctx, name(w), metav1.GetOptions{})
	if err := r.Ensure(ctx, w, profile()); err != nil {
		t.Fatal(err)
	}
	after, _ := client.AppsV1().Deployments(r.Namespace).Get(ctx, name(w), metav1.GetOptions{})
	if after.Spec.Replicas == nil || *after.Spec.Replicas != 1 {
		t.Fatalf("replicas=%v, want 1", after.Spec.Replicas)
	}
	if after.Spec.Template.Annotations[restartedAtLabel] != before.Spec.Template.Annotations[restartedAtLabel] {
		t.Fatal("scaling up changed the pod template")
	}
}

func TestEnsureRewritesTheSpecWhenTheProfileChanges(t *testing.T) {
	w := control.Workspace{ID: "demo"}
	client := fake.NewSimpleClientset()
	r := Runtime{Namespace: "agent-workspace", Client: client}
	ctx := context.Background()
	if err := r.Ensure(ctx, w, profile()); err != nil {
		t.Fatal(err)
	}
	p := profile()
	p.Image = "demo:v2"
	if err := r.Ensure(ctx, w, p); err != nil {
		t.Fatal(err)
	}
	got, _ := client.AppsV1().Deployments(r.Namespace).Get(ctx, name(w), metav1.GetOptions{})
	if got.Spec.Template.Spec.Containers[0].Image != "demo:v2" {
		t.Fatal("profile change was not applied")
	}
}

func TestBuildObjectsIsDeterministic(t *testing.T) {
	p := profile()
	p.Env = map[string]string{"A": "1", "B": "2", "C": "3", "D": "4", "E": "5"}
	w := control.Workspace{ID: "demo"}
	_, first, _, err := BuildObjects(w, p, "agent-workspace")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		_, next, _, err := BuildObjects(w, p, "agent-workspace")
		if err != nil {
			t.Fatal(err)
		}
		if next.Annotations[specHashAnnotation] != first.Annotations[specHashAnnotation] {
			t.Fatal("the same profile produced different pod templates")
		}
	}
	env := first.Spec.Template.Spec.Containers[0].Env
	for i := 1; i < len(env)-1; i++ {
		if env[i-1].Name > env[i].Name {
			t.Fatalf("env is not sorted: %v", env)
		}
	}
}

func TestValidateProfileRejectsMalformedQuantities(t *testing.T) {
	for _, mutate := range []func(*control.Profile){
		func(p *control.Profile) { p.CPU = "one" },
		func(p *control.Profile) { p.Memory = "lots" },
		func(p *control.Profile) { p.Storage = "1 Gi" },
	} {
		p := profile()
		mutate(&p)
		if err := ValidateProfile(p); !errors.Is(err, control.ErrInvalid) {
			t.Fatalf("accepted %+v: %v", p, err)
		}
		if _, _, _, err := BuildObjects(control.Workspace{ID: "demo"}, p, "agent-workspace"); err == nil {
			t.Fatal("BuildObjects accepted a malformed quantity")
		}
	}
	if err := ValidateProfile(profile()); err != nil {
		t.Fatal(err)
	}
}

// A workspace pod has to satisfy the Pod Security "restricted" profile, so that
// a namespace can enforce it. The non-root requirement is checked by the
// kubelet against the image's numeric USER, which every profile image sets.
func TestWorkspacePodMeetsThePodSecurityRestrictedProfile(t *testing.T) {
	_, d, _, err := BuildObjects(control.Workspace{ID: "a1"}, profile(), "agent-workspace")
	if err != nil {
		t.Fatal(err)
	}
	spec := d.Spec.Template.Spec
	psc := spec.SecurityContext
	if psc == nil || psc.RunAsNonRoot == nil || !*psc.RunAsNonRoot {
		t.Fatalf("pod must set runAsNonRoot: %+v", psc)
	}
	if psc.SeccompProfile == nil || psc.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Fatalf("pod must use the RuntimeDefault seccomp profile: %+v", psc.SeccompProfile)
	}
	if psc.FSGroup == nil || *psc.FSGroup != 1000 {
		t.Fatalf("fsGroup changed: %+v", psc.FSGroup)
	}
	for _, c := range spec.Containers {
		sc := c.SecurityContext
		if sc == nil || sc.AllowPrivilegeEscalation == nil || *sc.AllowPrivilegeEscalation {
			t.Fatalf("container %s must forbid privilege escalation", c.Name)
		}
		if sc.Capabilities == nil || len(sc.Capabilities.Drop) != 1 || sc.Capabilities.Drop[0] != "ALL" {
			t.Fatalf("container %s must drop ALL capabilities: %+v", c.Name, sc.Capabilities)
		}
		if sc.Privileged != nil && *sc.Privileged {
			t.Fatalf("container %s is privileged", c.Name)
		}
	}
}
