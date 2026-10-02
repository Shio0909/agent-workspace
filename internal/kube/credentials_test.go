package kube

import (
	"context"
	"strings"
	"testing"

	"agent-workspace/internal/control"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func credProfile() control.Profile {
	p := profile()
	p.CredentialPath = "/var/run/agent-credentials"
	return p
}

func getSecret(t *testing.T, r *Runtime, w control.Workspace) *corev1.Secret {
	t.Helper()
	s, err := r.Client.CoreV1().Secrets(r.Namespace).Get(context.Background(), credentialSecretName(w), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestSetCredentialsCreatesThenReplacesTheWholeSet(t *testing.T) {
	w := control.Workspace{ID: "a1"}
	r := &Runtime{Namespace: "agent-workspace", Client: fake.NewSimpleClientset()}
	ctx := context.Background()
	if err := r.SetCredentials(ctx, w, 1, map[string]string{"llm_api_key": "k1", "extra": "e"}); err != nil {
		t.Fatal(err)
	}
	s := getSecret(t, r, w)
	if string(s.Data["llm_api_key"]) != "k1" || string(s.Data[VersionKey]) != "1" || !owns(s.Labels, w) {
		t.Fatalf("secret=%+v", s)
	}
	if err := r.SetCredentials(ctx, w, 2, map[string]string{"llm_api_key": "k2"}); err != nil {
		t.Fatal(err)
	}
	s = getSecret(t, r, w)
	if string(s.Data["llm_api_key"]) != "k2" || string(s.Data[VersionKey]) != "2" {
		t.Fatalf("rotation not applied: %+v", s.Data)
	}
	if _, stale := s.Data["extra"]; stale {
		t.Fatal("a key dropped from the request is still mounted")
	}
}

func TestSetCredentialsRefusesAnUnmanagedSecret(t *testing.T) {
	w := control.Workspace{ID: "a1"}
	foreign := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: credentialSecretName(w), Namespace: "agent-workspace"}}
	r := &Runtime{Namespace: "agent-workspace", Client: fake.NewSimpleClientset(foreign)}
	if err := r.SetCredentials(context.Background(), w, 1, map[string]string{"k": "v"}); err == nil || !strings.Contains(err.Error(), "unmanaged") {
		t.Fatalf("err=%v", err)
	}
	if err := r.ClearCredentials(context.Background(), w); err == nil {
		t.Fatal("cleared a secret this controller does not own")
	}
	if err := r.Delete(context.Background(), w); err == nil {
		t.Fatal("hard delete proceeded past an unmanaged secret")
	}
}

func TestClearCredentialsIsIdempotent(t *testing.T) {
	w := control.Workspace{ID: "a1"}
	r := &Runtime{Namespace: "agent-workspace", Client: fake.NewSimpleClientset()}
	ctx := context.Background()
	if err := r.ClearCredentials(ctx, w); err != nil {
		t.Fatalf("clearing nothing: %v", err)
	}
	if err := r.SetCredentials(ctx, w, 1, map[string]string{"k": "v"}); err != nil {
		t.Fatal(err)
	}
	if err := r.ClearCredentials(ctx, w); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Client.CoreV1().Secrets(r.Namespace).Get(ctx, credentialSecretName(w), metav1.GetOptions{}); err == nil {
		t.Fatal("secret still exists")
	}
}

func TestCredentialVolumeIsOptionalReadOnlyAndRotationSafe(t *testing.T) {
	w := control.Workspace{ID: "a1"}
	_, d, _, err := BuildObjects(w, credProfile(), "agent-workspace")
	if err != nil {
		t.Fatal(err)
	}
	var vol *corev1.Volume
	for i := range d.Spec.Template.Spec.Volumes {
		if d.Spec.Template.Spec.Volumes[i].Name == "credentials" {
			vol = &d.Spec.Template.Spec.Volumes[i]
		}
	}
	if vol == nil || vol.Secret == nil || vol.Secret.SecretName != "nc-a1-cred" {
		t.Fatalf("credentials volume missing: %+v", d.Spec.Template.Spec.Volumes)
	}
	if vol.Secret.Optional == nil || !*vol.Secret.Optional {
		t.Fatal("volume must be optional, or the pod cannot start before its first key is set")
	}
	mounts := d.Spec.Template.Spec.Containers[0].VolumeMounts
	found := false
	for _, m := range mounts {
		if m.Name == "credentials" {
			found = true
			if !m.ReadOnly || m.MountPath != "/var/run/agent-credentials" || m.SubPath != "" {
				t.Fatalf("mount=%+v: a subPath mount never receives updated files", m)
			}
		}
	}
	if !found {
		t.Fatal("credentials not mounted")
	}

	// The pod template is built from the profile alone. If it depended on the
	// credential version, every rotation would roll the pod.
	_, d2, _, _ := BuildObjects(control.Workspace{ID: "a1", CredentialVersion: 7}, credProfile(), "agent-workspace")
	if d.Annotations[specHashAnnotation] != d2.Annotations[specHashAnnotation] {
		t.Fatal("credential version leaks into the pod template")
	}

	// A profile without credential_path is unchanged by this feature.
	_, plain, _, _ := BuildObjects(w, profile(), "agent-workspace")
	for _, v := range plain.Spec.Template.Spec.Volumes {
		if v.Name == "credentials" {
			t.Fatal("credentials volume added to a profile that did not ask for it")
		}
	}
}

func TestStopKeepsCredentialsAndDeleteRemovesThem(t *testing.T) {
	w := control.Workspace{ID: "a1"}
	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name(w), Namespace: "agent-workspace", Labels: workspaceLabels(w)}}
	r := &Runtime{Namespace: "agent-workspace", Client: fake.NewSimpleClientset(dep)}
	ctx := context.Background()
	if err := r.SetCredentials(ctx, w, 1, map[string]string{"k": "v"}); err != nil {
		t.Fatal(err)
	}
	if err := r.Stop(ctx, w); err != nil {
		t.Fatal(err)
	}
	getSecret(t, r, w) // still there after stop
	if err := r.Delete(ctx, w); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Client.CoreV1().Secrets(r.Namespace).Get(ctx, credentialSecretName(w), metav1.GetOptions{}); err == nil {
		t.Fatal("hard delete left the credential secret behind")
	}
}

func TestSetCredentialsNudgesOnlyThisWorkspacesPods(t *testing.T) {
	w := control.Workspace{ID: "a1"}
	mine := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "nc-a1-x", Namespace: "agent-workspace", Labels: workspaceLabels(w)}}
	other := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "nc-b1-x", Namespace: "agent-workspace", Labels: workspaceLabels(control.Workspace{ID: "b1"})}}
	r := &Runtime{Namespace: "agent-workspace", Client: fake.NewSimpleClientset(mine, other)}
	ctx := context.Background()
	if err := r.SetCredentials(ctx, w, 3, map[string]string{"k": "v"}); err != nil {
		t.Fatal(err)
	}
	got, _ := r.Client.CoreV1().Pods("agent-workspace").Get(ctx, "nc-a1-x", metav1.GetOptions{})
	if got.Annotations[credentialVersionAnnotation] != "3" {
		t.Fatalf("pod not nudged: %v", got.Annotations)
	}
	untouched, _ := r.Client.CoreV1().Pods("agent-workspace").Get(ctx, "nc-b1-x", metav1.GetOptions{})
	if _, ok := untouched.Annotations[credentialVersionAnnotation]; ok {
		t.Fatal("another workspace's pod was nudged")
	}
}
