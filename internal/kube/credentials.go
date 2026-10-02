package kube

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"

	"agent-workspace/internal/control"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// VersionKey is the file the controller writes next to the credentials. The
// agent reads it to report which version it has loaded.
const VersionKey = ".version"

const credentialVersionAnnotation = "agent-workspace/credential-version"

func credentialSecretName(w control.Workspace) string { return name(w) + "-cred" }

var _ control.CredentialStore = (*Runtime)(nil)

// SetCredentials writes the workspace's credential Secret. The pod template
// only references the Secret by name, so a rotation never changes the
// Deployment and never restarts the pod: the kubelet refreshes the mounted
// files in place.
func (r *Runtime) SetCredentials(ctx context.Context, w control.Workspace, version int, values map[string]string) error {
	if !control.ValidName(w.ID) {
		return control.ErrInvalid
	}
	client, err := r.client()
	if err != nil {
		return err
	}
	data := make(map[string][]byte, len(values)+1)
	for k, v := range values {
		data[k] = []byte(v)
	}
	data[VersionKey] = []byte(strconv.Itoa(version))
	secrets := client.CoreV1().Secrets(r.Namespace)
	current, err := secrets.Get(ctx, credentialSecretName(w), metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		_, err = secrets.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: credentialSecretName(w), Namespace: r.Namespace, Labels: workspaceLabels(w)},
			Type:       corev1.SecretTypeOpaque,
			Data:       data,
		}, metav1.CreateOptions{})
		if err != nil {
			return err
		}
		// A pod that started before the first key was set has an empty
		// optional volume; it needs the same nudge as a rotation.
		r.nudgePods(ctx, w, version)
		return nil
	case err != nil:
		return err
	case !owns(current.Labels, w):
		return fmt.Errorf("refusing unmanaged secret %s", credentialSecretName(w))
	}
	// Replace the whole data map: a key dropped from the request must vanish
	// from the pod too, not linger as a stale credential.
	current.Data = data
	if _, err = secrets.Update(ctx, current, metav1.UpdateOptions{}); err != nil {
		return err
	}
	r.nudgePods(ctx, w, version)
	return nil
}

// nudgePods annotates the workspace's pods with the new version. The pod
// template is untouched, so nothing rolls; the point is that a pod update makes
// the kubelet re-sync its volumes now instead of at the next periodic sync.
// It is best effort: the Secret is already written, and the kubelet will
// converge on its own if this fails.
func (r *Runtime) nudgePods(ctx context.Context, w control.Workspace, version int) {
	client, err := r.client()
	if err != nil {
		return
	}
	pods, err := client.CoreV1().Pods(r.Namespace).List(ctx, metav1.ListOptions{LabelSelector: workspaceLabel + "=" + w.ID})
	if err != nil {
		slog.Warn("list pods to nudge after credential update", "workspace", w.ID, "error", err)
		return
	}
	patch := fmt.Sprintf(`{"metadata":{"annotations":{%q:%q}}}`, credentialVersionAnnotation, strconv.Itoa(version))
	for _, p := range pods.Items {
		if !owns(p.Labels, w) {
			continue
		}
		if _, err := client.CoreV1().Pods(r.Namespace).Patch(ctx, p.Name, types.MergePatchType, []byte(patch), metav1.PatchOptions{}); err != nil {
			slog.Warn("nudge pod after credential update", "workspace", w.ID, "pod", p.Name, "error", err)
		}
	}
}

// ClearCredentials deletes the Secret. The pod's optional volume then becomes
// empty, which the agent sees as "no credential".
func (r *Runtime) ClearCredentials(ctx context.Context, w control.Workspace) error {
	client, err := r.client()
	if err != nil {
		return err
	}
	secrets := client.CoreV1().Secrets(r.Namespace)
	current, err := secrets.Get(ctx, credentialSecretName(w), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !owns(current.Labels, w) {
		return fmt.Errorf("refusing unmanaged secret %s", credentialSecretName(w))
	}
	if err := secrets.Delete(ctx, credentialSecretName(w), metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// credentialVolume mounts the Secret read-only. optional lets the pod start
// before any credential exists; defaultMode makes the files readable by the
// pod's fsGroup but not by other users.
func credentialVolume(w control.Workspace) corev1.Volume {
	mode := int32(0440)
	return corev1.Volume{
		Name: "credentials",
		VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
			SecretName:  credentialSecretName(w),
			Optional:    boolPtr(true),
			DefaultMode: &mode,
		}},
	}
}

func boolPtr(b bool) *bool { return &b }
