package kube

// These tests keep the original quantitative property: the scale/suspend/restart
// paths must never delete a PVC, and hard delete must scale the workload before
// removing storage.

import (
	"context"
	"testing"

	"agent-workspace/internal/control"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

type actionRecord struct{ verb, resource, subresource string }

func testObjects(w control.Workspace, replicas int32) (*appsv1.Deployment, *corev1.Service, *corev1.PersistentVolumeClaim) {
	return ownedDeployment(w, replicas, 0),
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name(w), Namespace: "agent-workspace", Labels: labels(w)}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: name(w), Namespace: "agent-workspace", Labels: labels(w)}}
}

func TestQuantStopAndRestartNeverDeletePVC(t *testing.T) {
	for _, tc := range []struct {
		name  string
		stage string
	}{
		{"挂起（suspended 阶段复用 Stop）", "stop"},
		{"重启（换工作负载不换存储）", "restart"},
		{"空闲缩容（stopped 阶段复用 Stop）", "stop-idle"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := control.Workspace{ID: "demo"}
			deployment, service, pvc := testObjects(w, 1)
			client := fake.NewSimpleClientset(deployment, service, pvc)
			r := Runtime{Namespace: "agent-workspace", Client: client}
			var err error
			if tc.stage == "restart" {
				err = r.Restart(context.Background(), w)
			} else {
				err = r.Stop(context.Background(), w)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := client.CoreV1().PersistentVolumeClaims(r.Namespace).Get(context.Background(), name(w), metav1.GetOptions{}); err != nil {
				t.Fatalf("PVC was removed on %s: %v", tc.stage, err)
			}
			if _, err := client.CoreV1().Services(r.Namespace).Get(context.Background(), name(w), metav1.GetOptions{}); err != nil {
				t.Fatalf("Service was removed on %s: %v", tc.stage, err)
			}
		})
	}
}

func TestQuantOnlyHardDeleteRemovesPVC(t *testing.T) {
	w := control.Workspace{ID: "demo"}
	deployment, service, pvc := testObjects(w, 1)
	client := fake.NewSimpleClientset(deployment, service, pvc)

	var actions []actionRecord
	client.PrependReactor("*", "*", func(action k8stesting.Action) (bool, runtime.Object, error) {
		actions = append(actions, actionRecord{action.GetVerb(), action.GetResource().Resource, action.GetSubresource()})
		return false, nil, nil
	})

	r := Runtime{Namespace: "agent-workspace", Client: client}
	if err := r.Delete(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	for _, resource := range []string{"deployments", "services", "persistentvolumeclaims"} {
		var err error
		switch resource {
		case "deployments":
			_, err = client.AppsV1().Deployments(r.Namespace).Get(context.Background(), name(w), metav1.GetOptions{})
		case "services":
			_, err = client.CoreV1().Services(r.Namespace).Get(context.Background(), name(w), metav1.GetOptions{})
		case "persistentvolumeclaims":
			_, err = client.CoreV1().PersistentVolumeClaims(r.Namespace).Get(context.Background(), name(w), metav1.GetOptions{})
		}
		if !apierrors.IsNotFound(err) {
			t.Fatalf("%s still exists: %v", resource, err)
		}
	}

	scaleAt, pvcDeleteAt := -1, -1
	for i, action := range actions {
		if action.verb == "update" && action.resource == "deployments" && scaleAt < 0 {
			scaleAt = i
		}
		if action.verb == "delete" && action.resource == "persistentvolumeclaims" && pvcDeleteAt < 0 {
			pvcDeleteAt = i
		}
	}
	if scaleAt < 0 || pvcDeleteAt < 0 || scaleAt > pvcDeleteAt {
		t.Fatalf("delete order is wrong: scale=%d pvc=%d actions=%v", scaleAt, pvcDeleteAt, actionStrings(actions))
	}
}

func actionStrings(actions []actionRecord) []string {
	out := make([]string, 0, len(actions))
	for _, action := range actions {
		value := action.verb + " " + action.resource
		if action.subresource != "" {
			value += "/" + action.subresource
		}
		out = append(out, value)
	}
	return out
}
