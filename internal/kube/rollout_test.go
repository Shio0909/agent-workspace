package kube

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
)

func rollout(replicas int32, mutate func(*appsv1.Deployment)) *appsv1.Deployment {
	d := &appsv1.Deployment{
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To(replicas),
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Image: "reg/agent:v2"}}}},
		},
		Status: appsv1.DeploymentStatus{ObservedGeneration: 3, Replicas: replicas, UpdatedReplicas: replicas, ReadyReplicas: replicas},
	}
	d.Generation = 3
	if mutate != nil {
		mutate(d)
	}
	return d
}

func TestRolledOutImageIsReportedOnlyWhenTheRolloutIsComplete(t *testing.T) {
	for name, tc := range map[string]struct {
		d    *appsv1.Deployment
		want string
	}{
		"complete":               {rollout(1, nil), "reg/agent:v2"},
		"spec newer than status": {rollout(1, func(d *appsv1.Deployment) { d.Generation = 4 }), ""},
		"new pod not ready":      {rollout(1, func(d *appsv1.Deployment) { d.Status.ReadyReplicas = 0 }), ""},
		"old pod still counted":  {rollout(1, func(d *appsv1.Deployment) { d.Status.Replicas = 2 }), ""},
		"nothing updated yet":    {rollout(1, func(d *appsv1.Deployment) { d.Status.UpdatedReplicas = 0 }), ""},
		"scaled to zero":         {rollout(0, nil), ""},
		"no replicas field":      {rollout(1, func(d *appsv1.Deployment) { d.Spec.Replicas = nil }), ""},
		"no containers":          {rollout(1, func(d *appsv1.Deployment) { d.Spec.Template.Spec.Containers = nil }), ""},
	} {
		if got := rolledOutImage(tc.d); got != tc.want {
			t.Errorf("%s: got %q want %q", name, got, tc.want)
		}
	}
}
