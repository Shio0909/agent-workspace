package kube

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"agent-workspace/internal/control"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	corelisters "k8s.io/client-go/listers/core/v1"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
)

func TestRuntimeStartUsesInformerCache(t *testing.T) {
	w := control.Workspace{ID: "demo"}
	deployment := ownedDeployment(w, 1, 1)
	scheduled := time.Unix(100, 0).UTC()
	containerStarted := scheduled.Add(2 * time.Second)
	ready := containerStarted.Add(3 * time.Second)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "demo-pod", Namespace: "agent-workspace", Labels: workspaceLabels(w),
			CreationTimestamp: metav1.NewTime(scheduled),
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodScheduled, LastTransitionTime: metav1.NewTime(scheduled)},
				{Type: corev1.PodReady, LastTransitionTime: metav1.NewTime(ready)},
			},
			ContainerStatuses: []corev1.ContainerStatus{
				{Name: "agent", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(containerStarted)}}},
			},
		},
	}
	client := fake.NewSimpleClientset(deployment, pod)
	r := Runtime{Namespace: "agent-workspace", Client: client}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if !r.synced.Load() {
		t.Fatal("informer did not report a synced cache")
	}

	// A synced runtime must not consult the API client for reads. Removing the
	// client makes any accidental fallback fail loudly.
	r.Client = nil
	got, err := r.getDeployment(ctx, name(w))
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != deployment.Name {
		t.Fatalf("cached deployment=%q, want %q", got.Name, deployment.Name)
	}
	ts, ok := r.StartupTimestamps(ctx, w, true)
	if !ok {
		t.Fatal("startup timestamps were not read from the pod cache")
	}
	if ts.PodName != pod.Name || !ts.Scheduled.Equal(scheduled) || !ts.ContainerStarted.Equal(containerStarted) || !ts.Ready.Equal(ready) {
		t.Fatalf("unexpected startup timestamps: %+v", ts)
	}

	select {
	case id := <-r.WorkspaceEvents():
		if id != w.ID {
			t.Fatalf("initial informer event=%q, want %q", id, w.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("initial informer event was not published")
	}
}

func TestRuntimeCacheMissDoesNotFallbackAfterSync(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	r := Runtime{Namespace: "agent-workspace", Client: fake.NewSimpleClientset()}
	if err := r.Start(ctx); err != nil {
		t.Fatal(err)
	}
	r.Client = nil
	_, err := r.getDeployment(ctx, "nc-missing")
	if !apierrors.IsNotFound(err) {
		t.Fatalf("cache miss error=%v, want NotFound", err)
	}
	if _, ok := r.StartupTimestamps(ctx, control.Workspace{ID: "missing"}, false); ok {
		t.Fatal("cache miss produced startup timestamps")
	}
}

func TestRuntimeStartTimesOutWhenCacheCannotSync(t *testing.T) {
	oldTimeout := cacheSyncTimeout
	cacheSyncTimeout = 50 * time.Millisecond
	t.Cleanup(func() { cacheSyncTimeout = oldTimeout })

	client := fake.NewSimpleClientset()
	client.PrependReactor("list", "pods", func(clienttesting.Action) (bool, k8sruntime.Object, error) {
		return true, nil, errors.New("pods forbidden")
	})
	r := Runtime{Namespace: "agent-workspace", Client: client}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := time.Now()
	if err := r.Start(ctx); err == nil {
		t.Fatal("Start succeeded with a permanently failing informer")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("Start blocked for %s instead of honoring the cache sync timeout", elapsed)
	}
	if r.synced.Load() {
		t.Fatal("failed cache sync marked the runtime as synced")
	}
}

func TestStartupTimestampsFallbackToAPIServer(t *testing.T) {
	w := control.Workspace{ID: "demo"}
	scheduled := time.Unix(200, 0).UTC()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-pod", Namespace: "agent-workspace", Labels: workspaceLabels(w)},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodScheduled, LastTransitionTime: metav1.NewTime(scheduled)},
				{Type: corev1.PodReady, LastTransitionTime: metav1.NewTime(scheduled.Add(2 * time.Second))},
			},
			ContainerStatuses: []corev1.ContainerStatus{
				{Name: "agent", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(scheduled.Add(time.Second))}}},
			},
		},
	}
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	r := Runtime{
		Namespace: "agent-workspace",
		Client:    fake.NewSimpleClientset(pod),
		pods:      corelisters.NewPodLister(indexer),
	}
	r.synced.Store(true)
	ts, ok := r.StartupTimestamps(context.Background(), w, true)
	if !ok || !ts.Complete() {
		t.Fatalf("fallback returned incomplete timestamps: %+v ok=%v", ts, ok)
	}
}

func TestStartupTimestampsPrefersReplacementPodInSameSecond(t *testing.T) {
	created := time.Unix(300, 0).UTC()
	started := metav1.NewTime(created.Add(time.Second))
	oldPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "old", CreationTimestamp: metav1.NewTime(created)},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{
			{Type: corev1.PodReady, LastTransitionTime: metav1.NewTime(created.Add(time.Second))},
		}},
	}
	newPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "new", CreationTimestamp: metav1.NewTime(created)},
		Status: corev1.PodStatus{
			StartTime: &started,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodScheduled, LastTransitionTime: metav1.NewTime(created.Add(time.Second))},
				{Type: corev1.PodReady, LastTransitionTime: metav1.NewTime(created.Add(2 * time.Second))},
			},
			ContainerStatuses: []corev1.ContainerStatus{
				{Name: "agent", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(created.Add(1500 * time.Millisecond))}}},
			},
		},
	}
	ts := startupTimestamps([]*corev1.Pod{oldPod, newPod})
	if ts.PodName != "new" || !ts.Complete() {
		t.Fatalf("selected stale replacement pod: %+v", ts)
	}
}

func TestRuntimePublishesOnlyManagedWorkspaceEvents(t *testing.T) {
	r := Runtime{events: make(chan string, 8)}
	managed := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Labels: workspaceLabels(control.Workspace{ID: "demo"})}}
	r.publish(managed)
	if id := <-r.events; id != "demo" {
		t.Fatalf("managed event=%q, want demo", id)
	}
	r.publish(&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{managedByLabel: "someone-else"}}})
	r.publish(&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{managedByLabel: managedByValue, workspaceLabel: "INVALID"}}})
	if got := len(r.events); got != 0 {
		t.Fatalf("unmanaged or invalid object produced %d events", got)
	}
	r.publish(cache.DeletedFinalStateUnknown{Obj: managed})
	if id := <-r.events; id != "demo" {
		t.Fatalf("tombstone event=%q, want demo", id)
	}
}

func TestClassifyRequest(t *testing.T) {
	for _, tc := range []struct {
		method, target, verb, resource string
	}{
		{http.MethodGet, "https://cluster/apis/apps/v1/namespaces/ns/deployments/demo", "get", "deployments"},
		{http.MethodGet, "https://cluster/apis/apps/v1/namespaces/ns/deployments", "list", "deployments"},
		{http.MethodGet, "https://cluster/api/v1/namespaces/ns/pods?watch=true", "watch", "pods"},
		{http.MethodPost, "https://cluster/apis/apps/v1/namespaces/ns/deployments", "post", "deployments"},
		{http.MethodGet, "https://cluster/api/v1/namespaces/ns/pods/p/status", "get", "pods"},
		{http.MethodGet, "https://cluster/version", "get", "discovery"},
	} {
		req, err := http.NewRequest(tc.method, tc.target, nil)
		if err != nil {
			t.Fatal(err)
		}
		verb, resource := classifyRequest(req)
		if verb != tc.verb || resource != tc.resource {
			t.Fatalf("%s %s => %s/%s, want %s/%s", tc.method, tc.target, verb, resource, tc.verb, tc.resource)
		}
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestAPIMetricsWrapAndRender(t *testing.T) {
	var calls int
	var mu sync.Mutex
	next := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		mu.Lock()
		calls++
		mu.Unlock()
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("{}")), Header: make(http.Header)}, nil
	})
	m := NewAPIMetrics()
	wrapped := m.Wrap(next)
	for _, target := range []string{
		"https://cluster/apis/apps/v1/namespaces/ns/deployments/demo",
		"https://cluster/apis/apps/v1/namespaces/ns/deployments/demo",
		"https://cluster/api/v1/namespaces/ns/pods?watch=true",
		"https://cluster/version",
	} {
		req, err := http.NewRequest(http.MethodGet, target, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := wrapped.RoundTrip(req); err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	gotCalls := calls
	mu.Unlock()
	if gotCalls != 4 {
		t.Fatalf("wrapped calls=%d, want 4", gotCalls)
	}

	var b strings.Builder
	if err := m.WriteMetrics(&b); err != nil {
		t.Fatal(err)
	}
	text := b.String()
	for _, want := range []string{
		`nc_kube_api_requests_total{verb="get",resource="deployments"} 2`,
		`nc_kube_api_requests_total{verb="watch",resource="pods"} 1`,
		`nc_kube_api_requests_total{verb="get",resource="discovery"} 1`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("API metrics missing %q:\n%s", want, text)
		}
	}
}

func TestAPIMetricsWrapPreservesTransportError(t *testing.T) {
	want := errors.New("transport failed")
	m := NewAPIMetrics()
	wrapped := m.Wrap(roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, want }))
	req, err := http.NewRequest(http.MethodGet, "https://cluster/version", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wrapped.RoundTrip(req); !errors.Is(err, want) {
		t.Fatalf("transport error=%v, want %v", err, want)
	}
}

func TestRuntimeExportsAPIMetrics(t *testing.T) {
	var exporter control.MetricsExporter = &Runtime{APIRequests: NewAPIMetrics()}
	var b strings.Builder
	if err := exporter.WriteMetrics(&b); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(b.String(), "# TYPE nc_kube_api_requests_total counter") {
		t.Fatalf("API metrics were not exported:\n%s", b.String())
	}
	var empty control.MetricsExporter = &Runtime{}
	if err := empty.WriteMetrics(&b); err != nil {
		t.Fatal(err)
	}
}
