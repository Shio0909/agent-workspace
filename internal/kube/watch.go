package kube

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"agent-workspace/internal/control"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
)

var cacheSyncTimeout = 15 * time.Second

// Start watches the managed Deployments and Pods in the configured namespace
// and switches reads over to the local cache. Until Start has synced, every
// read goes straight to the API server, which keeps unit tests on the fake
// client unchanged.
//
// The resync period is 0 on purpose: periodic redelivery would enqueue every
// workspace every period, and the controller already has a periodic scheduler
// sweep as its backstop. Watch reconnects re-list automatically, so a dropped
// watch does not silently freeze the cache.
func (r *Runtime) Start(ctx context.Context) error {
	client, err := r.client()
	if err != nil {
		return err
	}
	if r.events == nil {
		r.events = make(chan string, 4096)
	}
	selector := managedByLabel + "=" + managedByValue
	factory := informers.NewSharedInformerFactoryWithOptions(client, 0,
		informers.WithNamespace(r.Namespace),
		informers.WithTweakListOptions(func(o *metav1.ListOptions) { o.LabelSelector = selector }))
	deps := factory.Apps().V1().Deployments()
	pods := factory.Core().V1().Pods()
	r.deployments, r.pods = deps.Lister(), pods.Lister()
	handler := cache.ResourceEventHandlerFuncs{
		AddFunc:    r.publish,
		UpdateFunc: func(_, obj any) { r.publish(obj) },
		DeleteFunc: r.publish,
	}
	if _, err := deps.Informer().AddEventHandler(handler); err != nil {
		return fmt.Errorf("watch deployments: %w", err)
	}
	if _, err := pods.Informer().AddEventHandler(handler); err != nil {
		return fmt.Errorf("watch pods: %w", err)
	}
	factory.Start(ctx.Done())
	syncCtx, cancel := context.WithTimeout(ctx, cacheSyncTimeout)
	defer cancel()
	if !cache.WaitForCacheSync(syncCtx.Done(), deps.Informer().HasSynced, pods.Informer().HasSynced) {
		return fmt.Errorf("timed out waiting for the workload cache to sync")
	}
	r.synced.Store(true)
	return nil
}

// WorkspaceEvents delivers the ID of a managed workspace whenever its
// Deployment or one of its Pods changes. The channel is nil until Start runs.
// Events are coalescing hints, not a log: when the buffer fills, events are
// dropped and the periodic sweep covers the gap.
func (r *Runtime) WorkspaceEvents() <-chan string { return r.events }

func (r *Runtime) publish(obj any) {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	var itemLabels map[string]string
	switch o := obj.(type) {
	case *appsv1.Deployment:
		itemLabels = o.Labels
	case *corev1.Pod:
		itemLabels = o.Labels
	default:
		return
	}
	if itemLabels[managedByLabel] != managedByValue {
		return
	}
	id := itemLabels[workspaceLabel]
	if !control.ValidName(id) {
		return
	}
	select {
	case r.events <- id:
	default:
	}
}

// getDeployment reads from the synced cache when there is one. A cache miss
// after sync means the Deployment really is gone, so no API fallback is needed;
// the create path handles the lagging-cache case (AlreadyExists) itself.
func (r *Runtime) getDeployment(ctx context.Context, name string) (*appsv1.Deployment, error) {
	if r.synced.Load() {
		return r.deployments.Deployments(r.Namespace).Get(name)
	}
	client, err := r.client()
	if err != nil {
		return nil, err
	}
	return client.AppsV1().Deployments(r.Namespace).Get(ctx, name, metav1.GetOptions{})
}

// StartupTimestamps reads the condition timestamps of the workspace's newest
// pod. The cold-start breakdown is derived from these; ok is false when the
// cache is off or the pod has not been seen yet.
func (r *Runtime) StartupTimestamps(ctx context.Context, w control.Workspace, allowDirect bool) (control.StartupTimestamps, bool) {
	selector := labels.SelectorFromSet(labels.Set{managedByLabel: managedByValue, workspaceLabel: w.ID})
	var pods []*corev1.Pod
	if r.synced.Load() && r.pods != nil {
		cached, err := r.pods.Pods(r.Namespace).List(selector)
		if err == nil {
			pods = cached
		}
	}
	ts := startupTimestamps(pods)

	// The informer may observe ready replicas before it has seen the final Pod
	// condition update. Cold starts are rare, so one direct LIST is preferable
	// to recording a partial latency breakdown.
	if allowDirect && !ts.Complete() {
		client, err := r.client()
		if err == nil {
			direct, listErr := client.CoreV1().Pods(r.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector.String()})
			if listErr == nil {
				pods = make([]*corev1.Pod, 0, len(direct.Items))
				for i := range direct.Items {
					pods = append(pods, &direct.Items[i])
				}
				ts = startupTimestamps(pods)
			}
		}
	}
	if len(pods) == 0 {
		return control.StartupTimestamps{}, false
	}
	return ts, true
}

func startupTimestamps(pods []*corev1.Pod) control.StartupTimestamps {
	if len(pods) == 0 {
		return control.StartupTimestamps{}
	}
	newest := pods[0]
	for _, p := range pods[1:] {
		if podObservationTime(p).After(podObservationTime(newest)) {
			newest = p
		}
	}
	ts := control.StartupTimestamps{PodName: newest.Name}
	for _, cond := range newest.Status.Conditions {
		switch cond.Type {
		case corev1.PodScheduled:
			ts.Scheduled = cond.LastTransitionTime.Time
		case corev1.PodReady:
			ts.Ready = cond.LastTransitionTime.Time
		}
	}
	for _, cs := range newest.Status.ContainerStatuses {
		if cs.Name == "agent" && cs.State.Running != nil {
			ts.ContainerStarted = cs.State.Running.StartedAt.Time
		}
	}
	return ts
}

// podObservationTime gives a deterministic recency key even when Kubernetes
// timestamps have second-level precision and a replacement Pod is created in
// the same second as the terminating one.
func podObservationTime(pod *corev1.Pod) time.Time {
	newest := pod.CreationTimestamp.Time
	if pod.Status.StartTime != nil && pod.Status.StartTime.After(newest) {
		newest = pod.Status.StartTime.Time
	}
	for _, cond := range pod.Status.Conditions {
		if cond.LastTransitionTime.Time.After(newest) {
			newest = cond.LastTransitionTime.Time
		}
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.State.Running != nil && status.State.Running.StartedAt.Time.After(newest) {
			newest = status.State.Running.StartedAt.Time
		}
	}
	return newest
}

// APIMetrics counts requests this process sends to the API server, by verb and
// resource. It exists so benchmarks can show what the cache saves: with the
// informer on, steady-state scheduler rounds should produce no reads at all.
type APIMetrics struct {
	mu     sync.Mutex
	counts map[apiRequestKey]*atomic.Int64
}

type apiRequestKey struct{ verb, resource string }

func NewAPIMetrics() *APIMetrics { return &APIMetrics{counts: map[apiRequestKey]*atomic.Int64{}} }

// Wrap counts every request on its way to the API server.
func (m *APIMetrics) Wrap(next http.RoundTripper) http.RoundTripper {
	return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		verb, resource := classifyRequest(req)
		m.mu.Lock()
		c := m.counts[apiRequestKey{verb, resource}]
		if c == nil {
			c = &atomic.Int64{}
			m.counts[apiRequestKey{verb, resource}] = c
		}
		m.mu.Unlock()
		c.Add(1)
		return next.RoundTrip(req)
	})
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// WriteMetrics lets the control-plane metrics endpoint expose API request
// counters without making the control package depend on Kubernetes.
func (r *Runtime) WriteMetrics(w io.Writer) error {
	if r.APIRequests == nil {
		return nil
	}
	return r.APIRequests.WriteMetrics(w)
}

// classifyRequest reduces an API path to (verb, resource) so metric labels
// stay bounded: /apis/apps/v1/namespaces/x/deployments/y -> ("get",
// "deployments"), and the same path without the trailing name -> ("list",
// "deployments").
func classifyRequest(req *http.Request) (verb, resource string) {
	parts := strings.Split(strings.Trim(req.URL.Path, "/"), "/")
	// The resource follows "namespaces/<ns>", or the apiVersion segment.
	idx := -1
	for i, p := range parts {
		if p == "namespaces" && i+2 < len(parts) {
			idx = i + 2
			break
		}
	}
	if idx == -1 {
		for i := 1; i < len(parts); i++ {
			if isVersion(parts[i]) && i+1 < len(parts) {
				idx = i + 1
				break
			}
		}
	}
	named := false
	if idx >= 0 {
		resource = parts[idx]
		named = idx+1 < len(parts)
	} else {
		resource = "discovery"
	}
	verb = strings.ToLower(req.Method)
	if req.Method == http.MethodGet {
		switch {
		case req.URL.Query().Get("watch") == "true":
			return "watch", resource
		case idx >= 0 && !named:
			return "list", resource
		}
	}
	return verb, resource
}

func isVersion(s string) bool {
	if len(s) < 2 || s[0] != 'v' {
		return false
	}
	for _, r := range s[1:] {
		if r != '.' && (r < '0' || r > '9') && (r < 'a' || r > 'z') {
			return false
		}
	}
	return s[1] >= '0' && s[1] <= '9'
}

// WriteMetrics appends the counters in Prometheus text format.
func (m *APIMetrics) WriteMetrics(w io.Writer) error {
	m.mu.Lock()
	rows := make([]apiRequestKey, 0, len(m.counts))
	values := make(map[apiRequestKey]int64, len(m.counts))
	for k, c := range m.counts {
		rows = append(rows, k)
		values[k] = c.Load()
	}
	m.mu.Unlock()
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].resource != rows[j].resource {
			return rows[i].resource < rows[j].resource
		}
		return rows[i].verb < rows[j].verb
	})
	var b strings.Builder
	b.WriteString("# HELP nc_kube_api_requests_total Requests sent to the Kubernetes API server.\n")
	b.WriteString("# TYPE nc_kube_api_requests_total counter\n")
	for _, k := range rows {
		fmt.Fprintf(&b, "nc_kube_api_requests_total{verb=%q,resource=%q} %d\n", k.verb, k.resource, values[k])
	}
	_, err := io.WriteString(w, b.String())
	return err
}
