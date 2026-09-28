package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"agent-workspace/internal/control"
	"agent-workspace/internal/httpapi"
	"agent-workspace/internal/kube"
)

const (
	// shutdownWait 是 HTTP 收尾的上限，drainWait 是等在途对账结束的上限。
	// 两者回答的是"等多久"，不是"能不能做完"：对账是幂等的，没跑完的部分
	// 下次启动会重做，所以到点放弃是安全的。
	shutdownWait = 10 * time.Second
	drainWait    = 10 * time.Second
	// minTokenLength 拒绝明显过短的共享令牌。
	minTokenLength = 16
)

type config struct {
	listen       string
	data         string
	profiles     string
	namespace    string
	kubeContext  string
	token        string
	idle         time.Duration
	grace        time.Duration
	startupGrace time.Duration
	interval     time.Duration
	roundTimeout time.Duration
	batch        int
	concurrency  int
}

func main() {
	if err := run(); err != nil {
		slog.Error("agent-workspace stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := parseConfig(os.Args[1:], os.Getenv)
	if err != nil {
		return err
	}
	profiles, err := loadProfiles(cfg.profiles)
	if err != nil {
		return err
	}
	store, err := control.OpenStore(cfg.data)
	if err != nil {
		return err
	}
	defer store.Close()
	runtime, err := kube.NewRuntime(cfg.namespace, cfg.kubeContext)
	if err != nil {
		return err
	}
	c := control.New(store, runtime, profiles, cfg.idle)
	c.GracePeriod, c.StartupGrace = cfg.grace, cfg.startupGrace
	scheduler := control.NewScheduler(c)
	scheduler.Interval, scheduler.RoundTimeout = cfg.interval, cfg.roundTimeout
	scheduler.BatchSize, scheduler.Concurrency = cfg.batch, cfg.concurrency

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		scheduler.Run(ctx)
	}()
	api := &httpapi.Server{Controller: c, Token: cfg.token, Actor: "api"}
	server := &http.Server{Addr: cfg.listen, Handler: api.Handler(), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second}
	serverErr := make(chan error, 1)
	go func() { serverErr <- server.ListenAndServe() }()
	slog.Info("agent-workspace listening", "address", cfg.listen, "namespace", cfg.namespace, "interval", cfg.interval)
	select {
	case <-ctx.Done():
	case err = <-serverErr:
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
	}
	// 优雅停机：先停调度循环（不再开新一轮）和 HTTP 入口，再等在途对账收尾。
	cancel()
	shutdownCtx, stop := context.WithTimeout(context.Background(), shutdownWait)
	defer stop()
	if shutdownErr := server.Shutdown(shutdownCtx); shutdownErr != nil {
		_ = server.Close()
	}
	drainCtx, stopDrain := context.WithTimeout(context.Background(), drainWait)
	defer stopDrain()
	select {
	case <-loopDone:
	case <-drainCtx.Done():
		slog.Warn("scheduler did not drain before the deadline; unfinished reconciles are retried on the next start")
	}
	return err
}

func parseConfig(args []string, getenv func(string) string) (config, error) {
	cfg := config{}
	fs := flag.NewFlagSet("agent-workspace", flag.ContinueOnError)
	fs.StringVar(&cfg.listen, "listen", "127.0.0.1:8090", "HTTP listen address")
	fs.StringVar(&cfg.data, "data", "data", "exclusive controller data directory")
	fs.StringVar(&cfg.profiles, "profiles", "configs/profiles.json", "runtime profile file")
	fs.StringVar(&cfg.namespace, "namespace", "agent-workspace", "dedicated Kubernetes namespace")
	fs.StringVar(&cfg.kubeContext, "context", "", "kubeconfig context; empty uses in-cluster/current context")
	fs.DurationVar(&cfg.idle, "idle", 15*time.Minute, "idle timeout and controller restart grace")
	fs.DurationVar(&cfg.grace, "grace", 24*time.Hour, "suspension grace period before storage is deleted")
	fs.DurationVar(&cfg.startupGrace, "startup-grace", time.Minute, "reclamation silence after controller start")
	fs.DurationVar(&cfg.interval, "reconcile", 5*time.Second, "scheduler interval")
	fs.DurationVar(&cfg.roundTimeout, "round-timeout", time.Minute, "upper bound for a single scheduler round")
	fs.IntVar(&cfg.batch, "batch", 64, "workspaces loaded per scan batch")
	fs.IntVar(&cfg.concurrency, "sweep-concurrency", 4, "workspaces reconciled concurrently")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	cfg.token = getenv("AGENT_WORKSPACE_TOKEN")
	return cfg, cfg.validate()
}

// validate 把非法配置挡在启动阶段。带病运行的代价是几小时后才暴露的坏行为，
// 而一条启动失败是立刻可见、可以马上修的。
func (c config) validate() error {
	switch {
	case c.listen == "":
		return errors.New("-listen must not be empty")
	case c.data == "":
		return errors.New("-data must not be empty")
	case c.profiles == "":
		return errors.New("-profiles must not be empty")
	case !control.ValidName(c.namespace):
		return fmt.Errorf("-namespace %q must be a lowercase DNS label", c.namespace)
	case len(c.token) < minTokenLength:
		return fmt.Errorf("AGENT_WORKSPACE_TOKEN must contain at least %d characters", minTokenLength)
	case c.idle <= 0:
		return errors.New("-idle must be positive")
	case c.grace <= 0:
		return errors.New("-grace must be positive")
	case c.startupGrace < 0:
		return errors.New("-startup-grace must not be negative")
	case c.interval <= 0:
		return errors.New("-reconcile must be positive")
	case c.roundTimeout <= 0:
		return errors.New("-round-timeout must be positive")
	case c.batch < 1 || c.batch > 10000:
		return errors.New("-batch must be in [1, 10000]")
	case c.concurrency < 1 || c.concurrency > 64:
		return errors.New("-sweep-concurrency must be in [1, 64]")
	}
	return nil
}

// loadProfiles 拒绝未知字段：profile 决定镜像、挂载和资源，一个拼错的键
// 静默生效比启动失败危险得多。
func loadProfiles(path string) (map[string]control.Profile, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	var profiles map[string]control.Profile
	if err := dec.Decode(&profiles); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if len(profiles) == 0 {
		return nil, errors.New("no runtime profiles configured")
	}
	for name, p := range profiles {
		if !control.ValidName(name) {
			return nil, fmt.Errorf("invalid profile name %q", name)
		}
		if err := kube.ValidateProfile(p); err != nil {
			return nil, fmt.Errorf("profile %s: %w", name, err)
		}
	}
	return profiles, nil
}
