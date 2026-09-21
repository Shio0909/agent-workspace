package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func tokenFunc(string) string { return strings.Repeat("k", 16) }

func TestParseConfigAppliesOverrides(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-listen", "0.0.0.0:9000", "-data", "/data", "-namespace", "workspaces",
		"-grace", "48h", "-startup-grace", "30s", "-reconcile", "2s",
		"-round-timeout", "20s", "-batch", "8", "-sweep-concurrency", "2",
	}, tokenFunc)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.listen != "0.0.0.0:9000" || cfg.data != "/data" || cfg.namespace != "workspaces" {
		t.Fatalf("options were not applied: %+v", cfg)
	}
	if cfg.grace != 48*time.Hour || cfg.startupGrace != 30*time.Second || cfg.interval != 2*time.Second ||
		cfg.roundTimeout != 20*time.Second || cfg.batch != 8 || cfg.concurrency != 2 {
		t.Fatalf("durations or bounds were not applied: %+v", cfg)
	}
	if cfg.token == "" {
		t.Fatal("token was not read from the environment")
	}
}

// 非法配置必须在启动阶段失败，而不是带着坏参数运行到几小时后才暴露。
func TestParseConfigRejectsInvalidOptions(t *testing.T) {
	if _, err := parseConfig(nil, tokenFunc); err != nil {
		t.Fatalf("defaults must be valid: %v", err)
	}
	for name, tc := range map[string]struct {
		args []string
		env  func(string) string
	}{
		"missing token":          {nil, func(string) string { return "" }},
		"short token":            {nil, func(string) string { return "too-short" }},
		"empty listen":           {[]string{"-listen", ""}, tokenFunc},
		"empty data dir":         {[]string{"-data", ""}, tokenFunc},
		"empty profile path":     {[]string{"-profiles", ""}, tokenFunc},
		"unsafe namespace":       {[]string{"-namespace", "Workspaces/../x"}, tokenFunc},
		"zero idle":              {[]string{"-idle", "0s"}, tokenFunc},
		"negative grace":         {[]string{"-grace", "-1h"}, tokenFunc},
		"negative startup grace": {[]string{"-startup-grace", "-1s"}, tokenFunc},
		"zero interval":          {[]string{"-reconcile", "0s"}, tokenFunc},
		"zero round timeout":     {[]string{"-round-timeout", "0s"}, tokenFunc},
		"batch too large":        {[]string{"-batch", "100000"}, tokenFunc},
		"zero concurrency":       {[]string{"-sweep-concurrency", "0"}, tokenFunc},
		"concurrency too large":  {[]string{"-sweep-concurrency", "1000"}, tokenFunc},
		"unknown flag":           {[]string{"-nope"}, tokenFunc},
	} {
		if _, err := parseConfig(tc.args, tc.env); err == nil {
			t.Fatalf("%s was accepted", name)
		}
	}
}

func TestLoadProfilesValidatesRuntimeConfiguration(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		t.Helper()
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	valid := `{"demo":{"image":"demo:local","port":8080,"health_path":"/health",` +
		`"mount_path":"/workspace","storage":"1Gi","cpu":"1","memory":"256Mi"}}`
	profiles, err := loadProfiles(write("valid.json", valid))
	if err != nil {
		t.Fatalf("valid profiles were rejected: %v", err)
	}
	if _, ok := profiles["demo"]; !ok || len(profiles) != 1 {
		t.Fatalf("unexpected profiles: %+v", profiles)
	}
	for name, body := range map[string]string{
		"typo in a key": `{"demo":{"image":"demo:local","port":8080,"health_path":"/health",` +
			`"mount_path":"/workspace","storage":"1Gi","cpu":"1","memory":"256Mi","image_pull":"Always"}}`,
		"missing image": `{"demo":{"port":8080,"health_path":"/health","mount_path":"/workspace",` +
			`"storage":"1Gi","cpu":"1","memory":"256Mi"}}`,
		"relative mount path": `{"demo":{"image":"demo:local","port":8080,"health_path":"/health",` +
			`"mount_path":"workspace","storage":"1Gi","cpu":"1","memory":"256Mi"}}`,
		"port out of range": `{"demo":{"image":"demo:local","port":70000,"health_path":"/health",` +
			`"mount_path":"/workspace","storage":"1Gi","cpu":"1","memory":"256Mi"}}`,
		"config secret without path": `{"demo":{"image":"demo:local","port":8080,"health_path":"/health",` +
			`"mount_path":"/workspace","storage":"1Gi","cpu":"1","memory":"256Mi","config_secret":"s"}}`,
		"unsafe profile name": `{"Demo":{"image":"demo:local","port":8080,"health_path":"/health",` +
			`"mount_path":"/workspace","storage":"1Gi","cpu":"1","memory":"256Mi"}}`,
		"no profiles":  `{}`,
		"broken json":  `{`,
		"invalid json": `[]`,
	} {
		if _, err := loadProfiles(write("bad.json", body)); err == nil {
			t.Fatalf("%s was accepted", name)
		}
	}
	if _, err := loadProfiles(filepath.Join(dir, "missing.json")); err == nil {
		t.Fatal("a missing profile file was accepted")
	}
	// 仓库自带的 profile 必须始终能通过校验，否则演示环境会在启动时挂掉。
	if _, err := loadProfiles(filepath.Join("..", "..", "configs", "profiles.json")); err != nil {
		t.Fatalf("shipped profiles are invalid: %v", err)
	}
}
