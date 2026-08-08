package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestBuildImageReference(t *testing.T) {
	t.Parallel()

	imageTag, err := buildImageReference(
		RegistryConfig{
			Host:             "GHCR.IO/",
			RepositoryPrefix: "Shiply/Builds",
		},
		"Project ABC",
		"Service_X",
		"abcdef1234567890",
	)
	if err != nil {
		t.Fatalf("buildImageReference returned error: %v", err)
	}

	expected := "ghcr.io/shiply/builds/project-abc/service_x:abcdef123456"
	if imageTag != expected {
		t.Fatalf("expected image tag %q, got %q", expected, imageTag)
	}
}

func TestCommandBuilderBuildAndPushUsesNixpacks(t *testing.T) {
	t.Parallel()

	cfg := Config{
		Registry: RegistryConfig{
			Host:     "ghcr.io",
			Username: "shiply",
			Password: "secret",
		},
	}

	var commands []commandSpec
	builder := &CommandBuilder{
		cfg: cfg,
		run: func(_ context.Context, spec commandSpec) (string, error) {
			commands = append(commands, spec)
			switch spec.Name {
			case "git":
				return "abcdef1234567890", nil
			case "nixpacks":
				return "image built", nil
			case "docker":
				return "ok", nil
			default:
				return "", nil
			}
		},
	}

	result, err := builder.BuildAndPush(context.Background(), BuildRequest{
		ProjectID:     "project-one",
		ServiceID:     "api",
		RepositoryDir: "/tmp/repo",
		WorkDir:       t.TempDir(),
	})
	if err != nil {
		t.Fatalf("BuildAndPush returned error: %v", err)
	}

	if result.Builder != "nixpacks" {
		t.Fatalf("expected builder nixpacks, got %q", result.Builder)
	}
	if result.ImageTag != "ghcr.io/project-one/api:abcdef123456" {
		t.Fatalf("unexpected image tag %q", result.ImageTag)
	}

	gotCommands := []string{
		commands[0].Name + " " + strings.Join(commands[0].Args, " "),
		commands[1].Name + " " + strings.Join(commands[1].Args, " "),
		commands[2].Name + " " + strings.Join(commands[2].Args, " "),
		commands[3].Name + " " + strings.Join(commands[3].Args, " "),
	}
	wantCommands := []string{
		"git rev-parse HEAD",
		"nixpacks build /tmp/repo --name ghcr.io/project-one/api:abcdef123456",
		"docker login ghcr.io --username shiply --password-stdin",
		"docker push ghcr.io/project-one/api:abcdef123456",
	}
	if !reflect.DeepEqual(gotCommands, wantCommands) {
		t.Fatalf("unexpected commands: %#v", gotCommands)
	}
	if commands[2].Stdin != "secret" {
		t.Fatalf("expected registry password on stdin")
	}
	if commands[2].Env["DOCKER_CONFIG"] == "" {
		t.Fatalf("expected DOCKER_CONFIG to be set for docker login")
	}
}

func TestCommandBuilderBuildAndPushFallsBackToDockerfile(t *testing.T) {
	t.Parallel()

	repoDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(repoDir, "Dockerfile"), []byte("FROM scratch\n"), 0o600); err != nil {
		t.Fatalf("write Dockerfile: %v", err)
	}

	cfg := Config{
		Registry: RegistryConfig{
			Host:     "registry.example.com",
			Username: "shiply",
			Password: "secret",
		},
	}

	var commands []commandSpec
	builder := &CommandBuilder{
		cfg: cfg,
		run: func(_ context.Context, spec commandSpec) (string, error) {
			commands = append(commands, spec)
			switch spec.Name {
			case "git":
				return "fedcba9876543210", nil
			case "nixpacks":
				return "", errors.New("no build plan")
			case "docker":
				return "ok", nil
			default:
				return "", nil
			}
		},
	}

	result, err := builder.BuildAndPush(context.Background(), BuildRequest{
		ProjectID:     "Project",
		ServiceID:     "Web",
		RepositoryDir: repoDir,
		WorkDir:       t.TempDir(),
	})
	if err != nil {
		t.Fatalf("BuildAndPush returned error: %v", err)
	}
	if result.Builder != "dockerfile" {
		t.Fatalf("expected dockerfile fallback, got %q", result.Builder)
	}

	var sawDockerBuild bool
	for _, command := range commands {
		if command.Name == "docker" && len(command.Args) >= 1 && command.Args[0] == "build" {
			sawDockerBuild = true
		}
	}
	if !sawDockerBuild {
		t.Fatalf("expected docker build fallback to run")
	}
}

func TestCommandBuilderBuildAndPushFailsWithoutDockerfileAfterNixpacks(t *testing.T) {
	t.Parallel()

	builder := &CommandBuilder{
		cfg: Config{
			Registry: RegistryConfig{
				Host:     "ghcr.io",
				Username: "shiply",
				Password: "secret",
			},
		},
		run: func(_ context.Context, spec commandSpec) (string, error) {
			switch spec.Name {
			case "git":
				return "abcdef1234567890", nil
			case "nixpacks":
				return "", errors.New("no build plan")
			default:
				return "ok", nil
			}
		},
	}

	result, err := builder.BuildAndPush(context.Background(), BuildRequest{
		ProjectID:     "project",
		ServiceID:     "service",
		RepositoryDir: t.TempDir(),
		WorkDir:       t.TempDir(),
	})
	if err == nil {
		t.Fatalf("expected BuildAndPush to fail without Dockerfile")
	}
	if result.Builder != "nixpacks" {
		t.Fatalf("expected failure builder to remain nixpacks, got %q", result.Builder)
	}
	if !strings.Contains(err.Error(), "no Dockerfile found") {
		t.Fatalf("expected no Dockerfile error, got %v", err)
	}
}
