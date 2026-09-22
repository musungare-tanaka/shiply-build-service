package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

type BuildRequest struct {
	ProjectID     string
	ServiceID     string
	RepositoryDir string
	WorkDir       string
}

type BuildResult struct {
	ImageTag  string
	CommitSHA string
	Builder   string
}

type BuildExecutor interface {
	BuildAndPush(ctx context.Context, request BuildRequest) (BuildResult, error)
}

type commandSpec struct {
	Name  string
	Args  []string
	Dir   string
	Env   map[string]string
	Stdin string
}

type commandRunner func(context.Context, commandSpec) (string, error)

type CommandBuilder struct {
	cfg Config
	run commandRunner
}

func NewCommandBuilder(cfg Config) *CommandBuilder {
	return &CommandBuilder{
		cfg: cfg,
		run: runCommand,
	}
}

func (b *CommandBuilder) BuildAndPush(ctx context.Context, request BuildRequest) (BuildResult, error) {
	result := BuildResult{}

	commitSHA, err := b.resolveCommitSHA(ctx, request.RepositoryDir)
	if err != nil {
		return result, err
	}
	result.CommitSHA = commitSHA

	imageTag, err := buildImageReference(b.cfg.Registry, request.ProjectID, request.ServiceID, commitSHA)
	if err != nil {
		return result, err
	}
	result.ImageTag = imageTag
	dockerEnv, err := b.registryDockerEnv(ctx, request.WorkDir)
	if err != nil {
		return result, err
	}
	defer func() {
		if _, removeErr := b.run(context.Background(), commandSpec{Name: "docker", Args: []string{"image", "rm", "-f", imageTag}}); removeErr != nil {
			log.Printf("remove local image failed for image=%s: %v", imageTag, removeErr)
		}
	}()

	if b.imageExists(ctx, imageTag, dockerEnv) {
		result.Builder = "registry"
		return result, nil
	}

	builderName, err := b.buildImage(ctx, request.RepositoryDir, imageTag)
	result.Builder = builderName
	if err != nil {
		return result, err
	}

	if err := b.pushImage(ctx, imageTag, dockerEnv); err != nil {
		return result, err
	}

	return result, nil
}

func (b *CommandBuilder) resolveCommitSHA(ctx context.Context, repositoryDir string) (string, error) {
	output, err := b.run(ctx, commandSpec{
		Name: "git",
		Args: []string{"rev-parse", "HEAD"},
		Dir:  repositoryDir,
	})
	if err != nil {
		return "", fmt.Errorf("resolve commit sha: %w", err)
	}

	commitSHA := strings.TrimSpace(output)
	if commitSHA == "" {
		return "", errors.New("resolve commit sha: git returned an empty commit sha")
	}
	return commitSHA, nil
}

func (b *CommandBuilder) buildImage(ctx context.Context, repositoryDir, imageTag string) (string, error) {
	log.Printf("running nixpacks build for image=%s", imageTag)
	_, nixpacksErr := b.run(ctx, commandSpec{
		Name: "nixpacks",
		Args: []string{"build", repositoryDir, "--name", imageTag},
	})
	if nixpacksErr == nil {
		return "nixpacks", nil
	}

	dockerfileExists, err := hasDockerfile(repositoryDir)
	if err != nil {
		return "nixpacks", err
	}
	if !dockerfileExists {
		return "nixpacks", fmt.Errorf("nixpacks build failed and no Dockerfile found: %w", nixpacksErr)
	}

	log.Printf("nixpacks failed for image=%s; falling back to docker build", imageTag)
	_, err = b.run(ctx, commandSpec{
		Name: "docker",
		Args: []string{"build", "-t", imageTag, repositoryDir},
	})
	if err != nil {
		return "dockerfile", fmt.Errorf("docker build fallback failed: %w", err)
	}

	return "dockerfile", nil
}

func (b *CommandBuilder) registryDockerEnv(ctx context.Context, workDir string) (map[string]string, error) {
	if err := validateRegistryConfig(b.cfg.Registry); err != nil {
		return nil, err
	}

	dockerConfigDir := filepath.Join(workDir, "docker-config")
	if err := os.MkdirAll(dockerConfigDir, 0o700); err != nil {
		return nil, fmt.Errorf("create docker config dir: %w", err)
	}

	env := map[string]string{
		"DOCKER_CONFIG": dockerConfigDir,
	}

	log.Printf("logging into container registry host=%s", b.cfg.Registry.Host)
	if _, err := b.run(ctx, commandSpec{
		Name:  "docker",
		Args:  []string{"login", normalizeRegistryHost(b.cfg.Registry.Host), "--username", b.cfg.Registry.Username, "--password-stdin"},
		Env:   env,
		Stdin: b.cfg.Registry.Password,
	}); err != nil {
		return nil, fmt.Errorf("docker login failed: %w", err)
	}
	return env, nil
}

func (b *CommandBuilder) imageExists(ctx context.Context, imageTag string, env map[string]string) bool {
	_, err := b.run(ctx, commandSpec{Name: "docker", Args: []string{"manifest", "inspect", imageTag}, Env: env})
	return err == nil
}

func (b *CommandBuilder) pushImage(ctx context.Context, imageTag string, env map[string]string) error {
	log.Printf("pushing built image=%s", imageTag)
	if _, err := b.run(ctx, commandSpec{
		Name: "docker",
		Args: []string{"push", imageTag},
		Env:  env,
	}); err != nil {
		return fmt.Errorf("docker push failed: %w", err)
	}

	return nil
}

func validateRegistryConfig(cfg RegistryConfig) error {
	switch {
	case strings.TrimSpace(cfg.Host) == "":
		return errors.New("missing CONTAINER_REGISTRY_HOST")
	case strings.TrimSpace(cfg.Username) == "":
		return errors.New("missing CONTAINER_REGISTRY_USERNAME")
	case cfg.Password == "":
		return errors.New("missing CONTAINER_REGISTRY_PASSWORD")
	default:
		return nil
	}
}

func hasDockerfile(repositoryDir string) (bool, error) {
	dockerfilePath := filepath.Join(repositoryDir, "Dockerfile")
	info, err := os.Stat(dockerfilePath)
	if err == nil {
		return !info.IsDir(), nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, fmt.Errorf("check Dockerfile: %w", err)
}

func buildImageReference(registry RegistryConfig, projectID, serviceID, commitSHA string) (string, error) {
	host := normalizeRegistryHost(registry.Host)
	if host == "" {
		return "", errors.New("missing CONTAINER_REGISTRY_HOST")
	}

	shortSHA := shortCommitSHA(commitSHA)
	if shortSHA == "" {
		return "", errors.New("missing commit sha for image tag")
	}

	segments := []string{host}
	prefix := normalizeRepositoryPrefix(registry.RepositoryPrefix)
	if prefix != "" {
		segments = append(segments, prefix)
	}
	segments = append(segments,
		sanitizeImageComponent(projectID, "project"),
		sanitizeImageComponent(serviceID, "service"),
	)

	return strings.Join(segments, "/") + ":" + shortSHA, nil
}

func normalizeRegistryHost(host string) string {
	return strings.TrimSuffix(strings.ToLower(strings.TrimSpace(host)), "/")
}

func normalizeRepositoryPrefix(prefix string) string {
	if strings.TrimSpace(prefix) == "" {
		return ""
	}

	parts := strings.Split(prefix, "/")
	normalized := make([]string, 0, len(parts))
	for _, part := range parts {
		part = sanitizeImageComponent(part, "")
		if part == "" {
			continue
		}
		normalized = append(normalized, part)
	}
	return strings.Join(normalized, "/")
}

func sanitizeImageComponent(value, fallback string) string {
	text := strings.ToLower(strings.TrimSpace(value))
	if text == "" {
		return fallback
	}

	var builder strings.Builder
	lastSeparator := false
	for _, r := range text {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			builder.WriteRune(r)
			lastSeparator = false
			continue
		}

		switch r {
		case '.', '-', '_':
			if builder.Len() == 0 || lastSeparator {
				continue
			}
			builder.WriteRune(r)
			lastSeparator = true
		default:
			if builder.Len() == 0 || lastSeparator {
				continue
			}
			builder.WriteRune('-')
			lastSeparator = true
		}
	}

	sanitized := strings.Trim(builder.String(), "._-")
	if sanitized == "" {
		return fallback
	}
	return sanitized
}

func shortCommitSHA(commitSHA string) string {
	commitSHA = strings.TrimSpace(commitSHA)
	if len(commitSHA) > 12 {
		return commitSHA[:12]
	}
	return commitSHA
}

func runCommand(ctx context.Context, spec commandSpec) (string, error) {
	cmd := exec.CommandContext(ctx, spec.Name, spec.Args...)
	cmd.Dir = spec.Dir
	cmd.Env = mergeCommandEnv(spec.Env)
	if spec.Stdin != "" {
		cmd.Stdin = strings.NewReader(spec.Stdin)
	}

	output, err := cmd.CombinedOutput()
	trimmedOutput := strings.TrimSpace(string(output))
	if err != nil {
		if trimmedOutput == "" {
			return "", fmt.Errorf("%s: %w", spec.Name, err)
		}
		return "", fmt.Errorf("%s: %s", spec.Name, redact(trimmedOutput))
	}

	return trimmedOutput, nil
}

func mergeCommandEnv(extra map[string]string) []string {
	env := os.Environ()
	for key, value := range extra {
		env = append(env, key+"="+value)
	}
	return env
}
