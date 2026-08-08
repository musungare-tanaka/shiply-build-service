package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

type publishFunc func(context.Context, string, any) error

type deliveryAction struct {
	ack     bool
	requeue bool
}

var (
	actionAck         = deliveryAction{ack: true}
	actionNackDrop    = deliveryAction{}
	actionNackRequeue = deliveryAction{requeue: true}
)

type BuildConsumer struct {
	cfg  Config
	conn *amqp091.Connection
	ch   *amqp091.Channel

	cloneRepositoryFn func(ApplicationBuildRequestedPayload, string) error
	buildExecutor     BuildExecutor
	publishFn         publishFunc
}

type ServiceEvent[T any] struct {
	EventID   string          `json:"eventId"`
	EventType string          `json:"eventType"`
	Timestamp json.RawMessage `json:"timestamp"`
	ProjectID string          `json:"projectId"`
	ServiceID string          `json:"serviceId"`
	UserID    string          `json:"userId"`
	Payload   T               `json:"payload"`
}

type ApplicationBuildRequestedPayload struct {
	RepositoryURL         string `json:"repositoryUrl"`
	Branch                string `json:"branch"`
	LinkedDatabaseService string `json:"linkedDatabaseServiceId"`
	ServiceAlias          string `json:"serviceAlias"`
	ProjectSlug           string `json:"projectSlug"`
	ServiceSlug           string `json:"serviceSlug"`
	ContainerPort         int32  `json:"containerPort"`
	RepositoryProvider    string `json:"repositoryProvider"`
	GitHubInstallationID  *int64 `json:"githubInstallationId"`
	GitHubRepositoryID    *int64 `json:"githubRepositoryId"`
	RepositoryOwner       string `json:"repositoryOwner"`
	RepositoryName        string `json:"repositoryName"`
	CloneURL              string `json:"cloneUrl"`
	PrivateRepository     bool   `json:"privateRepository"`
}

func NewBuildConsumer(cfg Config) (*BuildConsumer, error) {
	conn, err := amqp091.Dial(cfg.RabbitMQURL)
	if err != nil {
		return nil, fmt.Errorf("connect rabbitmq: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("open rabbitmq channel: %w", err)
	}

	if err := ch.ExchangeDeclare(cfg.RabbitMQExchange, "direct", true, false, false, false, nil); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("declare exchange: %w", err)
	}

	queue, err := ch.QueueDeclare(cfg.ApplicationBuildQueue, true, false, false, false, nil)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("declare queue: %w", err)
	}

	if err := ch.QueueBind(queue.Name, cfg.ApplicationRoutingKey, cfg.RabbitMQExchange, false, nil); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("bind queue: %w", err)
	}

	if err := ch.Qos(1, 0, false); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("set qos: %w", err)
	}

	consumer := &BuildConsumer{
		cfg:           cfg,
		conn:          conn,
		ch:            ch,
		buildExecutor: NewCommandBuilder(cfg),
	}
	consumer.cloneRepositoryFn = consumer.cloneRepository
	consumer.publishFn = consumer.publishJSONEvent
	return consumer, nil
}

func (c *BuildConsumer) Close() {
	if c.ch != nil {
		_ = c.ch.Close()
	}
	if c.conn != nil {
		_ = c.conn.Close()
	}
}

func (c *BuildConsumer) Start() error {
	msgs, err := c.ch.Consume(c.cfg.ApplicationBuildQueue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume queue: %w", err)
	}

	for msg := range msgs {
		c.processDelivery(msg)
	}

	return errors.New("rabbitmq consumer channel closed")
}

func (c *BuildConsumer) processDelivery(msg amqp091.Delivery) {
	action, err := c.handleMessage(msg.Body)
	if err != nil {
		log.Printf("build event failed: %s", redact(err.Error()))
	}

	if action.ack {
		if ackErr := msg.Ack(false); ackErr != nil {
			log.Printf("ack failed: %v", ackErr)
		}
		return
	}

	if nackErr := msg.Nack(false, action.requeue); nackErr != nil {
		log.Printf("nack failed: %v", nackErr)
	}
}

func (c *BuildConsumer) handleMessage(body []byte) (deliveryAction, error) {
	var event ServiceEvent[ApplicationBuildRequestedPayload]
	if err := json.Unmarshal(body, &event); err != nil {
		return actionNackDrop, fmt.Errorf("decode build event: %w", err)
	}

	return c.handleBuildEvent(event)
}

func (c *BuildConsumer) handleBuildEvent(event ServiceEvent[ApplicationBuildRequestedPayload]) (deliveryAction, error) {
	payload := event.Payload
	targetDir, err := os.MkdirTemp("", "shiply-build-*")
	if err != nil {
		return c.completeFailure(event, BuildResult{}, fmt.Errorf("create workdir: %w", err))
	}
	defer func() {
		if removeErr := os.RemoveAll(targetDir); removeErr != nil {
			log.Printf("cleanup failed for %s: %v", targetDir, removeErr)
		}
	}()

	repositoryDir := filepath.Join(targetDir, sanitizeName(payload.ServiceAlias))
	cloneFn := c.cloneRepositoryFn
	if cloneFn == nil {
		cloneFn = c.cloneRepository
	}
	if err := cloneFn(payload, repositoryDir); err != nil {
		return c.completeFailure(event, BuildResult{}, err)
	}

	log.Printf(
		"build source prepared for service=%s project=%s branch=%s path=%s",
		sanitizeName(payload.ServiceAlias),
		event.ProjectID,
		payload.Branch,
		repositoryDir,
	)

	builder := c.buildExecutor
	if builder == nil {
		builder = NewCommandBuilder(c.cfg)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	result, err := builder.BuildAndPush(ctx, BuildRequest{
		ProjectID:     event.ProjectID,
		ServiceID:     event.ServiceID,
		RepositoryDir: repositoryDir,
		WorkDir:       targetDir,
	})
	if err != nil {
		return c.completeFailure(event, result, err)
	}

	successEvent := newBuildSucceededEvent(event, result)
	if err := c.publishEvent(ctx, c.cfg.BuildSucceededRoutingKey, successEvent); err != nil {
		return actionNackRequeue, fmt.Errorf("publish build succeeded event: %w", err)
	}

	log.Printf(
		"build succeeded for service=%s project=%s image=%s builder=%s",
		event.ServiceID,
		event.ProjectID,
		result.ImageTag,
		result.Builder,
	)
	return actionAck, nil
}

func (c *BuildConsumer) completeFailure(
	event ServiceEvent[ApplicationBuildRequestedPayload],
	result BuildResult,
	buildErr error,
) (deliveryAction, error) {
	failureEvent := newBuildFailedEvent(event, result, buildErr)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := c.publishEvent(ctx, c.cfg.BuildFailedRoutingKey, failureEvent); err != nil {
		return actionNackRequeue, fmt.Errorf("%v; publish build failed event: %w", buildErr, err)
	}

	return actionAck, buildErr
}

func (c *BuildConsumer) publishEvent(ctx context.Context, routingKey string, event any) error {
	publish := c.publishFn
	if publish == nil {
		publish = c.publishJSONEvent
	}
	return publish(ctx, routingKey, event)
}

func (c *BuildConsumer) publishJSONEvent(ctx context.Context, routingKey string, event any) error {
	if c.ch == nil {
		return errors.New("rabbitmq channel is not initialized")
	}

	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event payload: %w", err)
	}

	if err := c.ch.PublishWithContext(ctx, c.cfg.RabbitMQExchange, routingKey, false, false, amqp091.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp091.Persistent,
		Timestamp:    time.Now().UTC(),
		Body:         body,
	}); err != nil {
		return fmt.Errorf("publish rabbitmq event: %w", err)
	}

	return nil
}

func (c *BuildConsumer) cloneRepository(payload ApplicationBuildRequestedPayload, repositoryDir string) error {
	cloneURL := payload.CloneURL
	if cloneURL == "" {
		cloneURL = payload.RepositoryURL
	}
	if cloneURL == "" {
		return errors.New("missing clone url")
	}

	if payload.RepositoryProvider == "GITHUB_APP" && payload.PrivateRepository {
		if payload.GitHubInstallationID == nil {
			return errors.New("missing GitHub installation id for private repository")
		}
		token, err := generateInstallationToken(c.cfg, *payload.GitHubInstallationID)
		if err != nil {
			return err
		}
		return cloneWithAskPass(cloneURL, payload.Branch, repositoryDir, token)
	}

	return runGitClone(cloneURL, payload.Branch, repositoryDir, nil)
}

func cloneWithAskPass(cloneURL, branch, repositoryDir, token string) error {
	tmpDir, err := os.MkdirTemp("", "shiply-askpass-*")
	if err != nil {
		return fmt.Errorf("create askpass dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	scriptPath := filepath.Join(tmpDir, "askpass.sh")
	script := "#!/bin/sh\ncase \"$1\" in\n*Username*) printf '%s' \"x-access-token\" ;;\n*) printf '%s' \"$GIT_TOKEN\" ;;\nesac\n"
	if err := os.WriteFile(scriptPath, []byte(script), 0o700); err != nil {
		return fmt.Errorf("write askpass script: %w", err)
	}

	env := map[string]string{
		"GIT_ASKPASS":         scriptPath,
		"GIT_TOKEN":           token,
		"GIT_TERMINAL_PROMPT": "0",
	}
	return runGitClone(cloneURL, branch, repositoryDir, env)
}

func runGitClone(cloneURL, branch, repositoryDir string, extraEnv map[string]string) error {
	args := []string{"clone", "--depth", "1", "--single-branch"}
	if strings.TrimSpace(branch) != "" {
		args = append(args, "--branch", branch)
	}
	args = append(args, cloneURL, repositoryDir)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Env = os.Environ()
	for key, value := range extraEnv {
		cmd.Env = append(cmd.Env, key+"="+value)
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("git clone failed: %s", redact(string(output)))
	}
	return nil
}

func sanitizeName(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "service"
	}
	replacer := strings.NewReplacer("/", "-", "\\", "-", " ", "-")
	return replacer.Replace(trimmed)
}

func redact(value string) string {
	value = strings.ReplaceAll(value, "\n", " ")
	for _, needle := range []string{"x-access-token", "ghs_", "github_pat_", "Bearer "} {
		if strings.Contains(value, needle) {
			return "redacted git or GitHub credential output"
		}
	}
	return value
}
