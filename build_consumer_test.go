package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/rabbitmq/amqp091-go"
)

type stubBuildExecutor struct {
	result  BuildResult
	err     error
	workDir string
}

func (s *stubBuildExecutor) BuildAndPush(_ context.Context, request BuildRequest) (BuildResult, error) {
	s.workDir = request.WorkDir
	return s.result, s.err
}

type ackRecorder struct {
	ackCount     int
	nackCount    int
	lastRequeue  bool
	lastMultiple bool
	rejectCount  int
}

func (a *ackRecorder) Ack(tag uint64, multiple bool) error {
	a.ackCount++
	a.lastMultiple = multiple
	return nil
}

func (a *ackRecorder) Nack(tag uint64, multiple bool, requeue bool) error {
	a.nackCount++
	a.lastMultiple = multiple
	a.lastRequeue = requeue
	return nil
}

func (a *ackRecorder) Reject(tag uint64, requeue bool) error {
	a.rejectCount++
	a.lastRequeue = requeue
	return nil
}

func TestProcessDeliveryAcksAfterPublishingBuildSucceeded(t *testing.T) {
	t.Parallel()

	buildExecutor := &stubBuildExecutor{
		result: BuildResult{
			ImageTag:  "ghcr.io/project/service:abcdef123456",
			CommitSHA: "abcdef1234567890",
			Builder:   "nixpacks",
		},
	}

	var published any
	consumer := &BuildConsumer{
		cfg: Config{
			DeploymentExchange:       "deployment.events",
			RabbitMQExchange:         "shiply.services",
			BuildStartedRoutingKey:   "build.started",
			BuildSucceededRoutingKey: "build.succeeded",
		},
		cloneRepositoryFn: func(_ ApplicationBuildRequestedPayload, repositoryDir string) error {
			return os.MkdirAll(repositoryDir, 0o755)
		},
		buildExecutor: buildExecutor,
		publishFn: func(_ context.Context, exchange, routingKey string, event any) error {
			if exchange == "deployment.events" {
				return nil
			}
			if exchange != "shiply.services" || routingKey != "build.succeeded" {
				t.Fatalf("unexpected publish target %q %q", exchange, routingKey)
			}
			published = event
			return nil
		},
	}

	recorder := &ackRecorder{}
	consumer.processDelivery(amqp091.Delivery{
		Body:         mustMarshalBuildRequest(t),
		Acknowledger: recorder,
		DeliveryTag:  1,
	})

	if recorder.ackCount != 1 {
		t.Fatalf("expected ack once, got %d", recorder.ackCount)
	}
	if recorder.nackCount != 0 {
		t.Fatalf("expected no nack, got %d", recorder.nackCount)
	}

	event, ok := published.(ServiceEvent[BuildSucceededPayload])
	if !ok {
		t.Fatalf("expected a success event, got %T", published)
	}
	if event.Payload.ImageTag != "ghcr.io/project/service:abcdef123456" {
		t.Fatalf("unexpected image tag %q", event.Payload.ImageTag)
	}
	if event.Payload.ProjectSlug != "project-one" {
		t.Fatalf("unexpected project slug %q", event.Payload.ProjectSlug)
	}
	if event.Payload.ServiceSlug != "shiply-api" {
		t.Fatalf("unexpected service slug %q", event.Payload.ServiceSlug)
	}
	if event.Payload.ContainerPort != 8080 {
		t.Fatalf("unexpected container port %d", event.Payload.ContainerPort)
	}
	if event.DeploymentID != "deployment-1" {
		t.Fatalf("unexpected deployment id %q", event.DeploymentID)
	}
	if _, err := os.Stat(buildExecutor.workDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected workdir to be cleaned up, stat err=%v", err)
	}
}

func TestProcessDeliveryPublishesBuildFailureAndAcks(t *testing.T) {
	t.Parallel()

	buildExecutor := &stubBuildExecutor{
		result: BuildResult{
			ImageTag:  "ghcr.io/project/service:abcdef123456",
			CommitSHA: "abcdef1234567890",
			Builder:   "dockerfile",
		},
		err: errors.New("docker push failed"),
	}

	var published any
	consumer := &BuildConsumer{
		cfg: Config{
			DeploymentExchange:     "deployment.events",
			RabbitMQExchange:       "shiply.services",
			BuildStartedRoutingKey: "build.started",
			BuildFailedRoutingKey:  "build.failed",
		},
		cloneRepositoryFn: func(_ ApplicationBuildRequestedPayload, repositoryDir string) error {
			return os.MkdirAll(repositoryDir, 0o755)
		},
		buildExecutor: buildExecutor,
		publishFn: func(_ context.Context, exchange, routingKey string, event any) error {
			if exchange == "deployment.events" {
				return nil
			}
			if exchange != "shiply.services" || routingKey != "build.failed" {
				t.Fatalf("unexpected publish target %q %q", exchange, routingKey)
			}
			published = event
			return nil
		},
	}

	recorder := &ackRecorder{}
	consumer.processDelivery(amqp091.Delivery{
		Body:         mustMarshalBuildRequest(t),
		Acknowledger: recorder,
		DeliveryTag:  2,
	})

	if recorder.ackCount != 1 {
		t.Fatalf("expected ack once, got %d", recorder.ackCount)
	}
	if recorder.nackCount != 0 {
		t.Fatalf("expected no nack, got %d", recorder.nackCount)
	}

	event, ok := published.(ServiceEvent[BuildFailedPayload])
	if !ok {
		t.Fatalf("expected a failure event, got %T", published)
	}
	if event.Payload.ErrorMessage != "docker push failed" {
		t.Fatalf("unexpected failure message %q", event.Payload.ErrorMessage)
	}
	if _, err := os.Stat(buildExecutor.workDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected workdir to be cleaned up, stat err=%v", err)
	}
}

func TestProcessDeliveryRequeuesWhenPublishingResultFails(t *testing.T) {
	t.Parallel()

	consumer := &BuildConsumer{
		cfg: Config{
			DeploymentExchange:       "deployment.events",
			RabbitMQExchange:         "shiply.services",
			BuildStartedRoutingKey:   "build.started",
			BuildSucceededRoutingKey: "build.succeeded",
		},
		cloneRepositoryFn: func(_ ApplicationBuildRequestedPayload, repositoryDir string) error {
			return os.MkdirAll(repositoryDir, 0o755)
		},
		buildExecutor: &stubBuildExecutor{
			result: BuildResult{
				ImageTag:  "ghcr.io/project/service:abcdef123456",
				CommitSHA: "abcdef1234567890",
				Builder:   "nixpacks",
			},
		},
		publishFn: func(_ context.Context, exchange, routingKey string, event any) error {
			return errors.New("rabbitmq unavailable")
		},
	}

	recorder := &ackRecorder{}
	consumer.processDelivery(amqp091.Delivery{
		Body:         mustMarshalBuildRequest(t),
		Acknowledger: recorder,
		DeliveryTag:  3,
	})

	if recorder.ackCount != 0 {
		t.Fatalf("expected no ack, got %d", recorder.ackCount)
	}
	if recorder.nackCount != 1 || !recorder.lastRequeue {
		t.Fatalf("expected nack with requeue, got nack=%d requeue=%v", recorder.nackCount, recorder.lastRequeue)
	}
}

func TestProcessDeliveryDropsMalformedMessage(t *testing.T) {
	t.Parallel()

	consumer := &BuildConsumer{}
	recorder := &ackRecorder{}

	consumer.processDelivery(amqp091.Delivery{
		Body:         []byte("{not-json"),
		Acknowledger: recorder,
		DeliveryTag:  4,
	})

	if recorder.ackCount != 0 {
		t.Fatalf("expected no ack, got %d", recorder.ackCount)
	}
	if recorder.nackCount != 1 || recorder.lastRequeue {
		t.Fatalf("expected nack without requeue, got nack=%d requeue=%v", recorder.nackCount, recorder.lastRequeue)
	}
}

func mustMarshalBuildRequest(t *testing.T) []byte {
	t.Helper()

	event := ServiceEvent[ApplicationBuildRequestedPayload]{
		EventID:      "request-1",
		EventType:    "app.build.requested",
		Timestamp:    json.RawMessage(`"2026-08-08T10:00:00Z"`),
		DeploymentID: "deployment-1",
		ProjectID:    "project-1",
		ServiceID:    "service-1",
		ServiceName:  "Shiply API",
		UserID:       "user-1",
		Payload: ApplicationBuildRequestedPayload{
			RepositoryURL: "https://github.com/shiply/example.git",
			Branch:        "main",
			ServiceAlias:  "shiply-api",
			ProjectSlug:   "project-one",
			ServiceSlug:   "shiply-api",
			ContainerPort: 8080,
		},
	}

	body, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal build request: %v", err)
	}
	return body
}
