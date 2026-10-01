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
	"sync/atomic"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

type publishFunc func(context.Context, string, string, any) error

type deliveryAction struct {
	ack     bool
	requeue bool
}

var (
	actionAck         = deliveryAction{ack: true}
	actionNackDrop    = deliveryAction{}
	actionNackRequeue = deliveryAction{requeue: true}
)

var (
	errBuildLeaseBusy      = errors.New("build stage lease is owned by another worker")
	errStoredResultPublish = errors.New("publish stored terminal build result")
)

type BuildConsumer struct {
	cfg    Config
	conn   *amqp091.Connection
	ch     *amqp091.Channel
	pub    *RabbitPublisher
	ledger StageLedger

	cloneRepositoryFn func(ApplicationBuildRequestedPayload, string) error
	buildExecutor     BuildExecutor
	publishFn         publishFunc
	jobs              chan buildJob
	connectedAt       atomic.Int64
	disconnectedAt    atomic.Int64
}

type buildJob struct {
	event       ServiceEvent[ApplicationBuildRequestedPayload]
	body        []byte
	headers     amqp091.Table
	contentType string
}

const buildConcurrency = 1

func consumerBackoff(attempt int) time.Duration {
	d := time.Second
	for i := 0; i < attempt && d < 30*time.Second; i++ {
		d *= 2
	}
	if d > 30*time.Second {
		return 30 * time.Second
	}
	return d
}

func sanitizeStoredText(s string) string {
	s = strings.ToValidUTF8(s, "")
	s = strings.ReplaceAll(s, "\x00", "")
	r := []rune(s)
	if len(r) > 2000 {
		r = r[len(r)-2000:]
	}
	return string(r)
}

type buildResultEvents struct {
	ServiceRoutingKey  string          `json:"serviceRoutingKey"`
	ServiceEvent       json.RawMessage `json:"serviceEvent"`
	ProgressRoutingKey string          `json:"progressRoutingKey"`
	ProgressEvent      json.RawMessage `json:"progressEvent"`
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
	consumer := &BuildConsumer{
		cfg:           cfg,
		buildExecutor: NewCommandBuilder(cfg),
		jobs:          make(chan buildJob, buildConcurrency),
	}
	ledger, err := NewPostgresStageLedger(context.Background(), cfg.DatabaseURL, cfg.LedgerSchema)
	if err != nil {
		consumer.Close()
		return nil, fmt.Errorf("initialize stage ledger: %w", err)
	}
	consumer.ledger = ledger
	consumer.cloneRepositoryFn = consumer.cloneRepository
	consumer.publishFn = consumer.publishJSONEvent
	return consumer, nil
}

func (c *BuildConsumer) connect() error {
	conn, err := amqp091.Dial(c.cfg.RabbitMQURL)
	if err != nil {
		return fmt.Errorf("connect rabbitmq: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("open rabbitmq channel: %w", err)
	}
	cleanup := func() { _ = ch.Close(); _ = conn.Close() }
	if err := ch.ExchangeDeclare(c.cfg.RabbitMQExchange, "direct", true, false, false, false, nil); err != nil {
		cleanup()
		return fmt.Errorf("declare exchange: %w", err)
	}
	queue, err := ch.QueueDeclare(c.cfg.ApplicationBuildQueue, true, false, false, false, nil)
	if err != nil {
		cleanup()
		return fmt.Errorf("declare queue: %w", err)
	}
	if err := ch.QueueBind(queue.Name, c.cfg.ApplicationRoutingKey, c.cfg.RabbitMQExchange, false, nil); err != nil {
		cleanup()
		return fmt.Errorf("bind queue: %w", err)
	}
	if err := declareRetryTopology(ch, c.cfg); err != nil {
		cleanup()
		return err
	}
	if err := ch.Qos(buildConcurrency, 0, false); err != nil {
		cleanup()
		return fmt.Errorf("set qos: %w", err)
	}
	pub, err := NewRabbitPublisher(conn, []ExchangeSpec{{Name: c.cfg.RabbitMQExchange, Kind: "direct"}, {Name: c.cfg.DeploymentExchange, Kind: "topic"}})
	if err != nil {
		cleanup()
		return fmt.Errorf("initialize publisher: %w", err)
	}
	c.conn, c.ch, c.pub = conn, ch, pub
	c.disconnectedAt.Store(0)
	return nil
}

func (c *BuildConsumer) Close() {
	if c.ledger != nil {
		_ = c.ledger.Close()
	}
	if c.pub != nil {
		_ = c.pub.Close()
	}
	if c.ch != nil {
		_ = c.ch.Close()
	}
	if c.conn != nil {
		_ = c.conn.Close()
	}
}

func (c *BuildConsumer) Start(ctx context.Context) error {
	if c.jobs == nil {
		c.jobs = make(chan buildJob, buildConcurrency)
	}
	for i := 0; i < buildConcurrency; i++ {
		go c.worker(ctx)
	}
	backoffAttempt := 0
	for {
		if ctx.Err() != nil {
			return nil
		}
		if err := c.connect(); err != nil {
			log.Printf("build consumer connection failed: %v", err)
			if !waitBackoff(ctx, consumerBackoff(backoffAttempt)) {
				return nil
			}
			backoffAttempt++
			continue
		}
		closeNotice := c.conn.NotifyClose(make(chan *amqp091.Error, 1))
		msgs, err := c.ch.Consume(c.cfg.ApplicationBuildQueue, "", false, false, false, false, nil)
		if err != nil {
			log.Printf("build consumer consume failed: %v", err)
		} else {
			c.connectedAt.Store(time.Now().UnixNano())
			backoffAttempt = 0
			log.Printf("build consumer connected")
			for {
				select {
				case <-ctx.Done():
					c.disconnect()
					return nil
				case closeErr := <-closeNotice:
					if closeErr != nil {
						log.Printf("build consumer connection closed: %v", closeErr)
					}
					goto reconnect
				case msg, ok := <-msgs:
					if !ok {
						log.Printf("build consumer deliveries channel closed")
						goto reconnect
					}
					c.acceptDelivery(msg)
				}
			}
		}
	reconnect:
		c.disconnect()
		if !waitBackoff(ctx, consumerBackoff(backoffAttempt)) {
			return nil
		}
		backoffAttempt++
	}
}

func waitBackoff(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
func (c *BuildConsumer) disconnect() {
	c.connectedAt.Store(0)
	if c.disconnectedAt.Load() == 0 {
		c.disconnectedAt.Store(time.Now().UnixNano())
	}
	if c.pub != nil {
		_ = c.pub.Close()
		c.pub = nil
	}
	if c.ch != nil {
		_ = c.ch.Close()
		c.ch = nil
	}
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
}
func (c *BuildConsumer) Unhealthy() bool {
	started := c.disconnectedAt.Load()
	return started != 0 && time.Since(time.Unix(0, started)) > 2*time.Minute
}
func (c *BuildConsumer) Connected() bool { return c.connectedAt.Load() != 0 }
func (c *BuildConsumer) acceptDelivery(msg amqp091.Delivery) {
	var event ServiceEvent[ApplicationBuildRequestedPayload]
	if err := json.Unmarshal(msg.Body, &event); err != nil {
		c.processDelivery(msg)
		return
	}
	if strings.TrimSpace(event.DeploymentID) == "" || validateBuildPayload(event.Payload) != nil {
		c.processDelivery(msg)
		return
	}
	if c.ledger != nil {
		disposition, record, err := c.ledger.Claim(context.Background(), event.DeploymentID, "build", c.cfg.LeaseDuration)
		if err != nil {
			log.Printf("claim build stage failed: %v", err)
			_ = msg.Nack(false, true)
			return
		}
		switch disposition {
		case ClaimBusy:
			_ = msg.Nack(false, true)
			return
		case ClaimCompleted, ClaimFailed:
			action, err := c.publishStored(record.ResultJSON)
			if err != nil {
				log.Printf("publish stored result failed: %v", err)
				_ = msg.Nack(false, true)
				return
			}
			if action.ack {
				_ = msg.Ack(false)
			} else {
				_ = msg.Nack(false, action.requeue)
			}
			return
		}
	}
	if err := msg.Ack(false); err != nil {
		log.Printf("early build ack failed: %v", err)
		return
	}
	select {
	case c.jobs <- buildJob{event: event, body: append([]byte(nil), msg.Body...), headers: cloneHeaders(msg.Headers), contentType: msg.ContentType}:
	default:
		job := buildJob{event: event, body: append([]byte(nil), msg.Body...), headers: cloneHeaders(msg.Headers), contentType: msg.ContentType}
		go func() { c.jobs <- job }()
	}
}
func (c *BuildConsumer) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-c.jobs:
			if ctx.Err() != nil {
				return
			}
			c.runBuildEvent(ctx, job)
		}
	}
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

	if action.requeue && c.ch != nil {
		if retryErr := c.scheduleRetry(msg, err, !errors.Is(err, errBuildLeaseBusy)); retryErr == nil {
			_ = msg.Ack(false)
			return
		}
	}
	if !action.requeue && c.ch != nil {
		if dlqErr := c.copyToDLQ(msg, err); dlqErr == nil {
			_ = msg.Ack(false)
			return
		}
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
	if strings.TrimSpace(event.DeploymentID) == "" {
		return actionNackDrop, errors.New("missing deploymentId")
	}
	if err := validateBuildPayload(event.Payload); err != nil {
		return actionNackDrop, err
	}
	if c.ledger != nil {
		disposition, record, err := c.ledger.Claim(context.Background(), event.DeploymentID, "build", c.cfg.LeaseDuration)
		if err != nil {
			return actionNackRequeue, fmt.Errorf("claim build stage: %w", err)
		}
		switch disposition {
		case ClaimCompleted, ClaimFailed:
			return c.publishStored(record.ResultJSON)
		case ClaimBusy:
			return actionNackRequeue, errBuildLeaseBusy
		}
	}
	return c.handleClaimedBuildEvent(event)
}

func (c *BuildConsumer) runBuildEvent(ctx context.Context, job buildJob) {
	if action, err := c.executeBuildEvent(ctx, job.event); !action.ack && err != nil {
		log.Printf("asynchronously processed build failed deployment=%s: %s", job.event.DeploymentID, redact(err.Error()))
		msg := amqp091.Delivery{Body: job.body, Headers: job.headers, ContentType: job.contentType}
		if action.requeue {
			if retryErr := c.scheduleRetry(msg, err, !errors.Is(err, errBuildLeaseBusy)); retryErr != nil {
				log.Printf("schedule asynchronous build retry failed: %v", retryErr)
			}
		} else if dlqErr := c.copyToDLQ(msg, err); dlqErr != nil {
			log.Printf("send asynchronous build failure to dlq failed: %v", dlqErr)
		}
	}
}

func (c *BuildConsumer) executeBuildEvent(_ context.Context, event ServiceEvent[ApplicationBuildRequestedPayload]) (deliveryAction, error) {
	// The claim and early acknowledgement happen in acceptDelivery. Run the
	// existing stage body without claiming the same lease a second time.
	return c.handleClaimedBuildEvent(event)
}

func (c *BuildConsumer) handleClaimedBuildEvent(event ServiceEvent[ApplicationBuildRequestedPayload]) (deliveryAction, error) {
	startedEvent := newBuildStartedDeploymentEvent(event)
	startedCtx, cancelStarted := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelStarted()
	if err := c.publishProgressEvent(startedCtx, c.cfg.DeploymentExchange, c.cfg.DeploymentBuildStartedRoutingKey, startedEvent); err != nil {
		return actionNackRequeue, fmt.Errorf("publish build started event: %w", err)
	}
	targetDir, err := os.MkdirTemp("", "shiply-build-*")
	if err != nil {
		return c.completeFailure(event, BuildResult{}, fmt.Errorf("create workdir: %w", err))
	}
	defer func() {
		if removeErr := os.RemoveAll(targetDir); removeErr != nil {
			log.Printf("cleanup failed for %s: %v", targetDir, removeErr)
		}
	}()
	repositoryDir := filepath.Join(targetDir, sanitizeName(event.Payload.ServiceAlias))
	cloneFn := c.cloneRepositoryFn
	if cloneFn == nil {
		cloneFn = c.cloneRepository
	}
	if err := cloneFn(event.Payload, repositoryDir); err != nil {
		return c.completeFailure(event, BuildResult{}, err)
	}
	log.Printf("build source prepared for service=%s project=%s branch=%s path=%s", sanitizeName(event.Payload.ServiceAlias), event.ProjectID, event.Payload.Branch, repositoryDir)
	builder := c.buildExecutor
	if builder == nil {
		builder = NewCommandBuilder(c.cfg)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
	defer cancel()
	stopRenew := c.startLeaseRenewal(ctx, event.DeploymentID)
	defer stopRenew()
	result, err := builder.BuildAndPush(ctx, BuildRequest{ProjectID: event.ProjectID, ServiceID: event.ServiceID, RepositoryDir: repositoryDir, WorkDir: targetDir})
	if err != nil {
		_, failureErr := c.completeFailure(event, BuildResult{}, err)
		return actionAck, failureErr
	}
	successEvent := newBuildSucceededEvent(event, result)
	bundle, err := makeBuildResultEvents(c.cfg.BuildSucceededRoutingKey, successEvent, c.cfg.DeploymentBuildSucceededRoutingKey, newBuildSucceededDeploymentEvent(event, result))
	if err != nil {
		return actionNackRequeue, err
	}
	if c.ledger != nil {
		if err := c.ledger.Complete(ctx, event.DeploymentID, "build", bundle); err != nil {
			return actionNackRequeue, fmt.Errorf("persist build result: %w", err)
		}
	}
	if err := c.publishEvent(ctx, c.cfg.RabbitMQExchange, c.cfg.BuildSucceededRoutingKey, successEvent); err != nil {
		return actionNackRequeue, fmt.Errorf("publish build succeeded event: %w", err)
	}
	if err := c.publishProgressEvent(ctx, c.cfg.DeploymentExchange, c.cfg.DeploymentBuildSucceededRoutingKey, newBuildSucceededDeploymentEvent(event, result)); err != nil {
		return actionNackRequeue, fmt.Errorf("publish deployment build succeeded event: %w", err)
	}
	return actionAck, nil
}

func makeBuildResultEvents(serviceKey string, service any, progressKey string, progress any) (buildResultEvents, error) {
	s, e := json.Marshal(service)
	if e != nil {
		return buildResultEvents{}, e
	}
	p, e := json.Marshal(progress)
	return buildResultEvents{serviceKey, s, progressKey, p}, e
}
func (c *BuildConsumer) publishStored(raw json.RawMessage) (deliveryAction, error) {
	var b buildResultEvents
	if err := json.Unmarshal(raw, &b); err != nil {
		return actionNackDrop, fmt.Errorf("decode stored build result: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return c.publishBundle(ctx, b)
}
func (c *BuildConsumer) publishBundle(ctx context.Context, b buildResultEvents) (deliveryAction, error) {
	var s, p any
	if err := json.Unmarshal(b.ServiceEvent, &s); err != nil {
		return actionNackDrop, err
	}
	if err := json.Unmarshal(b.ProgressEvent, &p); err != nil {
		return actionNackDrop, err
	}
	if err := c.publishEvent(ctx, c.cfg.RabbitMQExchange, b.ServiceRoutingKey, s); err != nil {
		return actionNackRequeue, fmt.Errorf("publish stored service event: %w", err)
	}
	if err := c.publishProgressEvent(ctx, c.cfg.DeploymentExchange, b.ProgressRoutingKey, p); err != nil {
		return actionNackRequeue, fmt.Errorf("publish stored progress event: %w", err)
	}
	return actionAck, nil
}
func (c *BuildConsumer) startLeaseRenewal(ctx context.Context, id string) func() {
	if c.ledger == nil {
		return func() {}
	}
	renewCtx, cancel := context.WithCancel(ctx)
	interval := c.cfg.LeaseDuration / 3
	if interval <= 0 {
		interval = time.Minute
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-renewCtx.Done():
				return
			case <-ticker.C:
				_ = c.ledger.Renew(renewCtx, id, "build", c.cfg.LeaseDuration)
			}
		}
	}()
	return cancel
}

func declareRetryTopology(ch *amqp091.Channel, cfg Config) error {
	if _, err := ch.QueueDeclare(cfg.DLQ, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare build dlq: %w", err)
	}
	for i, d := range cfg.RetryBackoffs {
		name := fmt.Sprintf("%s.%d", cfg.RetryQueuePrefix, i+1)
		args := amqp091.Table{"x-message-ttl": int32(d / time.Millisecond), "x-dead-letter-exchange": cfg.RabbitMQExchange, "x-dead-letter-routing-key": cfg.ApplicationRoutingKey}
		if _, err := ch.QueueDeclare(name, true, false, false, false, args); err != nil {
			return fmt.Errorf("declare retry queue: %w", err)
		}
	}
	return nil
}
func (c *BuildConsumer) scheduleRetry(msg amqp091.Delivery, cause error, countAttempt bool) error {
	attempt := headerAttempt(msg.Headers)
	retryPersisted := false
	if c.ledger != nil {
		var event ServiceEvent[ApplicationBuildRequestedPayload]
		if json.Unmarshal(msg.Body, &event) == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			persistedAttempt, persistErr := c.ledger.RecordRetry(ctx, event.DeploymentID, "build", safeReason(cause))
			cancel()
			if persistErr != nil {
				log.Printf("persist retry attempt failed; using message header fallback: %v", persistErr)
				attempt++
			} else {
				retryPersisted = true
				if persistedAttempt > attempt {
					attempt = persistedAttempt
				}
			}
		}
	} else {
		attempt++
	}
	if attempt >= c.cfg.MaxAttempts {
		if countAttempt && !errors.Is(cause, errStoredResultPublish) {
			if err := c.emitExhausted(msg.Body, cause); err != nil {
				return err
			}
		}
		return c.copyToDLQWithAttempt(msg, cause, attempt)
	}
	if countAttempt {
		if err := c.emitRetrying(msg.Body, attempt, cause); err != nil {
			return err
		}
	}
	idx := attempt
	if idx > 0 {
		idx--
	}
	if idx >= len(c.cfg.RetryBackoffs) {
		idx = len(c.cfg.RetryBackoffs) - 1
	}
	headers := cloneHeaders(msg.Headers)
	headers["x-shiply-attempt"] = int32(attempt)
	headers["x-shiply-retry-reason"] = safeReason(cause)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	// Release the execution lease before publishing the delayed copy. If the
	// publish fails, broker redelivery can reacquire the stage and the durable
	// retry counter still prevents an immediate, unbounded requeue loop.
	if countAttempt {
		if err := c.releaseClaim(ctx, msg.Body); err != nil {
			log.Printf("release build claim before retry failed: %v", err)
		}
	}
	publishing := amqp091.Publishing{ContentType: msg.ContentType, DeliveryMode: amqp091.Persistent, Headers: headers, Body: msg.Body}
	retryQueue := fmt.Sprintf("%s.%d", c.cfg.RetryQueuePrefix, idx+1)
	if c.pub != nil {
		if err := c.pub.Publish(ctx, "", retryQueue, publishing); err == nil {
			return nil
		} else {
			log.Printf("retry publish on publisher channel failed; trying consumer channel: %v", err)
		}
	}
	if c.ch != nil {
		if err := c.ch.PublishWithContext(ctx, "", retryQueue, false, false, publishing); err == nil {
			return nil
		} else if retryPersisted {
			return fmt.Errorf("publish durable retry: %w", err)
		} else {
			return fmt.Errorf("publish retry: %w", err)
		}
	}
	return errors.New("rabbitmq retry publisher is not initialized")
}
func (c *BuildConsumer) emitRetrying(body []byte, attempt int, cause error) error {
	var event ServiceEvent[ApplicationBuildRequestedPayload]
	if json.Unmarshal(body, &event) != nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return c.publishProgressEvent(ctx, c.cfg.DeploymentExchange, c.cfg.DeploymentBuildRetryingRoutingKey, newBuildRetryingDeploymentEvent(event, attempt, cause))
}
func (c *BuildConsumer) copyToDLQ(msg amqp091.Delivery, cause error) error {
	return c.copyToDLQWithAttempt(msg, cause, headerAttempt(msg.Headers))
}
func (c *BuildConsumer) copyToDLQWithAttempt(msg amqp091.Delivery, cause error, attempt int) error {
	h := cloneHeaders(msg.Headers)
	h["x-shiply-dead-letter-reason"] = safeReason(cause)
	h["x-shiply-attempt"] = int32(attempt)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if c.pub == nil {
		return errors.New("rabbitmq publisher is not initialized")
	}
	return c.pub.Publish(ctx, "", c.cfg.DLQ, amqp091.Publishing{ContentType: msg.ContentType, DeliveryMode: amqp091.Persistent, Headers: h, Body: msg.Body})
}

func (c *BuildConsumer) releaseClaim(ctx context.Context, body []byte) error {
	if c.ledger == nil {
		return nil
	}
	var event ServiceEvent[ApplicationBuildRequestedPayload]
	if err := json.Unmarshal(body, &event); err != nil {
		return err
	}
	return c.ledger.Release(ctx, event.DeploymentID, "build")
}
func headerAttempt(h amqp091.Table) int {
	switch v := h["x-shiply-attempt"].(type) {
	case int32:
		return int(v)
	case int64:
		return int(v)
	case int:
		return v
	}
	return 0
}
func cloneHeaders(h amqp091.Table) amqp091.Table {
	r := amqp091.Table{}
	for k, v := range h {
		r[k] = v
	}
	return r
}
func safeReason(err error) string {
	if err == nil {
		return "unspecified"
	}
	return sanitizeStoredText(redact(err.Error()))
}
func (c *BuildConsumer) emitExhausted(body []byte, cause error) error {
	var event ServiceEvent[ApplicationBuildRequestedPayload]
	if json.Unmarshal(body, &event) != nil {
		return nil
	}
	action, err := c.completeFailure(event, BuildResult{}, fmt.Errorf("retry attempts exhausted: %w", cause))
	if !action.ack {
		return err
	}
	return nil
}

func (c *BuildConsumer) completeFailure(
	event ServiceEvent[ApplicationBuildRequestedPayload],
	result BuildResult,
	buildErr error,
) (deliveryAction, error) {
	failureEvent := newBuildFailedEvent(event, result, buildErr)
	progressEvent := newBuildFailedDeploymentEvent(event, result, buildErr)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if c.ledger != nil {
		bundle, err := makeBuildResultEvents(c.cfg.BuildFailedRoutingKey, failureEvent, c.cfg.DeploymentBuildFailedRoutingKey, progressEvent)
		if err != nil {
			return actionNackRequeue, err
		}
		if err := c.ledger.Fail(ctx, event.DeploymentID, "build", bundle); err != nil {
			return actionNackRequeue, fmt.Errorf("persist build failure: %w", err)
		}
	}

	if err := c.publishEvent(ctx, c.cfg.RabbitMQExchange, c.cfg.BuildFailedRoutingKey, failureEvent); err != nil {
		return actionNackRequeue, fmt.Errorf("%v; publish build failed event: %w", buildErr, err)
	}
	if err := c.publishProgressEvent(ctx, c.cfg.DeploymentExchange, c.cfg.DeploymentBuildFailedRoutingKey, progressEvent); err != nil {
		return actionNackRequeue, fmt.Errorf("%v; publish deployment build failed event: %w", buildErr, err)
	}

	return actionAck, buildErr
}

func (c *BuildConsumer) publishProgressEvent(ctx context.Context, exchange, routingKey string, event any) error {
	if c.publishFn != nil {
		return c.publishFn(ctx, exchange, routingKey, event)
	}
	if c.pub == nil {
		return errors.New("rabbitmq publisher is not initialized")
	}
	return c.pub.PublishProgressJSON(ctx, exchange, routingKey, event)
}

func (c *BuildConsumer) publishEvent(ctx context.Context, exchange, routingKey string, event any) error {
	publish := c.publishFn
	if publish == nil {
		publish = c.publishJSONEvent
	}
	return publish(ctx, exchange, routingKey, event)
}

func (c *BuildConsumer) publishJSONEvent(ctx context.Context, exchange, routingKey string, event any) error {
	if c.pub == nil {
		return errors.New("rabbitmq publisher is not initialized")
	}
	return c.pub.PublishJSON(ctx, exchange, routingKey, event)
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

func validateBuildPayload(payload ApplicationBuildRequestedPayload) error {
	if strings.TrimSpace(payload.CloneURL) == "" && strings.TrimSpace(payload.RepositoryURL) == "" {
		return errors.New("missing clone url")
	}
	return nil
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
