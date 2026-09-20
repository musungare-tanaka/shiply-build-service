package main

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type RegistryConfig struct {
	Host             string
	RepositoryPrefix string
	Username         string
	Password         string
}

type Config struct {
	HTTPPort                           string
	RabbitMQURL                        string
	RabbitMQExchange                   string
	DeploymentExchange                 string
	ApplicationBuildQueue              string
	ApplicationRoutingKey              string
	BuildSucceededRoutingKey           string
	BuildFailedRoutingKey              string
	BuildStartedRoutingKey             string
	DeploymentBuildStartedRoutingKey   string
	DeploymentBuildSucceededRoutingKey string
	DeploymentBuildFailedRoutingKey    string
	DeploymentBuildRetryingRoutingKey  string
	DatabaseURL                        string
	LedgerSchema                       string
	LeaseDuration                      time.Duration
	MaxAttempts                        int
	RetryBackoffs                      []time.Duration
	RetryQueuePrefix                   string
	DLQ                                string
	GitHubAppID                        string
	GitHubPrivateKey                   string
	GitHubPrivateKeyB64                string
	Registry                           RegistryConfig
}

func loadConfig() Config {
	return Config{
		HTTPPort:                           envOrDefault("PORT", "8081"),
		RabbitMQURL:                        envOrDefault("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
		RabbitMQExchange:                   envOrDefault("RABBITMQ_EXCHANGE", "shiply.services"),
		DeploymentExchange:                 envOrDefault("RABBITMQ_DEPLOYMENT_EXCHANGE", "deployment.events"),
		ApplicationBuildQueue:              envOrDefault("RABBITMQ_APP_BUILD_QUEUE", "shiply.app.build"),
		ApplicationRoutingKey:              envOrDefault("RABBITMQ_APP_BUILD_ROUTING_KEY", "app.build.requested"),
		BuildStartedRoutingKey:             envOrDefault("RABBITMQ_BUILD_STARTED_ROUTING_KEY", "build.started"),
		BuildSucceededRoutingKey:           envOrDefault("RABBITMQ_BUILD_SUCCEEDED_ROUTING_KEY", "build.succeeded"),
		BuildFailedRoutingKey:              envOrDefault("RABBITMQ_BUILD_FAILED_ROUTING_KEY", "build.failed"),
		DeploymentBuildStartedRoutingKey:   envOrDefault("RABBITMQ_DEPLOYMENT_BUILD_STARTED_ROUTING_KEY", deploymentBuildStartedEventType),
		DeploymentBuildSucceededRoutingKey: envOrDefault("RABBITMQ_DEPLOYMENT_BUILD_SUCCEEDED_ROUTING_KEY", deploymentBuildSucceededEventType),
		DeploymentBuildFailedRoutingKey:    envOrDefault("RABBITMQ_DEPLOYMENT_BUILD_FAILED_ROUTING_KEY", deploymentBuildFailedEventType),
		DeploymentBuildRetryingRoutingKey:  envOrDefault("RABBITMQ_DEPLOYMENT_BUILD_RETRYING_ROUTING_KEY", "deployment.build.retrying"),
		DatabaseURL:                        os.Getenv("DATABASE_URL"),
		LedgerSchema:                       envOrDefault("STAGE_LEDGER_SCHEMA", "build_service"),
		LeaseDuration:                      durationOrDefault("STAGE_LEASE_DURATION", 35*time.Minute),
		MaxAttempts:                        intOrDefault("MAX_DELIVERY_ATTEMPTS", 5),
		RetryBackoffs:                      durationsOrDefault("RETRY_BACKOFF_SCHEDULE", []time.Duration{10 * time.Second, 30 * time.Second, 2 * time.Minute, 5 * time.Minute}),
		RetryQueuePrefix:                   envOrDefault("RABBITMQ_RETRY_QUEUE_PREFIX", "shiply.app.build.retry"),
		DLQ:                                envOrDefault("RABBITMQ_BUILD_DLQ", "shiply.app.build.dlq"),
		GitHubAppID:                        os.Getenv("GITHUB_APP_ID"),
		GitHubPrivateKey:                   os.Getenv("GITHUB_APP_PRIVATE_KEY"),
		GitHubPrivateKeyB64:                os.Getenv("GITHUB_APP_PRIVATE_KEY_BASE64"),
		Registry: RegistryConfig{
			Host:             os.Getenv("CONTAINER_REGISTRY_HOST"),
			RepositoryPrefix: os.Getenv("CONTAINER_REGISTRY_REPOSITORY_PREFIX"),
			Username:         os.Getenv("CONTAINER_REGISTRY_USERNAME"),
			Password:         os.Getenv("CONTAINER_REGISTRY_PASSWORD"),
		},
	}
}

func durationOrDefault(key string, fallback time.Duration) time.Duration {
	v, err := time.ParseDuration(strings.TrimSpace(os.Getenv(key)))
	if err != nil || v <= 0 {
		return fallback
	}
	return v
}
func intOrDefault(key string, fallback int) int {
	v, err := strconv.Atoi(strings.TrimSpace(os.Getenv(key)))
	if err != nil || v < 1 {
		return fallback
	}
	return v
}
func durationsOrDefault(key string, fallback []time.Duration) []time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parts := strings.Split(value, ",")
	result := make([]time.Duration, 0, len(parts))
	for _, part := range parts {
		d, err := time.ParseDuration(strings.TrimSpace(part))
		if err != nil || d <= 0 {
			return fallback
		}
		result = append(result, d)
	}
	return result
}

func envOrDefault(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
