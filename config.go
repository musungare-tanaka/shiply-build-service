package main

import "os"

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

func envOrDefault(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
