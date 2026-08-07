package main

import "os"

type Config struct {
	HTTPPort              string
	RabbitMQURL           string
	RabbitMQExchange      string
	ApplicationBuildQueue string
	ApplicationRoutingKey string
	GitHubAppID           string
	GitHubPrivateKey      string
	GitHubPrivateKeyB64   string
}

func loadConfig() Config {
	return Config{
		HTTPPort:              envOrDefault("PORT", "8081"),
		RabbitMQURL:           envOrDefault("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
		RabbitMQExchange:      envOrDefault("RABBITMQ_EXCHANGE", "shiply.services"),
		ApplicationBuildQueue: envOrDefault("RABBITMQ_APP_BUILD_QUEUE", "shiply.app.build"),
		ApplicationRoutingKey: envOrDefault("RABBITMQ_APP_BUILD_ROUTING_KEY", "app.build.requested"),
		GitHubAppID:           os.Getenv("GITHUB_APP_ID"),
		GitHubPrivateKey:      os.Getenv("GITHUB_APP_PRIVATE_KEY"),
		GitHubPrivateKeyB64:   os.Getenv("GITHUB_APP_PRIVATE_KEY_BASE64"),
	}
}

func envOrDefault(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
