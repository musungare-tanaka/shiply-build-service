# Build and Deployment Service

## Purpose
- Consumes `app.build.requested` events from RabbitMQ.
- Prepares source code for a deployment by cloning the requested repository.
- Builds a container image with Nixpacks, or falls back to a repo-root `Dockerfile`.
- Pushes the resulting image to a configurable container registry.
- Publishes `build.succeeded` or `build.failed` events back to RabbitMQ.
- Carries `projectSlug`, `serviceSlug`, and `containerPort` through to `build.succeeded` so downstream deployment workers can route traffic correctly.
- Uses short-lived GitHub App installation tokens for private GitHub repositories.

## Environment variables
- `PORT=8081`
- `RABBITMQ_URL=amqp://guest:guest@localhost:5672/`
- `RABBITMQ_EXCHANGE=shiply.services`
- `RABBITMQ_APP_BUILD_QUEUE=shiply.app.build`
- `RABBITMQ_APP_BUILD_ROUTING_KEY=app.build.requested`
- `RABBITMQ_BUILD_SUCCEEDED_ROUTING_KEY=build.succeeded`
- `RABBITMQ_BUILD_FAILED_ROUTING_KEY=build.failed`
- `RABBITMQ_DEPLOYMENT_EXCHANGE=deployment.events`
- `RABBITMQ_DEPLOYMENT_BUILD_STARTED_ROUTING_KEY=deployment.build.started`
- `RABBITMQ_DEPLOYMENT_BUILD_SUCCEEDED_ROUTING_KEY=deployment.build.succeeded`
- `RABBITMQ_DEPLOYMENT_BUILD_FAILED_ROUTING_KEY=deployment.build.failed`
- `MAX_DELIVERY_ATTEMPTS=5` (the initial delivery is attempt one)
- `RETRY_BACKOFF_SCHEDULE=10s,30s,2m,5m`
- `RABBITMQ_RETRY_QUEUE_PREFIX=shiply.app.build.retry`
- `RABBITMQ_BUILD_DLQ=shiply.app.build.dlq`
- `GITHUB_APP_ID`
- `GITHUB_APP_PRIVATE_KEY` or `GITHUB_APP_PRIVATE_KEY_BASE64`
- `CONTAINER_REGISTRY_HOST`
- `CONTAINER_REGISTRY_REPOSITORY_PREFIX`
- `CONTAINER_REGISTRY_USERNAME`
- `CONTAINER_REGISTRY_PASSWORD`

## Runtime dependencies
- `git`
- `nixpacks`
- `docker`

## Build behavior
- Claim and build failures are retried through durable TTL queues. Retry exhaustion is persisted as `FAILED` before `build.failed` and `deployment.build.failed` are published.
- The worker tags images as `<registry-host>/<optional-prefix>/<project-id>/<service-id>:<short-commit-sha>`.
- The worker tries `nixpacks build <repoDir> --name <imageRef>` first.
- If Nixpacks fails, the worker checks for a repo-root `Dockerfile` and falls back to `docker build -t <imageRef> <repoDir>`.
- If you need a rootless or cluster-native build flow, `builder.go` is the place to replace the Docker CLI steps with `docker buildx`, Kaniko, or another remote builder.

## Security notes
- The worker never stores credential-bearing clone URLs.
- Private GitHub credentials are supplied to `git` through a temporary `GIT_ASKPASS` script.
- Temporary credential files are deleted immediately after clone.
- Docker registry credentials are supplied to `docker login` through stdin and a temporary `DOCKER_CONFIG` directory inside the build workdir.
- Logs redact GitHub token-like output.
