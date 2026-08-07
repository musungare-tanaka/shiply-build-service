# Build and Deployment Service

## Purpose
- Consumes `app.build.requested` events from RabbitMQ.
- Prepares source code for a deployment by cloning the requested repository.
- Uses short-lived GitHub App installation tokens for private GitHub repositories.

## Environment variables
- `PORT=8081`
- `RABBITMQ_URL=amqp://guest:guest@localhost:5672/`
- `RABBITMQ_EXCHANGE=shiply.services`
- `RABBITMQ_APP_BUILD_QUEUE=shiply.app.build`
- `RABBITMQ_APP_BUILD_ROUTING_KEY=app.build.requested`
- `GITHUB_APP_ID`
- `GITHUB_APP_PRIVATE_KEY` or `GITHUB_APP_PRIVATE_KEY_BASE64`

## Security notes
- The worker never stores credential-bearing clone URLs.
- Private GitHub credentials are supplied to `git` through a temporary `GIT_ASKPASS` script.
- Temporary credential files are deleted immediately after clone.
- Logs redact GitHub token-like output.
