```command for running the build service on the vps

docker run -d \
  --name shiply-build-service \
  -p 8081:8081 \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e PORT=8081 \
  -e RABBITMQ_URL='amqp://guest:guest@localhost:5672/' \
  -e RABBITMQ_EXCHANGE='shiply.services' \
  -e RABBITMQ_APP_BUILD_QUEUE='shiply.app.build' \
  -e RABBITMQ_APP_BUILD_ROUTING_KEY='app.build.requested' \
  -e RABBITMQ_BUILD_SUCCEEDED_ROUTING_KEY='build.succeeded' \
  -e RABBITMQ_BUILD_FAILED_ROUTING_KEY='build.failed' \
  -e GITHUB_APP_ID='your-app-id' \
  -e GITHUB_APP_PRIVATE_KEY_BASE64='your-base64-private-key' \
  -e CONTAINER_REGISTRY_HOST='ghcr.io' \
  -e CONTAINER_REGISTRY_REPOSITORY_PREFIX='your-org' \
  -e CONTAINER_REGISTRY_USERNAME='your-username' \
  -e CONTAINER_REGISTRY_PASSWORD='your-password' \
  shiply-build-service:latest