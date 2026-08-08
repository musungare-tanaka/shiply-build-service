# -------- Build stage --------
FROM golang:1.25.1-bookworm AS build
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /app/build-service ./main.go

# -------- Runtime stage --------
FROM debian:bookworm-slim
WORKDIR /app

ARG NIXPACKS_VERSION=1.39.1

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl git gnupg \
    && install -m 0755 -d /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc \
    && chmod a+r /etc/apt/keyrings/docker.asc \
    && . /etc/os-release \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian ${VERSION_CODENAME} stable" > /etc/apt/sources.list.d/docker.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends docker-ce-cli \
    && curl -fsSL "https://github.com/railwayapp/nixpacks/releases/download/v${NIXPACKS_VERSION}/nixpacks-x86_64-unknown-linux-musl.tar.gz" \
        | tar -xz -C /usr/local/bin nixpacks \
    && chmod 0755 /usr/local/bin/nixpacks \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /app/build-service /app/build-service

EXPOSE 8081

# This container must be started with /var/run/docker.sock mounted, and the
# container's effective group access must match the host socket GID or docker
# CLI calls will fail at runtime.
ENTRYPOINT ["/app/build-service"]
