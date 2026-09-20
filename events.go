package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
)

const (
	buildSucceededEventType           = "build.succeeded"
	buildFailedEventType              = "build.failed"
	deploymentBuildStartedEventType   = "deployment.build.started"
	deploymentBuildSucceededEventType = "deployment.build.succeeded"
	deploymentBuildFailedEventType    = "deployment.build.failed"
	buildingStatus                    = "BUILDING"
	buildSucceededStatus              = "BUILD_SUCCEEDED"
	buildFailedStatus                 = "BUILD_FAILED"
)

type ServiceEvent[T any] struct {
	EventID      string          `json:"eventId"`
	EventType    string          `json:"eventType"`
	Timestamp    json.RawMessage `json:"timestamp"`
	DeploymentID string          `json:"deploymentId,omitempty"`
	ProjectID    string          `json:"projectId"`
	ServiceID    string          `json:"serviceId"`
	ServiceName  string          `json:"serviceName,omitempty"`
	UserID       string          `json:"userId"`
	Payload      T               `json:"payload"`
}

type DeploymentEvent struct {
	EventID      string         `json:"eventId"`
	DeploymentID string         `json:"deploymentId"`
	ProjectID    string         `json:"projectId"`
	ServiceName  string         `json:"serviceName"`
	EventType    string         `json:"eventType"`
	Status       string         `json:"status"`
	Timestamp    time.Time      `json:"timestamp"`
	Metadata     map[string]any `json:"metadata,omitempty"`
}

type BuildSucceededPayload struct {
	ImageTag      string `json:"imageTag"`
	CommitSHA     string `json:"commitSha,omitempty"`
	Builder       string `json:"builder,omitempty"`
	ProjectSlug   string `json:"projectSlug,omitempty"`
	ServiceSlug   string `json:"serviceSlug,omitempty"`
	ContainerPort int32  `json:"containerPort,omitempty"`
}

type BuildFailedPayload struct {
	ImageTag     string `json:"imageTag,omitempty"`
	CommitSHA    string `json:"commitSha,omitempty"`
	Builder      string `json:"builder,omitempty"`
	ErrorMessage string `json:"errorMessage"`
}

func newBuildSucceededEvent(
	request ServiceEvent[ApplicationBuildRequestedPayload],
	result BuildResult,
) ServiceEvent[BuildSucceededPayload] {
	return ServiceEvent[BuildSucceededPayload]{
		EventID:      newEventID(),
		EventType:    buildSucceededEventType,
		Timestamp:    marshalTimestamp(time.Now().UTC()),
		DeploymentID: request.DeploymentID,
		ProjectID:    request.ProjectID,
		ServiceID:    request.ServiceID,
		ServiceName:  firstNonEmpty(request.ServiceName, request.Payload.ServiceAlias),
		UserID:       request.UserID,
		Payload: BuildSucceededPayload{
			ImageTag:      result.ImageTag,
			CommitSHA:     result.CommitSHA,
			Builder:       result.Builder,
			ProjectSlug:   request.Payload.ProjectSlug,
			ServiceSlug:   request.Payload.ServiceSlug,
			ContainerPort: request.Payload.ContainerPort,
		},
	}
}

func newBuildFailedEvent(
	request ServiceEvent[ApplicationBuildRequestedPayload],
	result BuildResult,
	err error,
) ServiceEvent[BuildFailedPayload] {
	return ServiceEvent[BuildFailedPayload]{
		EventID:      newEventID(),
		EventType:    buildFailedEventType,
		Timestamp:    marshalTimestamp(time.Now().UTC()),
		DeploymentID: request.DeploymentID,
		ProjectID:    request.ProjectID,
		ServiceID:    request.ServiceID,
		ServiceName:  firstNonEmpty(request.ServiceName, request.Payload.ServiceAlias),
		UserID:       request.UserID,
		Payload: BuildFailedPayload{
			ImageTag:     result.ImageTag,
			CommitSHA:    result.CommitSHA,
			Builder:      result.Builder,
			ErrorMessage: redact(err.Error()),
		},
	}
}

func newBuildStartedDeploymentEvent(request ServiceEvent[ApplicationBuildRequestedPayload]) DeploymentEvent {
	return DeploymentEvent{
		EventID:      deploymentEventID(request.DeploymentID, "build", buildingStatus),
		DeploymentID: request.DeploymentID,
		ProjectID:    request.ProjectID,
		ServiceName:  firstNonEmpty(request.ServiceName, request.Payload.ServiceAlias),
		EventType:    deploymentBuildStartedEventType,
		Status:       buildingStatus,
		Timestamp:    time.Now().UTC(),
		Metadata: map[string]any{
			"serviceId":     request.ServiceID,
			"branch":        request.Payload.Branch,
			"repositoryUrl": request.Payload.RepositoryURL,
		},
	}
}

func newBuildSucceededDeploymentEvent(
	request ServiceEvent[ApplicationBuildRequestedPayload],
	result BuildResult,
) DeploymentEvent {
	return DeploymentEvent{
		EventID:      deploymentEventID(request.DeploymentID, "build", buildSucceededStatus),
		DeploymentID: request.DeploymentID,
		ProjectID:    request.ProjectID,
		ServiceName:  firstNonEmpty(request.ServiceName, request.Payload.ServiceAlias),
		EventType:    deploymentBuildSucceededEventType,
		Status:       buildSucceededStatus,
		Timestamp:    time.Now().UTC(),
		Metadata: map[string]any{
			"serviceId":     request.ServiceID,
			"imageTag":      result.ImageTag,
			"commitSha":     result.CommitSHA,
			"builder":       result.Builder,
			"projectSlug":   request.Payload.ProjectSlug,
			"serviceSlug":   request.Payload.ServiceSlug,
			"containerPort": request.Payload.ContainerPort,
		},
	}
}

func newBuildFailedDeploymentEvent(
	request ServiceEvent[ApplicationBuildRequestedPayload],
	result BuildResult,
	err error,
) DeploymentEvent {
	return DeploymentEvent{
		EventID:      deploymentEventID(request.DeploymentID, "build", buildFailedStatus),
		DeploymentID: request.DeploymentID,
		ProjectID:    request.ProjectID,
		ServiceName:  firstNonEmpty(request.ServiceName, request.Payload.ServiceAlias),
		EventType:    deploymentBuildFailedEventType,
		Status:       buildFailedStatus,
		Timestamp:    time.Now().UTC(),
		Metadata: map[string]any{
			"serviceId":     request.ServiceID,
			"imageTag":      result.ImageTag,
			"commitSha":     result.CommitSHA,
			"builder":       result.Builder,
			"errorMessage":  redact(err.Error()),
			"projectSlug":   request.Payload.ProjectSlug,
			"serviceSlug":   request.Payload.ServiceSlug,
			"containerPort": request.Payload.ContainerPort,
		},
	}
}

func deploymentEventID(deploymentID, stage, status string) string {
	sum := sha256.Sum256([]byte(deploymentID + ":" + stage + ":" + status))
	return hex.EncodeToString(sum[:])
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func marshalTimestamp(timestamp time.Time) json.RawMessage {
	payload, err := json.Marshal(timestamp)
	if err != nil {
		return json.RawMessage(`null`)
	}
	return json.RawMessage(payload)
}

func newEventID() string {
	var randomBytes [16]byte
	if _, err := rand.Read(randomBytes[:]); err != nil {
		return time.Now().UTC().Format("20060102150405.000000000")
	}

	randomBytes[6] = (randomBytes[6] & 0x0f) | 0x40
	randomBytes[8] = (randomBytes[8] & 0x3f) | 0x80

	encoded := hex.EncodeToString(randomBytes[:])
	return encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:32]
}
