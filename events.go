package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"time"
)

const (
	buildSucceededEventType = "build.succeeded"
	buildFailedEventType    = "build.failed"
)

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
		EventID:   newEventID(),
		EventType: buildSucceededEventType,
		Timestamp: marshalTimestamp(time.Now().UTC()),
		ProjectID: request.ProjectID,
		ServiceID: request.ServiceID,
		UserID:    request.UserID,
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
		EventID:   newEventID(),
		EventType: buildFailedEventType,
		Timestamp: marshalTimestamp(time.Now().UTC()),
		ProjectID: request.ProjectID,
		ServiceID: request.ServiceID,
		UserID:    request.UserID,
		Payload: BuildFailedPayload{
			ImageTag:     result.ImageTag,
			CommitSHA:    result.CommitSHA,
			Builder:      result.Builder,
			ErrorMessage: redact(err.Error()),
		},
	}
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
