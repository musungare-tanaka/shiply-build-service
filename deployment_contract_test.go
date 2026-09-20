package main

import (
	"encoding/json"
	"testing"
)

func TestDeploymentProgressContract(t *testing.T) {
	var request ServiceEvent[ApplicationBuildRequestedPayload]
	if err := json.Unmarshal(mustMarshalBuildRequest(t), &request); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		key   string
		event DeploymentEvent
	}{
		{deploymentBuildStartedEventType, newBuildStartedDeploymentEvent(request)},
		{deploymentBuildSucceededEventType, newBuildSucceededDeploymentEvent(request, BuildResult{ImageTag: "registry/app:sha"})},
		{deploymentBuildFailedEventType, newBuildFailedDeploymentEvent(request, BuildResult{}, errContract{})},
	}
	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			if tc.event.EventType != tc.key {
				t.Fatalf("routing key %q != eventType %q", tc.key, tc.event.EventType)
			}
			if tc.event.EventID != deploymentEventID(tc.event.DeploymentID, "build", tc.event.Status) {
				t.Fatal("event ID is not deterministic")
			}
			body, err := json.Marshal(tc.event)
			if err != nil || len(body) == 0 || tc.event.DeploymentID == "" || tc.event.ProjectID == "" || tc.event.Timestamp.IsZero() {
				t.Fatalf("invalid progress payload: %s (%v)", body, err)
			}
		})
	}
}

type errContract struct{}

func (errContract) Error() string { return "build failed" }
