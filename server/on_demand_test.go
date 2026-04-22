package server

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	githubv63 "github.com/google/go-github/v63/github"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOnDemandWebhookScalesMatchingPool(t *testing.T) {
	server, poolA, _ := newOnDemandTestServer(t)
	controller := newOnDemandController(server)

	payload := []byte(`{
		"action":"queued",
		"organization":{"login":"test-org"},
		"workflow_job":{"id":101,"labels":["self-hosted","fireactions","fire-1x1"]}
	}`)
	request := newSignedWebhookRequest(t, server.config.GitHub.WebhookSecret, payload)
	recorder := httptest.NewRecorder()

	controller.HandleGitHubWebhook(recorder, request)

	require.Equal(t, http.StatusAccepted, recorder.Code)
	assert.Equal(t, 1, poolA.GetDesiredReplicas())

	payload = []byte(`{
		"action":"completed",
		"organization":{"login":"test-org"},
		"workflow_job":{"id":101,"labels":["self-hosted","fireactions","fire-1x1"]}
	}`)
	request = newSignedWebhookRequest(t, server.config.GitHub.WebhookSecret, payload)
	recorder = httptest.NewRecorder()

	controller.HandleGitHubWebhook(recorder, request)

	require.Equal(t, http.StatusAccepted, recorder.Code)
	assert.Equal(t, 0, poolA.GetDesiredReplicas())
}

func TestOnDemandWebhookScalesPoolWhenJobOnlyRequestsPoolLabel(t *testing.T) {
	server, poolA, _ := newOnDemandTestServer(t)
	controller := newOnDemandController(server)

	payload := []byte(`{
		"action":"queued",
		"organization":{"login":"test-org"},
		"workflow_job":{"id":102,"labels":["fire-1x1"]}
	}`)
	request := newSignedWebhookRequest(t, server.config.GitHub.WebhookSecret, payload)
	recorder := httptest.NewRecorder()

	controller.HandleGitHubWebhook(recorder, request)

	require.Equal(t, http.StatusAccepted, recorder.Code)
	assert.Equal(t, 1, poolA.GetDesiredReplicas())
}

func TestOnDemandWebhookRejectsInvalidSignature(t *testing.T) {
	server, poolA, _ := newOnDemandTestServer(t)
	controller := newOnDemandController(server)

	payload := []byte(`{
		"action":"queued",
		"organization":{"login":"test-org"},
		"workflow_job":{"id":101,"labels":["self-hosted","fireactions","fire-1x1"]}
	}`)
	request := httptest.NewRequest(http.MethodPost, "/webhooks/github", bytes.NewReader(payload))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-GitHub-Event", "workflow_job")
	request.Header.Set(githubv63.SHA256SignatureHeader, "sha256=invalid")

	recorder := httptest.NewRecorder()
	controller.HandleGitHubWebhook(recorder, request)

	assert.Equal(t, http.StatusUnauthorized, recorder.Code)
	assert.Equal(t, 0, poolA.GetDesiredReplicas())
}

func TestOnDemandReconcileRebuildsPoolDemand(t *testing.T) {
	server, _, poolB := newOnDemandTestServer(t)
	controller := newOnDemandController(server)
	controller.installationIDs["test-org"] = 1
	controller.listRepositoriesFn = func(ctx context.Context, installationID int64) ([]*githubv63.Repository, error) {
		return []*githubv63.Repository{{
			Name: githubv63.String("repo"),
			Owner: &githubv63.User{
				Login: githubv63.String("test-org"),
			},
		}}, nil
	}
	controller.listWorkflowRunsFn = func(ctx context.Context, installationID int64, owner, repo, status string) ([]*githubv63.WorkflowRun, error) {
		if status != "queued" {
			return nil, nil
		}

		return []*githubv63.WorkflowRun{{
			ID: githubv63.Int64(55),
		}}, nil
	}
	controller.listWorkflowJobsFn = func(ctx context.Context, installationID int64, owner, repo string, runID int64) ([]*githubv63.WorkflowJob, error) {
		return []*githubv63.WorkflowJob{{
			ID:     githubv63.Int64(201),
			Status: githubv63.String("queued"),
			Labels: []string{"fire-4x16"},
		}}, nil
	}

	require.NoError(t, controller.reconcileOnce(context.Background()))
	assert.Equal(t, 1, poolB.GetDesiredReplicas())
}

func TestOnDemandReconcilePreservesWebhookTrackedJobAcrossTransientPollMiss(t *testing.T) {
	server, poolA, _ := newOnDemandTestServer(t)
	controller := newOnDemandController(server)
	controller.installationIDs["test-org"] = 1

	now := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	controller.nowFn = func() time.Time { return now }
	controller.listRepositoriesFn = func(ctx context.Context, installationID int64) ([]*githubv63.Repository, error) {
		return []*githubv63.Repository{{
			Name: githubv63.String("repo"),
			Owner: &githubv63.User{
				Login: githubv63.String("test-org"),
			},
		}}, nil
	}
	controller.listWorkflowRunsFn = func(ctx context.Context, installationID int64, owner, repo, status string) ([]*githubv63.WorkflowRun, error) {
		return nil, nil
	}
	controller.listWorkflowJobsFn = func(ctx context.Context, installationID int64, owner, repo string, runID int64) ([]*githubv63.WorkflowJob, error) {
		return nil, nil
	}

	err := controller.processWorkflowJobEvent(&githubv63.WorkflowJobEvent{
		Action: githubv63.String("in_progress"),
		Org:    &githubv63.Organization{Login: githubv63.String("test-org")},
		WorkflowJob: &githubv63.WorkflowJob{
			ID:     githubv63.Int64(301),
			Labels: []string{"self-hosted", "fireactions", "fire-1x1"},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, 1, poolA.GetDesiredReplicas())

	now = now.Add(onDemandReconcileInterval)
	require.NoError(t, controller.reconcileOnce(context.Background()))
	assert.Equal(t, 1, poolA.GetDesiredReplicas())

	now = now.Add(onDemandMissingJobGrace + time.Second)
	require.NoError(t, controller.reconcileOnce(context.Background()))
	assert.Equal(t, 0, poolA.GetDesiredReplicas())
}

func TestOnDemandCompletedWebhookBeatsStaleReconcileResult(t *testing.T) {
	server, poolA, _ := newOnDemandTestServer(t)
	controller := newOnDemandController(server)
	controller.installationIDs["test-org"] = 1

	now := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	controller.nowFn = func() time.Time { return now }
	controller.listRepositoriesFn = func(ctx context.Context, installationID int64) ([]*githubv63.Repository, error) {
		return []*githubv63.Repository{{
			Name: githubv63.String("repo"),
			Owner: &githubv63.User{
				Login: githubv63.String("test-org"),
			},
		}}, nil
	}
	controller.listWorkflowRunsFn = func(ctx context.Context, installationID int64, owner, repo, status string) ([]*githubv63.WorkflowRun, error) {
		if status != "in_progress" {
			return nil, nil
		}

		return []*githubv63.WorkflowRun{{
			ID: githubv63.Int64(88),
		}}, nil
	}
	controller.listWorkflowJobsFn = func(ctx context.Context, installationID int64, owner, repo string, runID int64) ([]*githubv63.WorkflowJob, error) {
		return []*githubv63.WorkflowJob{{
			ID:     githubv63.Int64(302),
			Status: githubv63.String("in_progress"),
			Labels: []string{"self-hosted", "fireactions", "fire-1x1"},
		}}, nil
	}

	err := controller.processWorkflowJobEvent(&githubv63.WorkflowJobEvent{
		Action: githubv63.String("in_progress"),
		Org:    &githubv63.Organization{Login: githubv63.String("test-org")},
		WorkflowJob: &githubv63.WorkflowJob{
			ID:     githubv63.Int64(302),
			Labels: []string{"self-hosted", "fireactions", "fire-1x1"},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, 1, poolA.GetDesiredReplicas())

	now = now.Add(time.Second)
	err = controller.processWorkflowJobEvent(&githubv63.WorkflowJobEvent{
		Action: githubv63.String("completed"),
		Org:    &githubv63.Organization{Login: githubv63.String("test-org")},
		WorkflowJob: &githubv63.WorkflowJob{
			ID:     githubv63.Int64(302),
			Labels: []string{"self-hosted", "fireactions", "fire-1x1"},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, 0, poolA.GetDesiredReplicas())

	now = now.Add(time.Second)
	require.NoError(t, controller.reconcileOnce(context.Background()))
	assert.Equal(t, 0, poolA.GetDesiredReplicas())
}

func newOnDemandTestServer(t *testing.T) (*Server, *Pool, *Pool) {
	t.Helper()

	poolA := newTestPool(t, "fire-1x1", 1024, 1, NewCapacityManager(nil))
	poolB := newTestPool(t, "fire-4x16", 16384, 4, NewCapacityManager(nil))

	logger := zerolog.New(bytes.NewBuffer(nil))
	server := &Server{
		config: &Config{
			OnDemand: true,
			GitHub: &GitHubConfig{
				WebhookSecret: "secret",
			},
			Metrics: &MetricsConfig{
				Address: "127.0.0.1:8081",
			},
		},
		pools: map[string]*Pool{
			poolA.config.Name: poolA,
			poolB.config.Name: poolB,
		},
		l:      &sync.Mutex{},
		logger: &logger,
	}

	return server, poolA, poolB
}

func newSignedWebhookRequest(t *testing.T, secret string, payload []byte) *http.Request {
	t.Helper()

	mac := hmac.New(sha256.New, []byte(secret))
	_, err := mac.Write(payload)
	require.NoError(t, err)

	request := httptest.NewRequest(http.MethodPost, "/webhooks/github", bytes.NewReader(payload))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-GitHub-Event", "workflow_job")
	request.Header.Set(githubv63.SHA256SignatureHeader, "sha256="+hex.EncodeToString(mac.Sum(nil)))
	return request
}
