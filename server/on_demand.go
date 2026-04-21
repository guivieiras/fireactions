package server

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	githubv63 "github.com/google/go-github/v63/github"
	"github.com/rs/zerolog"
)

const (
	onDemandReconcileInterval = 60 * time.Second
)

type jobDemandEntry struct {
	organization string
	poolName     string
}

type onDemandController struct {
	server        *Server
	logger        *zerolog.Logger
	webhookSecret []byte
	pollInterval  time.Duration

	mu              sync.Mutex
	jobs            map[int64]*jobDemandEntry
	installationIDs map[string]int64

	listRepositoriesFn func(context.Context, int64) ([]*githubv63.Repository, error)
	listWorkflowRunsFn func(context.Context, int64, string, string, string) ([]*githubv63.WorkflowRun, error)
	listWorkflowJobsFn func(context.Context, int64, string, string, int64) ([]*githubv63.WorkflowJob, error)
}

func newOnDemandController(server *Server) *onDemandController {
	logger := server.logger.With().Str("component", "on-demand").Logger()
	controller := &onDemandController{
		server:          server,
		logger:          &logger,
		webhookSecret:   []byte(server.config.GitHub.WebhookSecret),
		pollInterval:    onDemandReconcileInterval,
		jobs:            make(map[int64]*jobDemandEntry),
		installationIDs: make(map[string]int64),
	}
	controller.listRepositoriesFn = controller.listRepositories
	controller.listWorkflowRunsFn = controller.listWorkflowRuns
	controller.listWorkflowJobsFn = controller.listWorkflowJobs
	return controller
}

func (c *onDemandController) Initialize(ctx context.Context) error {
	pools := c.server.snapshotPools()
	for _, pool := range pools {
		metricOnDemandJobsActive.WithLabelValues(pool.config.Name, pool.config.Runner.Organization).Set(0)

		if _, ok := c.installationIDs[pool.config.Runner.Organization]; ok {
			continue
		}

		installation, _, err := c.server.github.Apps.FindOrganizationInstallation(ctx, pool.config.Runner.Organization)
		if err != nil {
			return fmt.Errorf("finding GitHub installation for organization %q: %w", pool.config.Runner.Organization, err)
		}

		c.installationIDs[pool.config.Runner.Organization] = installation.GetID()
	}

	return nil
}

func (c *onDemandController) Run(ctx context.Context) {
	go func() {
		c.reconcile(ctx)

		ticker := time.NewTicker(c.pollInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.reconcile(ctx)
			}
		}
	}()
}

func (c *onDemandController) HandleGitHubWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	payload, err := githubv63.ValidatePayload(r, c.webhookSecret)
	if err != nil {
		metricOnDemandWebhookEvents.WithLabelValues("unknown", "invalid_signature").Inc()
		c.logger.Warn().Err(err).Msg("Rejected GitHub webhook with invalid signature")
		http.Error(w, "invalid webhook signature", http.StatusUnauthorized)
		return
	}

	webhookType := githubv63.WebHookType(r)
	if webhookType != "workflow_job" {
		metricOnDemandWebhookEvents.WithLabelValues(webhookType, "ignored").Inc()
		w.WriteHeader(http.StatusAccepted)
		return
	}

	event, err := githubv63.ParseWebHook(webhookType, payload)
	if err != nil {
		metricOnDemandWebhookEvents.WithLabelValues(webhookType, "parse_error").Inc()
		c.logger.Error().Err(err).Msg("Failed to parse GitHub workflow_job webhook")
		http.Error(w, "invalid webhook payload", http.StatusBadRequest)
		return
	}

	workflowJobEvent, ok := event.(*githubv63.WorkflowJobEvent)
	if !ok {
		metricOnDemandWebhookEvents.WithLabelValues(webhookType, "parse_error").Inc()
		http.Error(w, "invalid workflow_job payload", http.StatusBadRequest)
		return
	}

	if err := c.processWorkflowJobEvent(workflowJobEvent); err != nil {
		metricOnDemandWebhookEvents.WithLabelValues(workflowJobEvent.GetAction(), "error").Inc()
		c.logger.Error().Err(err).Msg("Failed to process GitHub workflow_job webhook")
		http.Error(w, "failed to process webhook", http.StatusInternalServerError)
		return
	}

	metricOnDemandWebhookEvents.WithLabelValues(workflowJobEvent.GetAction(), "accepted").Inc()
	w.WriteHeader(http.StatusAccepted)
}

func (c *onDemandController) processWorkflowJobEvent(event *githubv63.WorkflowJobEvent) error {
	if event == nil || event.WorkflowJob == nil {
		return nil
	}

	jobID := event.WorkflowJob.GetID()
	if jobID == 0 {
		return nil
	}

	switch event.GetAction() {
	case "queued", "in_progress":
		organization := c.organizationFromEvent(event)
		poolName, matchResult := c.matchPool(organization, event.WorkflowJob.Labels)
		if matchResult != "matched" {
			c.logger.Info().
				Int64("job_id", jobID).
				Str("organization", organization).
				Str("result", matchResult).
				Strs("labels", event.WorkflowJob.Labels).
				Msg("Ignoring workflow job for on-demand scaling")
			return nil
		}

		c.logger.Info().
			Int64("job_id", jobID).
			Str("pool", poolName).
			Str("organization", organization).
			Str("action", event.GetAction()).
			Msg("Matched workflow job to pool")
		c.upsertJob(jobID, &jobDemandEntry{
			organization: organization,
			poolName:     poolName,
		})
	case "completed":
		c.removeJob(jobID)
	default:
		return nil
	}

	return nil
}

func (c *onDemandController) reconcile(ctx context.Context) {
	if err := c.reconcileOnce(ctx); err != nil {
		metricOnDemandReconciliations.WithLabelValues("error").Inc()
		c.logger.Error().Err(err).Msg("On-demand reconciliation failed")
		return
	}

	metricOnDemandReconciliations.WithLabelValues("success").Inc()
}

func (c *onDemandController) reconcileOnce(ctx context.Context) error {
	reconciledJobs := make(map[int64]*jobDemandEntry)

	for organization, installationID := range c.installationIDs {
		repositories, err := c.listRepositoriesFn(ctx, installationID)
		if err != nil {
			return fmt.Errorf("listing repositories for organization %q: %w", organization, err)
		}

		for _, repository := range repositories {
			owner := repository.GetOwner().GetLogin()
			name := repository.GetName()

			for _, status := range []string{"queued", "in_progress"} {
				runs, err := c.listWorkflowRunsFn(ctx, installationID, owner, name, status)
				if err != nil {
					return fmt.Errorf("listing workflow runs for %s/%s status %q: %w", owner, name, status, err)
				}

				for _, run := range runs {
					jobs, err := c.listWorkflowJobsFn(ctx, installationID, owner, name, run.GetID())
					if err != nil {
						return fmt.Errorf("listing workflow jobs for %s/%s run %d: %w", owner, name, run.GetID(), err)
					}

					for _, job := range jobs {
						if job == nil || job.GetID() == 0 {
							continue
						}
						if job.GetStatus() != "queued" && job.GetStatus() != "in_progress" {
							continue
						}

						poolName, matchResult := c.matchPool(organization, job.Labels)
						if matchResult != "matched" {
							continue
						}

						reconciledJobs[job.GetID()] = &jobDemandEntry{
							organization: organization,
							poolName:     poolName,
						}
					}
				}
			}
		}
	}

	c.replaceJobs(reconciledJobs)
	return nil
}

func (c *onDemandController) upsertJob(jobID int64, entry *jobDemandEntry) {
	c.mu.Lock()
	c.jobs[jobID] = entry
	poolDemand := c.poolDemandLocked()
	c.mu.Unlock()

	c.applyPoolDemand(poolDemand)
}

func (c *onDemandController) removeJob(jobID int64) {
	c.mu.Lock()
	delete(c.jobs, jobID)
	poolDemand := c.poolDemandLocked()
	c.mu.Unlock()

	c.applyPoolDemand(poolDemand)
}

func (c *onDemandController) replaceJobs(jobs map[int64]*jobDemandEntry) {
	c.mu.Lock()
	c.jobs = jobs
	poolDemand := c.poolDemandLocked()
	c.mu.Unlock()

	c.applyPoolDemand(poolDemand)
}

func (c *onDemandController) poolDemandLocked() map[string]int {
	poolDemand := make(map[string]int)
	for _, job := range c.jobs {
		poolDemand[job.poolName]++
	}

	return poolDemand
}

func (c *onDemandController) applyPoolDemand(poolDemand map[string]int) {
	for _, pool := range c.server.snapshotPools() {
		demand := poolDemand[pool.config.Name]
		pool.SetDemandReplicas(demand)
		metricOnDemandJobsActive.WithLabelValues(pool.config.Name, pool.config.Runner.Organization).Set(float64(demand))
	}
}

func (c *onDemandController) organizationFromEvent(event *githubv63.WorkflowJobEvent) string {
	if event.GetOrg().GetLogin() != "" {
		return event.GetOrg().GetLogin()
	}

	return event.GetRepo().GetOwner().GetLogin()
}

func (c *onDemandController) matchPool(organization string, labels []string) (string, string) {
	matches := make(map[string]struct{})
	for _, label := range labels {
		pool, err := c.server.findPool(label)
		if err != nil {
			continue
		}
		if pool.config.Runner.Organization != organization {
			continue
		}

		matches[pool.config.Name] = struct{}{}
	}

	switch len(matches) {
	case 0:
		return "", "unmatched"
	case 1:
		for poolName := range matches {
			return poolName, "matched"
		}
	}

	return "", "ambiguous"
}

func (c *onDemandController) listRepositories(ctx context.Context, installationID int64) ([]*githubv63.Repository, error) {
	client := c.server.github.Installation(installationID)
	options := &githubv63.ListOptions{PerPage: 100}
	repositories := make([]*githubv63.Repository, 0)

	for {
		result, response, err := client.Apps.ListRepos(ctx, options)
		if err != nil {
			return nil, err
		}

		repositories = append(repositories, result.Repositories...)
		if response.NextPage == 0 {
			return repositories, nil
		}

		options.Page = response.NextPage
	}
}

func (c *onDemandController) listWorkflowRuns(ctx context.Context, installationID int64, owner, repo, status string) ([]*githubv63.WorkflowRun, error) {
	client := c.server.github.Installation(installationID)
	options := &githubv63.ListWorkflowRunsOptions{
		Status: status,
		ListOptions: githubv63.ListOptions{
			PerPage: 100,
		},
	}
	runs := make([]*githubv63.WorkflowRun, 0)

	for {
		result, response, err := client.Actions.ListRepositoryWorkflowRuns(ctx, owner, repo, options)
		if err != nil {
			return nil, err
		}

		runs = append(runs, result.WorkflowRuns...)
		if response.NextPage == 0 {
			return runs, nil
		}

		options.Page = response.NextPage
	}
}

func (c *onDemandController) listWorkflowJobs(ctx context.Context, installationID int64, owner, repo string, runID int64) ([]*githubv63.WorkflowJob, error) {
	client := c.server.github.Installation(installationID)
	options := &githubv63.ListWorkflowJobsOptions{
		ListOptions: githubv63.ListOptions{
			PerPage: 100,
		},
	}
	jobs := make([]*githubv63.WorkflowJob, 0)

	for {
		result, response, err := client.Actions.ListWorkflowJobs(ctx, owner, repo, runID, options)
		if err != nil {
			return nil, err
		}

		jobs = append(jobs, result.Jobs...)
		if response.NextPage == 0 {
			return jobs, nil
		}

		options.Page = response.NextPage
	}
}
