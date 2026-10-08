package system

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tphakala/birdnet-go/internal/api/v2/apitest"
	"github.com/tphakala/birdnet-go/internal/backup"
)

// stubBackupScheduler implements the narrow backupStatusReader surface for
// tests without constructing the real manager/processor dependency graph.
type stubBackupScheduler struct {
	running bool
	status  backup.SchedulerStatus
}

func (s *stubBackupScheduler) IsRunning() bool { return s.running }
func (s *stubBackupScheduler) Status() backup.SchedulerStatus {
	return s.status
}

// TestRenderBackupStatus_NotConfigured covers nil and incompatible values,
// which is what the endpoint reports when backup is disabled.
func TestRenderBackupStatus_NotConfigured(t *testing.T) {
	t.Parallel()
	assert.False(t, renderBackupStatus(nil).Configured)
	assert.False(t, renderBackupStatus("unrelated").Configured)
}

// TestRenderBackupStatus_Configured verifies the scheduler snapshot is
// surfaced verbatim, including the distinct plan-instance statuses.
func TestRenderBackupStatus_Configured(t *testing.T) {
	t.Parallel()
	scheduled := time.Date(2026, 10, 8, 2, 0, 0, 0, time.UTC)
	stub := &stubBackupScheduler{
		running: true,
		status: backup.SchedulerStatus{
			Running: true,
			Active: &backup.ActiveBackupRun{
				InstanceID: "daily@20261008T020000Z",
				Trigger:    backup.TriggerSchedule,
				StartedAt:  scheduled,
			},
			RecentInstances: []backup.PlanInstance{
				{ID: "daily@20261008T020000Z", Status: backup.StatusRunning, ScheduledTime: scheduled},
				{ID: "daily@20261007T020000Z", Status: backup.StatusSuccess},
				{ID: "daily@20261006T020000Z", Status: backup.StatusTimeout},
				{ID: "daily@20261005T020000Z", Status: backup.StatusFailed},
				{ID: "daily@20261004T020000Z", Status: backup.StatusMissed},
				{ID: "daily@20261003T020000Z", Status: backup.StatusSkipped},
			},
		},
	}

	resp := renderBackupStatus(stub)
	require.True(t, resp.Configured)
	require.NotNil(t, resp.Status)
	assert.True(t, resp.Status.Running)
	require.NotNil(t, resp.Status.Active)
	assert.Equal(t, "daily@20261008T020000Z", resp.Status.Active.InstanceID)

	byStatus := make(map[backup.RunStatus]backup.PlanInstance, len(resp.Status.RecentInstances))
	for _, inst := range resp.Status.RecentInstances {
		byStatus[inst.Status] = inst
	}
	for _, status := range []backup.RunStatus{
		backup.StatusRunning,
		backup.StatusSuccess,
		backup.StatusTimeout,
		backup.StatusFailed,
		backup.StatusMissed,
		backup.StatusSkipped,
	} {
		_, ok := byStatus[status]
		assert.True(t, ok, "status %s must be distinguishable in the snapshot", status)
	}
}

// TestGetBackupStatus_NoProcessor_HTTP200 verifies the endpoint stays
// available (and reports backup as unconfigured) when the processor is not
// wired, as in lightweight handler unit tests.
func TestGetBackupStatus_NoProcessor_HTTP200(t *testing.T) {
	// NOT parallel: apitest.NewCore publishes settings to the process-global snapshot.
	e := echo.New()
	controller := &Handler{Core: apitest.NewCore(t, apitest.WithEcho(e))}

	req := httptest.NewRequest(http.MethodGet, "/api/v2/system/backup/status", http.NoBody)
	rec := httptest.NewRecorder()
	ctx := e.NewContext(req, rec)

	require.NoError(t, controller.GetBackupStatus(ctx))
	assert.Equal(t, http.StatusOK, rec.Code)

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))
	assert.JSONEq(t, `false`, string(raw["configured"]))
	_, present := raw["status"]
	assert.False(t, present, "status must be omitted when backup is not configured")
}
