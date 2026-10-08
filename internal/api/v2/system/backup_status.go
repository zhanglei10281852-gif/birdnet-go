package system

import (
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/tphakala/birdnet-go/internal/backup"
)

// backupStatusReader is the slice of *backup.Scheduler the backup status
// endpoint depends on. The processor stores the scheduler as any to avoid an
// import cycle, so the handler narrows it to this interface at the boundary.
type backupStatusReader interface {
	IsRunning() bool
	Status() backup.SchedulerStatus
}

// backupStatusResponse is returned by GET /api/v2/system/backup/status. When
// no scheduler is wired (backup disabled or initializing) only Configured is
// present; otherwise Status carries the full scheduler snapshot.
type backupStatusResponse struct {
	Configured bool                    `json:"configured"`
	Status     *backup.SchedulerStatus `json:"status,omitempty"`
}

// GetBackupStatus handles GET /api/v2/system/backup/status. It exposes the
// scheduler lifecycle flag, the active run (if any), recent plan instances
// with their success/failed/timeout/missed/skipped outcomes, missed runs and
// per-target statistics, so the UI/history view can show backup state without
// scraping logs.
func (c *Handler) GetBackupStatus(ctx echo.Context) error {
	return ctx.JSON(http.StatusOK, renderBackupStatus(c.backupSchedulerOrNil()))
}

// backupSchedulerOrNil returns the scheduler registered on the processor, or
// nil when the processor/scheduler is unavailable.
func (c *Handler) backupSchedulerOrNil() any {
	if c.Core == nil || c.Processor == nil {
		return nil
	}
	return c.Processor.GetBackupScheduler()
}

// renderBackupStatus builds the response from an optional scheduler. It is a
// pure function so the configured path is testable without constructing the
// full processor dependency graph.
func renderBackupStatus(scheduler any) backupStatusResponse {
	if scheduler == nil {
		return backupStatusResponse{Configured: false}
	}
	reader, ok := scheduler.(backupStatusReader)
	if !ok {
		return backupStatusResponse{Configured: false}
	}
	status := reader.Status()
	return backupStatusResponse{Configured: true, Status: &status}
}
