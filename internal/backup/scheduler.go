package backup

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/tphakala/birdnet-go/internal/conf"
	"github.com/tphakala/birdnet-go/internal/errors"
	"github.com/tphakala/birdnet-go/internal/logger"
)

// schedulerTickInterval is how often the scheduler checks for due plans.
const schedulerTickInterval = 1 * time.Minute

// missedRunThreshold is how far past its scheduled time a plan may be before
// the scheduler records it as missed instead of executing it.
const missedRunThreshold = 5 * time.Minute

// BackupSchedule represents a scheduled backup task
type BackupSchedule struct {
	Hour     int          // Hour to run backup (0-23)
	Minute   int          // Minute to run backup (0-59)
	Weekday  time.Weekday // Day of week for weekly backups (-1 for daily)
	LastRun  time.Time    // Last successful run time
	NextRun  time.Time    // Next scheduled run time
	IsWeekly bool         // true for weekly backups, false for daily
}

// activeRun is the lease held by one running backup, scheduled or manual. The
// scheduler allows at most one active run at a time and tracks its goroutine
// in runWg so Stop can cancel and wait for it instead of leaking work that
// writes to closed state or targets.
type activeRun struct {
	instanceID string
	trigger    Trigger
	startedAt  time.Time
}

// Scheduler manages backup schedules and their execution
type Scheduler struct {
	manager   *Manager
	schedules []BackupSchedule
	isRunning bool

	mu     sync.RWMutex
	active *activeRun

	// rootCtx is cancelled by Stop; every scheduled, recovery and manual run
	// derives its context from it, so shutdown cancellation reaches all the
	// way into source extraction and target Store calls.
	rootCtx    context.Context
	rootCancel context.CancelFunc

	// runWg tracks the ticker loop, the recovery loop and every run goroutine.
	// Stop waits for the whole group after cancelling rootCtx.
	runWg sync.WaitGroup

	// startedAt is the moment this scheduler instance started. Recovery only
	// resumes instances started before it, so an instance a concurrent ticker
	// tick just created in this process can never be "recovered" by the
	// startup loop.
	startedAt time.Time

	logger logger.Logger
	state  *StateManager
}

// NewScheduler creates a new backup scheduler
func NewScheduler(manager *Manager, _ logger.Logger, stateManager *StateManager) (*Scheduler, error) {
	if stateManager == nil {
		GetLogger().Error("StateManager provided to NewScheduler cannot be nil")
		return nil, errors.Newf("state manager cannot be nil").
			Component("backup").
			Category(errors.CategoryConfiguration).
			Build()
	}
	return &Scheduler{
		manager: manager,
		logger:  GetLogger().Module("scheduler"),
		state:   stateManager,
	}, nil
}

// AddSchedule adds a new backup schedule
func (s *Scheduler) AddSchedule(hour, minute int, weekday time.Weekday, isWeekly bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Validate time parameters
	if hour < 0 || hour > 23 {
		return errors.Newf("invalid hour: %d", hour).
			Component("backup").
			Category(errors.CategoryConfiguration).
			Build()
	}
	if minute < 0 || minute > 59 {
		return errors.Newf("invalid minute: %d", minute).
			Component("backup").
			Category(errors.CategoryConfiguration).
			Build()
	}
	// For daily backups, set weekday to -1
	if !isWeekly {
		weekday = -1
	}
	// Calculate next run time
	now := time.Now()
	nextRun := s.calculateNextRun(now, hour, minute, weekday, isWeekly)
	schedule := BackupSchedule{
		Hour:     hour,
		Minute:   minute,
		Weekday:  weekday,
		IsWeekly: isWeekly,
		NextRun:  nextRun,
	}
	s.schedules = append(s.schedules, schedule)
	return nil
}

// Start begins the scheduler. It is idempotent: a second call while the
// scheduler is running is a no-op. The ticker loop, the restart-recovery
// loop and every run are tracked so Stop can converge all of them.
func (s *Scheduler) Start() {
	s.mu.Lock()
	if s.isRunning {
		s.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.rootCtx = ctx
	s.rootCancel = cancel
	s.isRunning = true
	s.startedAt = time.Now()
	s.runWg.Go(func() { s.run(ctx) })
	s.mu.Unlock()

	s.logger.Info("Backup scheduler started")

	// Resume plans that were in flight when the previous process exited.
	// Tracked by the same wait group and serialized by the same lease as
	// ticker-driven runs.
	s.runWg.Go(func() { s.recoverPendingRuns(ctx) })
}

// Stop cancels the ticker, restart recovery and any in-flight backup, then
// waits for every owned goroutine to finish. It is idempotent and safe to
// call before Start: a stopped scheduler performs no state or target writes
// after Stop returns, which is the ordering application shutdown relies on
// before closing the StateManager.
func (s *Scheduler) Stop() {
	s.mu.Lock()
	if !s.isRunning {
		s.mu.Unlock()
		return
	}
	s.isRunning = false
	cancel := s.rootCancel
	active := s.active
	s.mu.Unlock()

	if active != nil {
		s.logger.Info("Backup scheduler stopping; waiting for in-flight backup to finish or cancel",
			logger.String("instance_id", active.instanceID))
	} else {
		s.logger.Info("Backup scheduler stopping")
	}
	if cancel != nil {
		cancel()
	}
	s.runWg.Wait()
	s.logger.Info("Backup scheduler stopped")
}

// IsRunning returns whether the scheduler is currently running
func (s *Scheduler) IsRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.isRunning
}

// GetMissedRuns returns a list of missed backup times
func (s *Scheduler) GetMissedRuns() []time.Time {
	missed := s.state.GetMissedBackups()
	times := make([]time.Time, len(missed))
	for i, m := range missed {
		times[i] = m.ScheduledTime
	}
	return times
}

// ClearMissedRuns clears the list of missed runs
func (s *Scheduler) ClearMissedRuns() {
	if err := s.state.ClearMissedBackups(); err != nil {
		s.logger.Error("Failed to clear missed backups", logger.Error(err))
	}
}

// run is the main scheduler loop
func (s *Scheduler) run(ctx context.Context) {
	ticker := time.NewTicker(schedulerTickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			s.checkSchedules(now)
		}
	}
}

// dueSchedule is the immutable decision record for one due plan: the
// schedule snapshot at decision time, the time that was actually scheduled
// and the next future run time. Passing copies (never pointers into the
// schedules slice) into run goroutines removes the data race between the
// scheduler mutating NextRun and a run reading the schedule.
type dueSchedule struct {
	schedule      BackupSchedule
	scheduledTime time.Time
	nextRun       time.Time
	late          bool
}

// checkSchedules checks if any backups need to be run. All calendar
// decisions happen under the lock; state persistence and runs happen
// outside it so disk I/O never blocks other scheduler operations.
func (s *Scheduler) checkSchedules(now time.Time) {
	due := s.collectDueSchedules(now)
	for i := range due {
		s.handleDueSchedule(&due[i])
	}
}

// collectDueSchedules advances due schedules under the lock and returns
// their decision records. The in-memory slice always advances even when
// the plan is missed or skipped, matching the historical behavior.
func (s *Scheduler) collectDueSchedules(now time.Time) []dueSchedule {
	s.mu.Lock()
	defer s.mu.Unlock()
	var due []dueSchedule
	for i := range s.schedules {
		schedule := &s.schedules[i]
		if now.Before(schedule.NextRun) {
			continue
		}
		scheduledTime := schedule.NextRun
		d := dueSchedule{
			schedule:      *schedule,
			scheduledTime: scheduledTime,
			late:          now.Sub(scheduledTime) > missedRunThreshold,
		}
		schedule.LastRun = now
		schedule.NextRun = s.calculateNextRun(now, schedule.Hour, schedule.Minute, schedule.Weekday, schedule.IsWeekly)
		d.nextRun = schedule.NextRun
		d.schedule.LastRun = schedule.LastRun
		d.schedule.NextRun = scheduledTime // Handlers must use the scheduled time, not the advanced one.
		s.logger.Debug("Calculated next run time for schedule",
			logger.String("schedule_type", s.getScheduleType(schedule)),
			logger.Time("next_run", schedule.NextRun))
		due = append(due, d)
	}
	return due
}

// handleDueSchedule records the unique plan instance for one scheduled time
// and either marks it missed/skipped or acquires the lease and runs it.
func (s *Scheduler) handleDueSchedule(d *dueSchedule) {
	instance := s.newPlanInstance(&d.schedule, d.scheduledTime, TriggerSchedule)
	_, created, err := s.state.StartInstance(instance)
	if err != nil {
		s.logger.Error("Failed to record scheduled backup instance",
			logger.String("instance_id", instance.ID), logger.Error(err))
		return
	}
	if !created {
		// The same scheduled time was already observed (duplicate ticker
		// event or a racing check): never execute the plan twice.
		s.logger.Debug("Scheduled backup instance already recorded, skipping duplicate tick",
			logger.String("instance_id", instance.ID))
		return
	}

	if d.late {
		reason := fmt.Sprintf("system time %s was more than %s past scheduled time %s",
			time.Now().Format(time.RFC3339),
			missedRunThreshold,
			d.scheduledTime.Format(time.RFC3339))
		s.logger.Warn("Missed backup schedule",
			logger.Time("scheduled_time", d.scheduledTime),
			logger.String("reason", reason))
		if err := s.state.AddMissedBackupForInstance(instance, reason); err != nil {
			s.logger.Warn("Failed to record missed backup state", logger.Time("scheduled_time", d.scheduledTime), logger.Error(err))
		}
		if err := s.state.FinishInstance(instance.ID, StatusMissed, reason); err != nil {
			s.logger.Warn("Failed to finish missed backup instance", logger.String("instance_id", instance.ID), logger.Error(err))
		}
		return
	}

	if !s.beginRun(instance.ID, TriggerSchedule) {
		// The lease can be unavailable because another backup is running
		// (durable "skipped" outcome) or because the scheduler is stopping
		// (leave the plan recoverable for the next process start instead
		// of inventing a terminal outcome).
		if !s.IsRunning() {
			s.logger.Warn("Scheduler stopping as plan became due; leaving it recoverable",
				logger.String("instance_id", instance.ID))
			return
		}
		reason := "backup skipped because another backup was already running"
		s.logger.Warn("Skipping backup - another backup is already running",
			logger.Time("scheduled_time", d.scheduledTime),
			logger.String("schedule_type", s.getScheduleType(&d.schedule)))
		// Overlap skips are durable, unique (keyed by scheduled time) and
		// recoverable through the same missed-run view as real misses.
		if err := s.state.AddMissedBackupForInstance(instance, reason); err != nil {
			s.logger.Warn("Failed to record skipped backup state", logger.Time("scheduled_time", d.scheduledTime), logger.Error(err))
		}
		if err := s.state.FinishInstance(instance.ID, StatusSkipped, reason); err != nil {
			s.logger.Warn("Failed to finish skipped backup instance", logger.String("instance_id", instance.ID), logger.Error(err))
		}
		return
	}

	s.launchRun(instance, &d.schedule, d.nextRun)
}

// beginRun acquires the single-run lease and registers the goroutine with
// the shutdown wait group atomically. Adding to the wait group under the
// same lock Stop takes guarantees Stop cannot finish Wait while a new run
// is still being scheduled.
func (s *Scheduler) beginRun(instanceID string, trigger Trigger) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.isRunning || s.active != nil {
		return false
	}
	s.runWg.Add(1)
	s.active = &activeRun{
		instanceID: instanceID,
		trigger:    trigger,
		startedAt:  time.Now(),
	}
	return true
}

// finishRun releases the lease. It pairs with every successful beginRun.
func (s *Scheduler) finishRun(instanceID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active != nil && s.active.instanceID == instanceID {
		s.active = nil
	}
}

// launchRun starts the goroutine that owns one leased plan run. The lease
// and wait-group entry already exist from beginRun.
func (s *Scheduler) launchRun(instance *PlanInstance, schedule *BackupSchedule, nextRun time.Time) {
	go func() {
		defer s.runWg.Done()
		defer s.finishRun(instance.ID)
		s.executePlanRun(s.rootCtx, instance, schedule, nextRun)
	}()
}

// newPlanInstance builds the deterministic, unique instance record for a
// scheduled time. The ID is derived from the schedule key and the scheduled
// timestamp so the same tick can never produce two instances.
func (s *Scheduler) newPlanInstance(schedule *BackupSchedule, scheduledTime time.Time, trigger Trigger) *PlanInstance {
	return &PlanInstance{
		ID:            PlanInstanceID(schedule, scheduledTime),
		ScheduleKey:   scheduleKey(schedule),
		ScheduledTime: scheduledTime,
		Status:        StatusRunning,
		Trigger:       trigger,
		IsWeekly:      schedule.IsWeekly,
		Weekday:       s.formatWeekday(schedule.Weekday),
		Targets:       make(map[string]TargetRunResult),
	}
}

// PlanInstanceID returns the durable unique ID of a schedule's planned run.
func PlanInstanceID(schedule *BackupSchedule, scheduledTime time.Time) string {
	return scheduleKey(schedule) + "@" + scheduledTime.UTC().Format("20060102T150405Z")
}

// executePlanRun runs one leased plan instance to its terminal state using
// one RunResult for every state update, so schedule state, missed-run
// records, statistics and cleanup can never disagree about what happened.
func (s *Scheduler) executePlanRun(rootCtx context.Context, instance *PlanInstance, schedule *BackupSchedule, nextRun time.Time) {
	scheduleType := s.getScheduleType(schedule)
	s.logger.Info("Running scheduled backup",
		logger.String("schedule_type", scheduleType),
		logger.String("instance_id", instance.ID))
	start := time.Now()

	runCtx, cancel := context.WithTimeout(rootCtx, s.manager.getBackupTimeout())
	defer cancel()

	isWeekly := instance.IsWeekly
	isDaily := !isWeekly
	opts := RunOptions{
		// Pin the timestamp to the scheduled time so an interrupted run
		// resumed after restart produces the exact same backup IDs.
		Time:             instance.ScheduledTime.UTC(),
		IsDaily:          &isDaily,
		IsWeekly:         &isWeekly,
		InstanceID:       instance.ID,
		CompletedTargets: s.completedTargetsFor(instance),
	}

	result, err := s.manager.RunBackupWithOptions(runCtx, opts)
	if ids := result.BackupIDs(); len(ids) > 0 {
		if stateErr := s.state.RecordInstanceBackupIDs(instance.ID, ids...); stateErr != nil {
			s.logger.Warn("Failed to record backup IDs on plan instance",
				logger.String("instance_id", instance.ID), logger.Error(stateErr))
		}
	}
	duration := time.Since(start)

	// Shutdown interrupted the run: it must converge with the application.
	// Leave the instance recoverable (StatusRunning) so the next start only
	// redoes work that did not complete; never touch stats or cleanup while
	// the lifecycle is shutting down.
	if rootCtx.Err() != nil {
		s.logger.Warn("Scheduled backup interrupted by shutdown and left recoverable",
			logger.String("instance_id", instance.ID),
			logger.Int64("duration_ms", duration.Milliseconds()))
		return
	}

	status := StatusSuccess
	reason := ""
	switch {
	case err == nil && result.Succeeded():
		s.logger.Info("Scheduled backup completed successfully",
			logger.String("schedule_type", scheduleType),
			logger.String("instance_id", instance.ID),
			logger.Int64("duration_ms", duration.Milliseconds()))
	case errors.Is(runCtx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded):
		status = StatusTimeout
		reason = fmt.Sprintf("backup timed out after %s", s.manager.getBackupTimeout())
		s.logger.Error("Scheduled backup timed out",
			logger.String("schedule_type", scheduleType),
			logger.String("instance_id", instance.ID),
			logger.Int64("duration_ms", duration.Milliseconds()),
			logger.Error(err))
	default:
		status = StatusFailed
		if err != nil {
			reason = err.Error()
		} else {
			reason = result.FailureSummary()
		}
		s.logger.Error("Scheduled backup failed",
			logger.String("schedule_type", scheduleType),
			logger.String("instance_id", instance.ID),
			logger.Int64("duration_ms", duration.Milliseconds()),
			logger.String("reason", reason))
	}

	// Schedule state comes from the same instance/result. Use the advanced
	// next-run time decided when the tick fired.
	stateSchedule := *schedule
	stateSchedule.NextRun = nextRun
	if stateErr := s.state.UpdateScheduleState(&stateSchedule, status == StatusSuccess); stateErr != nil {
		s.logger.Warn("Failed to update schedule state after run",
			logger.String("schedule_type", scheduleType), logger.Error(stateErr))
	}

	// Failures and timeouts are visible in the missed-run view too, derived
	// from the same instance (historical behavior plus unique dedup).
	if status == StatusFailed || status == StatusTimeout {
		if stateErr := s.state.AddMissedBackupForInstance(instance, reason); stateErr != nil {
			s.logger.Warn("Failed to record failed backup as missed run",
				logger.String("instance_id", instance.ID), logger.Error(stateErr))
		}
	}

	if status == StatusSuccess {
		// Post-success validation, statistics and cleanup all derive from
		// this same successful run and run under lifecycle-derived contexts,
		// so a shutdown can never drive them against closed targets or
		// state. They finish before the instance becomes terminal, so the
		// terminal status really means the whole plan converged.
		s.runPostSuccessTasks(rootCtx, scheduleType)
	}

	// The single terminal conclusion for this plan instance, published only
	// after every schedule/missed/stats/cleanup write above landed.
	if stateErr := s.state.FinishInstance(instance.ID, status, reason); stateErr != nil {
		s.logger.Warn("Failed to finish plan instance",
			logger.String("instance_id", instance.ID), logger.Error(stateErr))
	}
}

// completedTargetsFor returns the durable per-target outcomes recorded by
// earlier attempts of an instance. Only targets that successfully stored an
// archive are eligible to be skipped; failed targets must be retried.
func (s *Scheduler) completedTargetsFor(instance *PlanInstance) map[string]TargetRunResult {
	if len(instance.Targets) == 0 {
		return nil
	}
	completed := make(map[string]TargetRunResult, len(instance.Targets))
	for name, outcome := range instance.Targets {
		if outcome.Status == targetOutcomeSuccess && len(outcome.BackupIDs) > 0 {
			completed[name] = outcome
		}
	}
	if len(completed) == 0 {
		return nil
	}
	return completed
}

// runPostSuccessTasks performs validation, stats refresh and retention
// cleanup after a successful scheduled run.
func (s *Scheduler) runPostSuccessTasks(rootCtx context.Context, scheduleType string) {
	validationCtx, validationCancel := context.WithTimeout(rootCtx, s.manager.getCleanupTimeout())
	defer validationCancel()
	if err := s.manager.ValidateBackupCounts(validationCtx); err != nil {
		s.logger.Warn("Backup retention policy validation failed after backup",
			logger.String("schedule_type", scheduleType), logger.Error(err))
	}

	statsCtx, statsCancel := context.WithTimeout(rootCtx, s.manager.getOperationTimeout())
	defer statsCancel()
	stats, err := s.manager.GetBackupStats(statsCtx)
	if err != nil {
		s.logger.Warn("Failed to get backup statistics after backup", logger.String("schedule_type", scheduleType), logger.Error(err))
		return // Don't proceed if stats failed
	}
	if err := s.state.UpdateStats(stats); err != nil {
		s.logger.Warn("Failed to update backup statistics in state", logger.String("schedule_type", scheduleType), logger.Error(err))
	}
	for targetName := range stats {
		targetStats := stats[targetName]
		s.logger.Info("Backup stats",
			logger.String("schedule_type", scheduleType),
			logger.String("target_name", targetName),
			logger.Int("total_backups", targetStats.TotalBackups),
			logger.Int("daily_backups", targetStats.DailyBackups),
			logger.Int("weekly_backups", targetStats.WeeklyBackups),
			logger.Int64("total_size_bytes", targetStats.TotalSize),
			logger.String("oldest_backup_ts", targetStats.OldestBackup.Format(time.RFC3339)),
			logger.String("newest_backup_ts", targetStats.NewestBackup.Format(time.RFC3339)),
		)
	}

	cleanupCtx, cleanupCancel := context.WithTimeout(rootCtx, s.manager.getCleanupTimeout())
	defer cleanupCancel()
	if err := s.manager.performBackupCleanup(cleanupCtx); err != nil {
		s.logger.Error("Failed to perform post-backup cleanup", logger.String("schedule_type", scheduleType), logger.Error(err))
	}
}

// recoverPendingRuns resumes instances that were still running when the
// previous process exited. Completed targets of each instance are skipped,
// so restart never re-submits a target write that already finished; terminal
// instances are never resumed.
func (s *Scheduler) recoverPendingRuns(ctx context.Context) {
	allPending := s.state.RecoverableInstances()
	// Only resume plans started before this scheduler instance came up:
	// those are the ones interrupted by a process restart. Instances a
	// ticker tick creates concurrently below have a later StartedAt and
	// are owned by the normal lease path, not by recovery.
	startedAt := s.startedAt
	pending := allPending[:0]
	for i := range allPending {
		if allPending[i].StartedAt.Before(startedAt) {
			pending = append(pending, allPending[i])
		}
	}
	if len(pending) == 0 {
		return
	}
	if !s.manager.Enabled() {
		s.logger.Warn("Backup is disabled, canceling interrupted backup plans", logger.Int("count", len(pending)))
		for i := range pending {
			if err := s.state.FinishInstance(pending[i].ID, StatusCanceled, "backup disabled at startup"); err != nil {
				s.logger.Warn("Failed to cancel interrupted plan instance",
					logger.String("instance_id", pending[i].ID), logger.Error(err))
			}
		}
		return
	}
	s.logger.Info("Recovering interrupted backup plans after restart", logger.Int("count", len(pending)))
	for i := range pending {
		select {
		case <-ctx.Done():
			return
		default:
		}
		instance := pending[i]
		if !s.beginRun(instance.ID, TriggerRecovery) {
			// A ticker-driven run (or another recovery) owns the lease; that
			// run is already doing the work, so recovery stops here.
			s.logger.Debug("Skipping recovery because a backup is already running",
				logger.String("instance_id", instance.ID))
			return
		}
		if err := s.state.SetInstanceTrigger(instance.ID, TriggerRecovery); err != nil {
			s.logger.Warn("Failed to mark plan instance as recovery-triggered",
				logger.String("instance_id", instance.ID), logger.Error(err))
		}
		schedule, nextRun := s.scheduleForInstance(&instance)
		s.launchRun(&instance, schedule, nextRun)
	}
}

// scheduleForInstance resolves the in-memory schedule matching an instance's
// schedule key. If configuration no longer contains the schedule (for
// example it was removed while the process was down), a minimal schedule
// reconstructed from the instance is returned with a zero next-run time so
// recovery still completes the interrupted plan without inventing a future
// tick.
func (s *Scheduler) scheduleForInstance(instance *PlanInstance) (*BackupSchedule, time.Time) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for i := range s.schedules {
		if scheduleKey(&s.schedules[i]) == instance.ScheduleKey {
			found := s.schedules[i]
			return &found, found.NextRun
		}
	}
	weekday := time.Weekday(-1)
	if instance.IsWeekly {
		if d, err := parseWeekday(instance.Weekday); err == nil {
			weekday = d
		}
	}
	return &BackupSchedule{
		IsWeekly: instance.IsWeekly,
		Weekday:  weekday,
		NextRun:  time.Time{},
	}, time.Time{}
}

// calculateNextRun determines the next run time for a schedule
func (s *Scheduler) calculateNextRun(now time.Time, hour, minute int, weekday time.Weekday, isWeekly bool) time.Time {
	var next time.Time
	if isWeekly {
		// For weekly backups
		next = time.Date(
			now.Year(), now.Month(), now.Day(),
			hour, minute, 0, 0, now.Location(),
		)
		// Adjust to next occurrence of weekday
		daysUntilWeekday := int(weekday - next.Weekday())
		if daysUntilWeekday < 0 {
			// We're past the target weekday, so add days to get to next week
			daysUntilWeekday += 7
		} else if daysUntilWeekday == 0 {
			// We're on the target weekday
			if now.After(next) {
				// The scheduled time has passed today, so schedule for next week
				daysUntilWeekday = 7
			}
			// Otherwise, daysUntilWeekday remains 0 (today)
		}
		next = next.AddDate(0, 0, daysUntilWeekday)
	} else {
		// For daily backups
		next = time.Date(
			now.Year(), now.Month(), now.Day(),
			hour, minute, 0, 0, now.Location(),
		)
		// If we're past the time today, schedule for tomorrow
		if now.After(next) {
			next = next.AddDate(0, 0, 1)
		}
	}
	return next
}

// UpdateSchedules updates all schedules based on new configuration
func (s *Scheduler) UpdateSchedules(schedules []BackupSchedule) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.schedules = make([]BackupSchedule, len(schedules))
	copy(s.schedules, schedules)
	// Recalculate next run times for all schedules
	now := time.Now()
	for i := range s.schedules {
		schedule := &s.schedules[i]
		schedule.NextRun = s.calculateNextRun(
			now,
			schedule.Hour,
			schedule.Minute,
			schedule.Weekday,
			schedule.IsWeekly,
		)
	}
}

// parseWeekday converts a weekday string to time.Weekday
func parseWeekday(weekday string) (time.Weekday, error) {
	switch strings.ToLower(weekday) {
	case "sunday":
		return time.Sunday, nil
	case "monday":
		return time.Monday, nil
	case "tuesday":
		return time.Tuesday, nil
	case "wednesday":
		return time.Wednesday, nil
	case "thursday":
		return time.Thursday, nil
	case "friday":
		return time.Friday, nil
	case "saturday":
		return time.Saturday, nil
	default:
		return -1, errors.Newf("invalid weekday: %s", weekday).
			Component("backup").
			Category(errors.CategoryConfiguration).
			Build()
	}
}

// LoadFromConfig loads schedules from the backup configuration
func (s *Scheduler) LoadFromConfig(config *conf.BackupConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.schedules = nil // Clear existing schedules before loading
	s.logger.Info("Loading backup schedules from configuration", logger.Int("schedule_count", len(config.Schedules)))
	for _, scheduleConf := range config.Schedules {
		if !scheduleConf.Enabled {
			s.logger.Debug("Skipping disabled schedule", logger.Int("hour", scheduleConf.Hour), logger.Int("minute", scheduleConf.Minute), logger.String("weekday", scheduleConf.Weekday))
			continue
		}
		// Validate time
		if scheduleConf.Hour < 0 || scheduleConf.Hour > 23 {
			errMsg := fmt.Sprintf("invalid hour %d in schedule config", scheduleConf.Hour)
			s.logger.Error(errMsg, logger.Any("config", scheduleConf))
			return errors.Newf("%s", errMsg).
				Component("backup").
				Category(errors.CategoryConfiguration).
				Context("operation", "parse_schedule_config").
				Context("hour", scheduleConf.Hour).
				Build()
		}
		if scheduleConf.Minute < 0 || scheduleConf.Minute > 59 {
			errMsg := fmt.Sprintf("invalid minute %d in schedule config", scheduleConf.Minute)
			s.logger.Error(errMsg, logger.Any("config", scheduleConf))
			return errors.Newf("%s", errMsg).
				Component("backup").
				Category(errors.CategoryConfiguration).
				Context("operation", "parse_schedule_config").
				Context("minute", scheduleConf.Minute).
				Build()
		}
		var weekday time.Weekday
		var isWeekly bool
		// Determine weekday and weekly status based on configuration
		switch {
		case scheduleConf.IsWeekly:
			// Explicitly marked as weekly
			isWeekly = true
			if scheduleConf.Weekday == "" { // No weekday specified but marked as weekly
				s.logger.Warn("Schedule marked as weekly but no weekday specified, defaulting to Sunday", logger.Any("config", scheduleConf))
				weekday = time.Sunday
			} else {
				// Parse the specified weekday
				parsedDay, err := parseWeekday(scheduleConf.Weekday)
				if err != nil {
					errMsg := fmt.Sprintf("invalid weekday '%s' in schedule config", scheduleConf.Weekday)
					s.logger.Error(errMsg, logger.Any("config", scheduleConf), logger.Error(err))
					return errors.New(err).
						Component("backup").
						Category(errors.CategoryConfiguration).
						Context("operation", "parse_weekday_for_weekly_schedule").
						Context("weekday", scheduleConf.Weekday).
						Build()
				}
				weekday = parsedDay
			}
		case scheduleConf.Weekday != "":
			// If weekday is specified but not explicitly marked as weekly, still make it weekly
			isWeekly = true
			parsedDay, err := parseWeekday(scheduleConf.Weekday)
			if err != nil {
				errMsg := fmt.Sprintf("invalid weekday '%s' in schedule config", scheduleConf.Weekday)
				s.logger.Error(errMsg, logger.Any("config", scheduleConf), logger.Error(err))
				return errors.New(err).
					Component("backup").
					Category(errors.CategoryConfiguration).
					Context("operation", "parse_weekday_for_schedule").
					Context("weekday", scheduleConf.Weekday).
					Build()
			}
			weekday = parsedDay
		default:
			// Neither IsWeekly nor Weekday specified, it's a daily schedule
			isWeekly = false
			weekday = -1 // Explicitly set weekday to -1 for daily schedules
		}

		// Calculate next run time based on current time
		now := time.Now()
		nextRun := s.calculateNextRun(now, scheduleConf.Hour, scheduleConf.Minute, weekday, isWeekly)

		schedule := BackupSchedule{
			Hour:     scheduleConf.Hour,
			Minute:   scheduleConf.Minute,
			Weekday:  weekday,
			IsWeekly: isWeekly,
			NextRun:  nextRun,
		}

		s.schedules = append(s.schedules, schedule)
		s.logger.Info("Loaded schedule",
			logger.Int("hour", schedule.Hour),
			logger.Int("minute", schedule.Minute),
			logger.String("weekday", s.formatWeekday(schedule.Weekday)),
			logger.Bool("is_weekly", schedule.IsWeekly),
			logger.String("next_run", schedule.NextRun.Format(time.RFC3339)),
		)
	}

	if len(s.schedules) == 0 {
		s.logger.Warn("No enabled backup schedules loaded from configuration.")
	}

	return nil
}

// GetMissedBackups returns all missed backups
func (s *Scheduler) GetMissedBackups() []MissedBackup {
	return s.state.GetMissedBackups()
}

// ClearMissedBackups clears the list of missed backups
func (s *Scheduler) ClearMissedBackups() error {
	return s.state.ClearMissedBackups()
}

// GetBackupStats returns current backup statistics
func (s *Scheduler) GetBackupStats() map[string]BackupStats {
	return s.state.GetStats()
}

// ActiveBackupRun describes the backup currently holding the run lease.
type ActiveBackupRun struct {
	InstanceID string    `json:"instance_id,omitempty"`
	Trigger    Trigger   `json:"trigger"`
	StartedAt  time.Time `json:"started_at"`
}

// SchedulerStatus is the observable state of the scheduler: lifecycle flag,
// the active run if any, the loaded schedules, recent plan instances, missed
// runs and target statistics. The API/history view maps directly onto it.
type SchedulerStatus struct {
	Running         bool                   `json:"running"`
	Active          *ActiveBackupRun       `json:"active,omitempty"`
	Schedules       []BackupSchedule       `json:"schedules"`
	RecentInstances []PlanInstance         `json:"recent_instances"`
	MissedRuns      []MissedBackup         `json:"missed_runs"`
	Stats           map[string]BackupStats `json:"stats"`
}

// Status returns a point-in-time snapshot safe to serialize for the API and
// UI. It distinguishes running / success / failed / timeout / missed /
// skipped through the plan instance records.
func (s *Scheduler) Status() SchedulerStatus {
	s.mu.RLock()
	running := s.isRunning
	schedules := make([]BackupSchedule, len(s.schedules))
	copy(schedules, s.schedules)
	var active *ActiveBackupRun
	if s.active != nil {
		active = &ActiveBackupRun{
			InstanceID: s.active.instanceID,
			Trigger:    s.active.trigger,
			StartedAt:  s.active.startedAt,
		}
	}
	s.mu.RUnlock()

	return SchedulerStatus{
		Running:         running,
		Active:          active,
		Schedules:       schedules,
		RecentInstances: s.state.GetInstances(),
		MissedRuns:      s.state.GetMissedBackups(),
		Stats:           s.state.GetStats(),
	}
}

// TriggerBackup initiates an immediate backup operation. When the scheduler
// is running the manual run shares the single lease and lifecycle context,
// so application shutdown cancels and waits for it just like a scheduled
// run; manual runs never create plan instances or touch schedule state.
// When the scheduler is stopped the run executes standalone with its own
// timeout, preserving the historical direct-trigger behavior.
func (s *Scheduler) TriggerBackup(ctx context.Context) error {
	if s.IsRunning() {
		if !s.beginRun("", Trigger("manual")) {
			s.logger.Warn("Cannot trigger manual backup - another backup is already running")
			return errors.Newf("another backup is already in progress").
				Component("backup").
				Category(errors.CategorySystem).
				Build()
		}
		defer s.finishRun("")
		defer s.runWg.Done()

		// Combine the application lifecycle root with the caller context:
		// shutdown always cancels; a canceled request cancels too.
		runCtx, cancel := context.WithCancel(s.rootCtx)
		defer cancel()
		if ctx != nil {
			stopAfterFunc := context.AfterFunc(ctx, cancel)
			defer stopAfterFunc()
		}
		timeoutCtx, timeoutCancel := context.WithTimeout(runCtx, s.manager.getBackupTimeout())
		defer timeoutCancel()

		s.logger.Info("Manually triggering backup run...")
		if err := s.manager.RunBackup(timeoutCtx); err != nil {
			s.logger.Error("Manual backup trigger failed", logger.Error(err))
			return err
		}
		s.logger.Info("Manual backup trigger completed successfully")
		return nil
	}

	// Standalone mode (scheduler never started): keep the direct behavior.
	s.logger.Info("Manually triggering standalone backup run...")
	timeoutCtx, cancel := context.WithTimeout(context.Background(), s.manager.getBackupTimeout())
	defer cancel()
	if err := s.manager.RunBackup(timeoutCtx); err != nil {
		s.logger.Error("Manual backup trigger failed", logger.Error(err))
		return err
	}
	s.logger.Info("Manual backup trigger completed successfully")
	return nil
}

// getScheduleType returns a string representation of the schedule type
func (s *Scheduler) getScheduleType(schedule *BackupSchedule) string {
	if schedule.IsWeekly {
		return fmt.Sprintf("Weekly (%s)", s.formatWeekday(schedule.Weekday))
	}
	return "Daily"
}

// formatWeekday returns a string representation of the weekday
func (s *Scheduler) formatWeekday(wd time.Weekday) string {
	// Check if weekday is valid (Sunday=0 to Saturday=6)
	if wd < time.Sunday || wd > time.Saturday {
		// Handle -1 case specifically for "Daily"
		if wd == -1 {
			return "Daily"
		}
		return "Invalid"
	}
	return wd.String() // Call String() on the value wd
}
