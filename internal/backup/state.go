package backup

import (
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/tphakala/birdnet-go/internal/conf"
	"github.com/tphakala/birdnet-go/internal/errors"
	"github.com/tphakala/birdnet-go/internal/logger"
)

// RunStatus is the lifecycle status of a single scheduled backup plan
// instance. Every scheduled time produces exactly one instance and every
// instance reaches exactly one terminal status. Instances that were in
// progress when the process stopped stay in StatusRunning and are recovered
// on the next start.
type RunStatus string

const (
	// StatusRunning means the instance has started but has no terminal
	// result yet. This includes instances interrupted by a graceful
	// shutdown: they are the only status recovered on restart.
	StatusRunning RunStatus = "running"
	// StatusSuccess means every source was backed up and every target
	// stored the archive successfully.
	StatusSuccess RunStatus = "success"
	// StatusFailed means the run finished with at least one source or
	// target failure.
	StatusFailed RunStatus = "failed"
	// StatusTimeout means the run context deadline expired before the
	// run completed.
	StatusTimeout RunStatus = "timeout"
	// StatusMissed means the scheduled time was already more than the
	// late-run threshold old when the scheduler observed it, so the
	// instance was never executed.
	StatusMissed RunStatus = "missed"
	// StatusSkipped means the instance could not start because another
	// backup already held the run lease.
	StatusSkipped RunStatus = "skipped"
	// StatusCanceled is a terminal status for an in-flight instance that
	// cannot be recovered because backing it up is no longer possible
	// (for example backup was disabled in configuration).
	StatusCanceled RunStatus = "canceled"
)

// IsTerminal reports whether the status is a final outcome.
func (s RunStatus) IsTerminal() bool {
	switch s {
	case StatusSuccess, StatusFailed, StatusTimeout, StatusMissed, StatusSkipped, StatusCanceled:
		return true
	default:
		return false
	}
}

// Trigger identifies what started a plan instance.
type Trigger string

const (
	// TriggerSchedule is a ticker-driven scheduled run.
	TriggerSchedule Trigger = "schedule"
	// TriggerRecovery is a run resumed after a process restart.
	TriggerRecovery Trigger = "recovery"
)

// Target outcome status values recorded per plan instance.
const (
	targetOutcomeSuccess = "success"
	targetOutcomeFailed  = "failed"
	targetOutcomeSkipped = "skipped"
)

// retention bounds for the durable state file. Only terminal instances beyond
// the most recent ones are pruned; running instances are always retained so
// they can be recovered.
const (
	maxStoredTerminalInstances = 100
	maxStoredMissedRuns        = 200
)

// ErrStateClosed is returned by StateManager mutators after Close has been
// called. It lets shutdown-path callers distinguish "state is intentionally
// gone" from a real persistence failure.
var ErrStateClosed = errors.NewStd("backup state manager is closed")

// BackupState represents the persistent state of the backup system
type BackupState struct {
	LastUpdate time.Time                `json:"last_update"`
	Schedules  map[string]ScheduleState `json:"schedules"` // Key is "daily" or "weekly-{weekday}"
	Targets    map[string]TargetState   `json:"targets"`   // Key is target name
	MissedRuns []MissedBackup           `json:"missed_runs"`
	Stats      map[string]BackupStats   `json:"stats"` // Key is target name

	// Instances holds one entry per scheduled backup plan execution keyed
	// by PlanInstance.ID. It is the durable source of truth for what a
	// scheduled time did and what still needs to be recovered.
	Instances map[string]PlanInstance `json:"instances"`
}

// ScheduleState represents the state of a backup schedule
type ScheduleState struct {
	LastSuccessful time.Time `json:"last_successful"`
	LastAttempted  time.Time `json:"last_attempted"`
	NextScheduled  time.Time `json:"next_scheduled"`
	FailureCount   int       `json:"failure_count"`
}

// TargetState represents the state of a backup target
type TargetState struct {
	LastBackupID     string    `json:"last_backup_id"`
	LastBackupTime   time.Time `json:"last_backup_time"`
	LastBackupStatus string    `json:"last_backup_status"`
	LastBackupSize   int64     `json:"last_backup_size"`
	TotalBackups     int       `json:"total_backups"`
	TotalSize        int64     `json:"total_size"`
	LastValidation   time.Time `json:"last_validation"`
	ValidationStatus string    `json:"validation_status"`
}

// MissedBackup represents a missed backup event
type MissedBackup struct {
	ScheduledTime time.Time `json:"scheduled_time"`
	Reason        string    `json:"reason"`
	IsWeekly      bool      `json:"is_weekly"`
	Weekday       string    `json:"weekday,omitempty"`
}

// TargetRunResult is the outcome for one target within a plan instance.
// BackupIDs lists the source archives the target has stored (or already held
// when a recovered run skipped it); it drives restart idempotency.
type TargetRunResult struct {
	Name      string   `json:"name"`
	Status    string   `json:"status"` // targetOutcomeSuccess | targetOutcomeFailed | targetOutcomeSkipped
	BackupIDs []string `json:"backup_ids"`
	Size      int64    `json:"size"`
	Error     string   `json:"error,omitempty"`
}

// PlanInstance is the unique, recoverable record of one scheduled backup
// execution. Its ID is deterministic from the schedule key and scheduled
// time, so recording or finishing the same instance twice is idempotent even
// across process restarts.
type PlanInstance struct {
	ID            string                     `json:"id"`
	ScheduleKey   string                     `json:"schedule_key"`
	ScheduledTime time.Time                  `json:"scheduled_time"`
	StartedAt     time.Time                  `json:"started_at"`
	FinishedAt    time.Time                  `json:"finished_at"`
	Status        RunStatus                  `json:"status"`
	Trigger       Trigger                    `json:"trigger"`
	IsWeekly      bool                       `json:"is_weekly"`
	Weekday       string                     `json:"weekday,omitempty"`
	BackupIDs     []string                   `json:"backup_ids,omitempty"`
	Targets       map[string]TargetRunResult `json:"targets,omitempty"`
	Error         string                     `json:"error,omitempty"`
}

// StateManager handles persistence of backup states
type StateManager struct {
	state     *BackupState
	statePath string
	mu        sync.RWMutex
	closed    bool
	// writeMu serializes the temp-file write and atomic rename. Several
	// targets report outcomes concurrently, so without this serialization
	// two writers would race on the same .tmp path (and on Windows hold
	// conflicting file handles).
	writeMu sync.Mutex
	logger  logger.Logger
}

// NewStateManager creates a new state manager
func NewStateManager(_ logger.Logger) (*StateManager, error) {
	// Get config directory
	configPaths, err := conf.GetDefaultConfigPaths()
	if err != nil {
		return nil, errors.New(err).
			Component("backup").
			Category(errors.CategoryConfiguration).
			Context("operation", "get_config_paths").
			Build()
	}
	if len(configPaths) == 0 {
		return nil, errors.Newf("no config paths available").
			Component("backup").
			Category(errors.CategoryConfiguration).
			Build()
	}

	// Create state file path
	statePath := filepath.Join(configPaths[0], "backup-state.json")
	return newStateManagerAtPath(statePath)
}

// newStateManagerAtPath creates a state manager backed by the given file. It
// is shared by the production constructor (after path resolution) and tests.
func newStateManagerAtPath(statePath string) (*StateManager, error) {
	sm := &StateManager{
		statePath: statePath,
		state:     newBackupState(),
		logger:    GetLogger().Module("statemanager"),
	}

	// Load existing state if available
	if err := sm.loadState(); err != nil {
		// If file doesn't exist, that's fine - we'll create it on first save
		if !os.IsNotExist(err) {
			sm.logger.Error("Failed to load existing backup state file", logger.String("path", statePath), logger.Error(err))
		} else {
			sm.logger.Info("No existing backup state file found, creating new one", logger.String("path", statePath))
		}
	} else {
		sm.logger.Info("Successfully loaded existing backup state", logger.String("path", statePath))
	}
	sm.ensureMaps()
	return sm, nil
}

// newBackupState returns an initialized empty BackupState.
func newBackupState() *BackupState {
	return &BackupState{
		Schedules:  make(map[string]ScheduleState),
		Targets:    make(map[string]TargetState),
		Stats:      make(map[string]BackupStats),
		Instances:  make(map[string]PlanInstance),
		MissedRuns: make([]MissedBackup, 0),
	}
}

// ensureMaps initializes maps/slices that may be missing when loading a state
// file written by an older version. Callers must hold sm.mu.
func (sm *StateManager) ensureMaps() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.state.Schedules == nil {
		sm.state.Schedules = make(map[string]ScheduleState)
	}
	if sm.state.Targets == nil {
		sm.state.Targets = make(map[string]TargetState)
	}
	if sm.state.Stats == nil {
		sm.state.Stats = make(map[string]BackupStats)
	}
	if sm.state.Instances == nil {
		sm.state.Instances = make(map[string]PlanInstance)
	}
	if sm.state.MissedRuns == nil {
		sm.state.MissedRuns = make([]MissedBackup, 0)
	}
}

// loadState loads the backup state from disk
func (sm *StateManager) loadState() error {
	data, err := os.ReadFile(sm.statePath)
	if err != nil {
		return err
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return json.Unmarshal(data, sm.state)
}

// snapshotLocked returns a deep copy of the current state safe to marshal
// without holding the lock. Callers must hold sm.mu.
func (sm *StateManager) snapshotLocked() BackupState {
	snapshot := *sm.state
	snapshot.Schedules = maps.Clone(sm.state.Schedules)
	snapshot.Targets = maps.Clone(sm.state.Targets)
	snapshot.Stats = maps.Clone(sm.state.Stats)
	snapshot.Instances = make(map[string]PlanInstance, len(sm.state.Instances))
	for id := range sm.state.Instances {
		inst := sm.state.Instances[id]
		inst.BackupIDs = slices.Clone(inst.BackupIDs)
		if inst.Targets != nil {
			inst.Targets = maps.Clone(inst.Targets)
			for name := range inst.Targets {
				outcome := inst.Targets[name]
				outcome.BackupIDs = slices.Clone(outcome.BackupIDs)
				inst.Targets[name] = outcome
			}
		}
		snapshot.Instances[id] = inst
	}
	snapshot.MissedRuns = slices.Clone(sm.state.MissedRuns)
	return snapshot
}

// writeState persists a state snapshot atomically. It performs no locking and
// is safe to call after releasing sm.mu.
func (sm *StateManager) writeState(state *BackupState) error {
	sm.writeMu.Lock()
	defer sm.writeMu.Unlock()
	start := time.Now()
	state.LastUpdate = time.Now()

	// Create state directory if it doesn't exist (internal config path)
	dirPath := filepath.Dir(sm.statePath)
	if err := os.MkdirAll(dirPath, DefaultDirectoryPermissions()); err != nil {
		sm.logger.Error("Failed to create state directory", logger.String("path", dirPath), logger.Error(err))
		return errors.New(err).
			Component("backup").
			Category(errors.CategoryFileIO).
			Context("operation", "create_state_directory").
			Build()
	}

	// Marshal state to JSON with pretty printing
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		sm.logger.Error("Failed to marshal backup state to JSON", logger.Error(err))
		return errors.New(err).
			Component("backup").
			Category(errors.CategoryFileIO).
			Context("operation", "marshal_state").
			Build()
	}

	// Write to temporary file first
	tempFile := sm.statePath + ".tmp"
	if err := os.WriteFile(tempFile, data, PermSecureFile); err != nil {
		sm.logger.Error("Failed to write temporary state file", logger.String("path", tempFile), logger.Error(err))
		return errors.New(err).
			Component("backup").
			Category(errors.CategoryFileIO).
			Context("operation", "write_temp_state_file").
			Build()
	}

	// Rename temporary file to actual state file (atomic operation)
	if err := os.Rename(tempFile, sm.statePath); err != nil {
		if removeErr := os.Remove(tempFile); removeErr != nil {
			sm.logger.Warn("Failed to clean up temp file after rename failure", logger.String("temp_path", tempFile), logger.Error(removeErr))
		}
		sm.logger.Error("Failed to save state file (rename failed)", logger.String("temp_path", tempFile), logger.String("final_path", sm.statePath), logger.Error(err))
		return errors.New(err).
			Component("backup").
			Category(errors.CategoryFileIO).
			Context("operation", "save_state_file").
			Build()
	}
	sm.logger.Debug("Backup state saved successfully", logger.String("path", sm.statePath), logger.Int64("duration_ms", time.Since(start).Milliseconds()))
	return nil
}

// mutate runs fn under the write lock and persists the resulting state in one
// step. No state mutation may escape it without persistence, and persistence
// happens from a deep copy taken under the lock so disk I/O never blocks
// readers. It returns ErrStateClosed once the manager has been closed.
func (sm *StateManager) mutate(operation string, fn func() error) error {
	sm.mu.Lock()
	if sm.closed {
		sm.mu.Unlock()
		return ErrStateClosed
	}
	if fn != nil {
		if err := fn(); err != nil {
			sm.mu.Unlock()
			return err
		}
	}
	snapshot := sm.snapshotLocked()
	sm.mu.Unlock()

	if err := sm.writeState(&snapshot); err != nil {
		sm.logger.Error("Failed to save state after mutation", logger.String("operation", operation), logger.Error(err))
		return err
	}
	return nil
}

// Close marks the state manager closed. After Close returns, mutators return
// ErrStateClosed and nothing writes to the state file. Close is idempotent.
// Callers must stop every backup worker (Scheduler.Stop waits for in-flight
// runs) before closing so no writer can observe a half-shutdown application.
func (sm *StateManager) Close() error {
	sm.mu.Lock()
	wasClosed := sm.closed
	sm.closed = true
	sm.mu.Unlock()
	if !wasClosed {
		sm.logger.Info("Backup state manager closed")
	}
	return nil
}

// IsClosed reports whether Close has been called.
func (sm *StateManager) IsClosed() bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.closed
}

// scheduleKey returns the persistent state key for a schedule.
func scheduleKey(schedule *BackupSchedule) string {
	if schedule.IsWeekly {
		return "weekly-" + schedule.Weekday.String()
	}
	return "daily"
}

// UpdateScheduleState updates the state for a specific schedule
func (sm *StateManager) UpdateScheduleState(schedule *BackupSchedule, successful bool) error {
	key := scheduleKey(schedule)
	sm.logger.Debug("Updating schedule state", logger.String("schedule_key", key), logger.Bool("successful", successful))
	return sm.mutate("update_schedule_state", func() error {
		state := sm.state.Schedules[key]
		state.LastAttempted = time.Now()
		if successful {
			state.LastSuccessful = time.Now()
			state.FailureCount = 0
		} else {
			state.FailureCount++
		}
		state.NextScheduled = schedule.NextRun
		sm.state.Schedules[key] = state
		return nil
	})
}

// AddMissedBackup records a missed backup using the scheduled time the
// instance was planned for. Callers must pass the plan instance created for
// that exact tick so the missed-run record and instance stay consistent.
func (sm *StateManager) AddMissedBackup(schedule *BackupSchedule, reason string) error {
	missed := MissedBackup{
		ScheduledTime: schedule.NextRun,
		Reason:        reason,
		IsWeekly:      schedule.IsWeekly,
	}
	if schedule.IsWeekly {
		missed.Weekday = schedule.Weekday.String()
	}
	return sm.appendMissed(missed)
}

// AddMissedBackupForInstance records a missed/skipped run derived from a plan
// instance, guaranteeing the durable missed-run entry and the instance refer
// to the same scheduled time.
func (sm *StateManager) AddMissedBackupForInstance(instance *PlanInstance, reason string) error {
	missed := MissedBackup{
		ScheduledTime: instance.ScheduledTime,
		Reason:        reason,
		IsWeekly:      instance.IsWeekly,
		Weekday:       instance.Weekday,
	}
	return sm.appendMissed(missed)
}

// appendMissed persists one missed-run entry, bounding the retained history.
func (sm *StateManager) appendMissed(missed MissedBackup) error {
	sm.logger.Warn("Recording missed backup",
		logger.Time("scheduled_time", missed.ScheduledTime),
		logger.String("reason", missed.Reason),
		logger.Bool("is_weekly", missed.IsWeekly),
		logger.String("weekday", missed.Weekday))
	return sm.mutate("add_missed_backup", func() error {
		sm.state.MissedRuns = append(sm.state.MissedRuns, missed)
		if len(sm.state.MissedRuns) > maxStoredMissedRuns {
			sm.state.MissedRuns = slices.Clone(sm.state.MissedRuns[len(sm.state.MissedRuns)-maxStoredMissedRuns:])
		}
		return nil
	})
}

// UpdateTargetState updates the state for a specific backup target
func (sm *StateManager) UpdateTargetState(targetName string, metadata *Metadata, status string) error {
	sm.logger.Debug("Updating target state",
		logger.String("target_name", targetName),
		logger.String("backup_id", metadata.ID),
		logger.String("status", status))
	return sm.mutate("update_target_state", func() error {
		state := sm.state.Targets[targetName]
		state.LastBackupID = metadata.ID
		state.LastBackupTime = metadata.Timestamp
		state.LastBackupStatus = status
		state.LastBackupSize = metadata.Size
		if status == "success" {
			state.TotalBackups++
			state.TotalSize += metadata.Size
		}
		sm.state.Targets[targetName] = state
		return nil
	})
}

// UpdateStats updates the backup statistics
func (sm *StateManager) UpdateStats(stats map[string]BackupStats) error {
	sm.logger.Debug("Updating overall backup stats")
	return sm.mutate("update_stats", func() error {
		sm.state.Stats = maps.Clone(stats)
		return nil
	})
}

// StartInstance persists a new plan instance in StatusRunning. If an instance
// with the same ID already exists the call is a no-op and returns the
// existing instance with created=false, which makes re-submitting the same
// scheduled tick idempotent across goroutines and restarts.
func (sm *StateManager) StartInstance(instance *PlanInstance) (existing *PlanInstance, created bool, err error) {
	if instance == nil {
		return nil, false, errors.Newf("plan instance cannot be nil").
			Component("backup").
			Category(errors.CategoryValidation).
			Context("operation", "start_instance").
			Build()
	}
	if instance.ID == "" {
		return nil, false, errors.Newf("plan instance ID cannot be empty").
			Component("backup").
			Category(errors.CategoryValidation).
			Context("operation", "start_instance").
			Build()
	}
	err = sm.mutate("start_instance", func() error {
		if current, ok := sm.state.Instances[instance.ID]; ok {
			cur := current
			existing = &cur
			return nil
		}
		stored := *instance
		if stored.Status == "" {
			stored.Status = StatusRunning
		}
		if stored.StartedAt.IsZero() {
			stored.StartedAt = time.Now()
		}
		if stored.Targets == nil {
			stored.Targets = make(map[string]TargetRunResult)
		}
		sm.state.Instances[stored.ID] = stored
		storedCopy := stored
		existing = &storedCopy
		created = true
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return existing, created, nil
}

// RecordInstanceBackupIDs merges produced backup IDs into an instance so a
// later recovery knows exactly which archives belong to it.
func (sm *StateManager) RecordInstanceBackupIDs(instanceID string, backupIDs ...string) error {
	return sm.mutate("record_instance_backup_ids", func() error {
		inst, ok := sm.state.Instances[instanceID]
		if !ok {
			return errors.Newf("plan instance %q not found", instanceID).
				Component("backup").
				Category(errors.CategoryValidation).
				Context("operation", "record_instance_backup_ids").
				Build()
		}
		inst.BackupIDs = mergeStrings(inst.BackupIDs, backupIDs)
		sm.state.Instances[instanceID] = inst
		return nil
	})
}

// RecordInstanceTarget merges one target's outcome into a plan instance.
// Repeated outcomes for the same target (source by source) accumulate backup
// IDs and sizes and a failure in any source keeps the aggregate status
// failed. This is the durable progress record used to skip already-completed
// target writes during recovery.
func (sm *StateManager) RecordInstanceTarget(instanceID string, result *TargetRunResult) error {
	return sm.mutate("record_instance_target", func() error {
		inst, ok := sm.state.Instances[instanceID]
		if !ok {
			return errors.Newf("plan instance %q not found", instanceID).
				Component("backup").
				Category(errors.CategoryValidation).
				Context("operation", "record_instance_target").
				Build()
		}
		if inst.Targets == nil {
			inst.Targets = make(map[string]TargetRunResult)
		}
		current := inst.Targets[result.Name]
		if current.Name == "" {
			current.Name = result.Name
		}
		current.BackupIDs = mergeStrings(current.BackupIDs, result.BackupIDs)
		current.Size += result.Size
		switch {
		case current.Status == targetOutcomeFailed || result.Status == targetOutcomeFailed:
			current.Status = targetOutcomeFailed
			if result.Error != "" {
				current.Error = result.Error
			}
		case result.Status == targetOutcomeSkipped:
			// Skipped outcomes ("target already held this archive") must not
			// downgrade a genuine success/failure recorded earlier.
			if current.Status == "" {
				current.Status = targetOutcomeSuccess
			}
		default:
			current.Status = result.Status
			if result.Error != "" {
				current.Error = result.Error
			}
		}
		inst.Targets[result.Name] = current
		sm.state.Instances[instanceID] = inst
		return nil
	})
}

// FinishInstance records the terminal status of a plan instance. The first
// terminal result wins: finishing an instance that already has a terminal
// status is an idempotent no-op so duplicated completion paths (for example a
// timeout racing a failure) cannot rewrite history.
func (sm *StateManager) FinishInstance(instanceID string, status RunStatus, failureReason string) error {
	if !status.IsTerminal() {
		return errors.Newf("plan instance status %q is not terminal", status).
			Component("backup").
			Category(errors.CategoryValidation).
			Context("operation", "finish_instance").
			Build()
	}
	return sm.mutate("finish_instance", func() error {
		inst, ok := sm.state.Instances[instanceID]
		if !ok {
			return errors.Newf("plan instance %q not found", instanceID).
				Component("backup").
				Category(errors.CategoryValidation).
				Context("operation", "finish_instance").
				Build()
		}
		if inst.Status.IsTerminal() {
			// First terminal outcome is authoritative; keep it.
			return nil
		}
		inst.Status = status
		inst.FinishedAt = time.Now()
		if failureReason != "" {
			inst.Error = failureReason
		}
		sm.state.Instances[instanceID] = inst
		sm.pruneInstancesLocked()
		return nil
	})
}

// ReopenInstance returns a previously terminal instance to running state.
// It is used when a graceful shutdown interrupted a run so that the next
// process start recovers it instead of leaving a false terminal record.
func (sm *StateManager) ReopenInstance(instanceID, reason string) error {
	return sm.mutate("reopen_instance", func() error {
		inst, ok := sm.state.Instances[instanceID]
		if !ok {
			return errors.Newf("plan instance %q not found", instanceID).
				Component("backup").
				Category(errors.CategoryValidation).
				Context("operation", "reopen_instance").
				Build()
		}
		inst.Status = StatusRunning
		inst.FinishedAt = time.Time{}
		if reason != "" {
			inst.Error = reason
		}
		sm.state.Instances[instanceID] = inst
		return nil
	})
}

// SetInstanceTrigger records what (re)started a non-terminal instance, for
// example when startup recovery resumes a plan left running by a previous
// process. It is a no-op on terminal instances.
func (sm *StateManager) SetInstanceTrigger(instanceID string, trigger Trigger) error {
	return sm.mutate("set_instance_trigger", func() error {
		inst, ok := sm.state.Instances[instanceID]
		if !ok || inst.Status.IsTerminal() {
			return nil
		}
		inst.Trigger = trigger
		sm.state.Instances[instanceID] = inst
		return nil
	})
}

// pruneInstancesLocked retains every non-terminal instance and at most
// maxStoredTerminalInstances terminal instances, dropping the oldest
// finished ones. Callers must hold sm.mu.
func (sm *StateManager) pruneInstancesLocked() {
	terminal := make([]PlanInstance, 0, len(sm.state.Instances))
	for id := range sm.state.Instances {
		if sm.state.Instances[id].Status.IsTerminal() {
			terminal = append(terminal, sm.state.Instances[id])
		}
	}
	if len(terminal) <= maxStoredTerminalInstances {
		return
	}
	slices.SortFunc(terminal, func(a, b PlanInstance) int {
		return b.FinishedAt.Compare(a.FinishedAt)
	})
	drop := terminal[maxStoredTerminalInstances:]
	for i := range drop {
		delete(sm.state.Instances, drop[i].ID)
	}
}

// GetInstance returns a copy of one plan instance.
func (sm *StateManager) GetInstance(instanceID string) (PlanInstance, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	inst, ok := sm.state.Instances[instanceID]
	return cloneInstance(&inst), ok
}

// GetInstances returns all plan instances ordered by scheduled time, newest
// first.
func (sm *StateManager) GetInstances() []PlanInstance {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	instances := make([]PlanInstance, 0, len(sm.state.Instances))
	for id := range sm.state.Instances {
		inst := sm.state.Instances[id]
		instances = append(instances, cloneInstance(&inst))
	}
	slices.SortFunc(instances, func(a, b PlanInstance) int {
		return b.ScheduledTime.Compare(a.ScheduledTime)
	})
	return instances
}

// RecoverableInstances returns instances that were still running when the
// previous process exited, ordered oldest scheduled time first so they are
// resumed deterministically.
func (sm *StateManager) RecoverableInstances() []PlanInstance {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	pending := make([]PlanInstance, 0)
	for id := range sm.state.Instances {
		inst := sm.state.Instances[id]
		if inst.Status == StatusRunning {
			pending = append(pending, cloneInstance(&inst))
		}
	}
	slices.SortFunc(pending, func(a, b PlanInstance) int {
		return a.ScheduledTime.Compare(b.ScheduledTime)
	})
	return pending
}

// cloneInstance returns a deep enough copy of inst for callers outside the
// lock.
func cloneInstance(inst *PlanInstance) PlanInstance {
	cloned := *inst
	cloned.BackupIDs = slices.Clone(inst.BackupIDs)
	if inst.Targets != nil {
		cloned.Targets = maps.Clone(inst.Targets)
		for name := range cloned.Targets {
			outcome := cloned.Targets[name]
			outcome.BackupIDs = slices.Clone(outcome.BackupIDs)
			cloned.Targets[name] = outcome
		}
	}
	return cloned
}

// mergeStrings appends values not already present, preserving order.
func mergeStrings(existing, additional []string) []string {
	seen := make(map[string]struct{}, len(existing)+len(additional))
	merged := make([]string, 0, len(existing)+len(additional))
	for _, v := range existing {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		merged = append(merged, v)
	}
	for _, v := range additional {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		merged = append(merged, v)
	}
	return merged
}

// GetScheduleState returns the state of a specific schedule
func (sm *StateManager) GetScheduleState(schedule *BackupSchedule) ScheduleState {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.state.Schedules[scheduleKey(schedule)]
}

// GetTargetState returns the state of a specific target
func (sm *StateManager) GetTargetState(targetName string) TargetState {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.state.Targets[targetName]
}

// GetMissedBackups returns all missed backups
func (sm *StateManager) GetMissedBackups() []MissedBackup {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	// Return a copy to prevent modification
	return slices.Clone(sm.state.MissedRuns)
}

// ClearMissedBackups clears the list of missed backups
func (sm *StateManager) ClearMissedBackups() error {
	return sm.mutate("clear_missed_backups", func() error {
		sm.logger.Info("Clearing missed backups list", logger.Int("previous_count", len(sm.state.MissedRuns)))
		sm.state.MissedRuns = make([]MissedBackup, 0)
		return nil
	})
}

// GetStats returns the current overall statistics.
func (sm *StateManager) GetStats() map[string]BackupStats {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	// Return a deep copy to prevent modification of the internal state map
	return maps.Clone(sm.state.Stats)
}
