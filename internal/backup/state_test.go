package backup

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tphakala/birdnet-go/internal/errors"
)

// newTestStateManager builds a StateManager backed by a file in t.TempDir so
// tests never touch the real configuration directory.
func newTestStateManager(t *testing.T) *StateManager {
	t.Helper()
	sm, err := newStateManagerAtPath(filepath.Join(t.TempDir(), "backup-state.json"))
	require.NoError(t, err)
	return sm
}

// TestStateManager_UpdateScheduleStatePersists pins that mutating schedule
// state returns and is durable. The fixed-version implementation deadlocked
// itself by taking the write lock and then calling a save function that took
// the same non-reentrant lock again, so this call would hang forever.
func TestStateManager_UpdateScheduleStatePersists(t *testing.T) {
	sm := newTestStateManager(t)

	done := make(chan struct{})
	go func() {
		defer close(done)
		schedule := &BackupSchedule{NextRun: time.Now().Add(time.Hour)}
		err := sm.UpdateScheduleState(schedule, true)
		assert.NoError(t, err)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("UpdateScheduleState deadlocked")
	}

	stored := sm.GetScheduleState(&BackupSchedule{})
	require.False(t, stored.LastSuccessful.IsZero())
	require.Equal(t, 0, stored.FailureCount)

	// A failure increments the failure count instead of resetting success.
	failedSchedule := &BackupSchedule{}
	require.NoError(t, sm.UpdateScheduleState(failedSchedule, false))
	stored = sm.GetScheduleState(&BackupSchedule{})
	assert.Equal(t, 1, stored.FailureCount)
	assert.False(t, stored.LastAttempted.IsZero())
}

// TestStateManager_PlanInstanceLifecycle covers the unique, recoverable
// lifecycle of a plan instance: start is idempotent, target progress merges,
// the first terminal status wins and only running instances are recovered.
func TestStateManager_PlanInstanceLifecycle(t *testing.T) {
	sm := newTestStateManager(t)
	scheduled := time.Date(2026, 10, 8, 2, 0, 0, 0, time.UTC)
	schedule := &BackupSchedule{NextRun: scheduled}
	instance := &PlanInstance{
		ID:            PlanInstanceID(schedule, scheduled),
		ScheduleKey:   scheduleKey(schedule),
		ScheduledTime: scheduled,
		Status:        StatusRunning,
		Trigger:       TriggerSchedule,
		Targets:       map[string]TargetRunResult{},
	}

	_, created, err := sm.StartInstance(instance)
	require.NoError(t, err)
	assert.True(t, created)

	// Re-recording the same scheduled time is a no-op: one tick can never
	// produce two instances.
	_, created, err = sm.StartInstance(instance)
	require.NoError(t, err)
	assert.False(t, created)

	require.NoError(t, sm.RecordInstanceBackupIDs(instance.ID, "sqlite-20261008-020000"))
	require.NoError(t, sm.RecordInstanceTarget(instance.ID, &TargetRunResult{
		Name:      "local",
		Status:    targetOutcomeSuccess,
		BackupIDs: []string{"sqlite-20261008-020000"},
		Size:      42,
	}))

	got, ok := sm.GetInstance(instance.ID)
	require.True(t, ok)
	assert.Equal(t, StatusRunning, got.Status)
	require.Contains(t, got.Targets, "local")
	assert.Equal(t, []string{"sqlite-20261008-020000"}, got.Targets["local"].BackupIDs)

	// Only running instances are recoverable.
	recoverable := sm.RecoverableInstances()
	require.Len(t, recoverable, 1)
	assert.Equal(t, instance.ID, recoverable[0].ID)

	require.NoError(t, sm.FinishInstance(instance.ID, StatusSuccess, ""))

	// First terminal result wins: a later racing failure cannot rewrite it.
	require.NoError(t, sm.FinishInstance(instance.ID, StatusFailed, "late failure"))
	got, ok = sm.GetInstance(instance.ID)
	require.True(t, ok)
	assert.Equal(t, StatusSuccess, got.Status)
	assert.False(t, got.FinishedAt.IsZero())

	assert.Empty(t, sm.RecoverableInstances())
}

// TestStateManager_FailedTargetOutcomeWins ensures a later failure for a
// target marks the aggregate target outcome failed even if an earlier source
// succeeded, so partial failures can never be hidden.
func TestStateManager_FailedTargetOutcomeWins(t *testing.T) {
	sm := newTestStateManager(t)
	inst := &PlanInstance{
		ID:            "daily@20261008T020000Z",
		ScheduleKey:   "daily",
		ScheduledTime: time.Date(2026, 10, 8, 2, 0, 0, 0, time.UTC),
		Status:        StatusRunning,
		Targets:       map[string]TargetRunResult{},
	}
	_, _, err := sm.StartInstance(inst)
	require.NoError(t, err)

	require.NoError(t, sm.RecordInstanceTarget(inst.ID, &TargetRunResult{
		Name: "local", Status: targetOutcomeSuccess, BackupIDs: []string{"id1"}, Size: 10,
	}))
	require.NoError(t, sm.RecordInstanceTarget(inst.ID, &TargetRunResult{
		Name: "sftp", Status: targetOutcomeSuccess, BackupIDs: []string{"id1"}, Size: 10,
	}))
	require.NoError(t, sm.RecordInstanceTarget(inst.ID, &TargetRunResult{
		Name: "sftp", Status: targetOutcomeFailed, BackupIDs: []string{"id1"}, Error: "connection reset",
	}))

	got, ok := sm.GetInstance(inst.ID)
	require.True(t, ok)
	assert.Equal(t, targetOutcomeSuccess, got.Targets["local"].Status)
	assert.Equal(t, targetOutcomeFailed, got.Targets["sftp"].Status)
	assert.Equal(t, "connection reset", got.Targets["sftp"].Error)
}

// TestStateManager_CloseRejectsWrites verifies the shutdown boundary: after
// Close no mutator can write to the state file.
func TestStateManager_CloseRejectsWrites(t *testing.T) {
	sm := newTestStateManager(t)
	require.NoError(t, sm.Close())
	assert.True(t, sm.IsClosed())

	// Idempotent.
	require.NoError(t, sm.Close())

	err := sm.UpdateScheduleState(&BackupSchedule{}, true)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrStateClosed))

	_, _, err = sm.StartInstance(&PlanInstance{ID: "x"})
	assert.True(t, errors.Is(err, ErrStateClosed))

	err = sm.FinishInstance("x", StatusFailed, "")
	assert.True(t, errors.Is(err, ErrStateClosed))
}

// TestStateManager_PersistsAcrossRestart verifies a running instance survives
// a state manager reload, which is what makes restart recovery work.
func TestStateManager_PersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "backup-state.json")
	sm, err := newStateManagerAtPath(path)
	require.NoError(t, err)

	inst := &PlanInstance{
		ID:            "daily@20261008T020000Z",
		ScheduleKey:   "daily",
		ScheduledTime: time.Date(2026, 10, 8, 2, 0, 0, 0, time.UTC),
		Status:        StatusRunning,
		Trigger:       TriggerSchedule,
		Targets: map[string]TargetRunResult{
			"local": {
				Name:      "local",
				Status:    targetOutcomeSuccess,
				BackupIDs: []string{"sqlite-20261008-020000"},
				Size:      7,
			},
		},
	}
	_, _, err = sm.StartInstance(inst)
	require.NoError(t, err)

	reloaded, err := newStateManagerAtPath(path)
	require.NoError(t, err)
	pending := reloaded.RecoverableInstances()
	require.Len(t, pending, 1)
	assert.Equal(t, "daily@20261008T020000Z", pending[0].ID)
	require.Contains(t, pending[0].Targets, "local")
	assert.Equal(t, []string{"sqlite-20261008-020000"}, pending[0].Targets["local"].BackupIDs)
}

// TestStateManager_OldStateFileLoads verifies state files written before plan
// instances existed still load (nil maps are initialized).
func TestStateManager_OldStateFileLoads(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "backup-state.json")
	oldFormat := `{"last_update":null,"schedules":{},"targets":{},"missed_runs":[],"stats":{}}`
	require.NoError(t, writeTestFile(path, []byte(oldFormat)))

	sm, err := newStateManagerAtPath(path)
	require.NoError(t, err)
	assert.NotNil(t, sm.GetInstances())
	assert.Empty(t, sm.GetInstances())
	require.NoError(t, sm.UpdateStats(map[string]BackupStats{}))
}
