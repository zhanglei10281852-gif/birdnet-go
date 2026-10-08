package backup

import (
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tphakala/birdnet-go/internal/conf"
	"github.com/tphakala/birdnet-go/internal/errors"
)

// errFakeTargetFailure is returned by scripted failing targets.
var errFakeTargetFailure = errors.NewStd("simulated target failure")

// lifecycleHarness wires a real Scheduler/Manager/StateManager with
// controllable fakes and an on-disk state file, so tests exercise the same
// production code paths as the application bootstrap.
type lifecycleHarness struct {
	t         *testing.T
	statePath string
	settings  *conf.Settings
	state     *StateManager
	manager   *Manager
	sched     *Scheduler
	source    *fakeSource
	targets   map[string]*fakeTarget
}

// harnessConfig allows individual tests to tune the harness.
type harnessConfig struct {
	enabled       bool
	backupTimeout time.Duration
	storeTimeout  time.Duration
}

func newLifecycleHarness(t *testing.T, cfg harnessConfig, targetDefs ...*fakeTarget) *lifecycleHarness {
	t.Helper()
	settings := &conf.Settings{}
	settings.Backup.Enabled = cfg.enabled
	if cfg.backupTimeout > 0 {
		settings.Backup.OperationTimeouts.Backup = cfg.backupTimeout
	}
	if cfg.storeTimeout > 0 {
		settings.Backup.OperationTimeouts.Store = cfg.storeTimeout
	}

	statePath := filepath.Join(t.TempDir(), "backup-state.json")
	sm, err := newStateManagerAtPath(statePath)
	require.NoError(t, err)

	manager, err := NewManager(settings, GetLogger(), sm, "test")
	require.NoError(t, err)

	source := &fakeSource{name: "sqlite", data: []byte("backup payload")}
	require.NoError(t, manager.RegisterSource(source))

	targets := make(map[string]*fakeTarget, len(targetDefs))
	for _, target := range targetDefs {
		require.NoError(t, manager.RegisterTarget(target))
		targets[target.Name()] = target
	}

	sched, err := NewScheduler(manager, GetLogger(), sm)
	require.NoError(t, err)

	h := &lifecycleHarness{
		t:         t,
		statePath: statePath,
		settings:  settings,
		state:     sm,
		manager:   manager,
		sched:     sched,
		source:    source,
		targets:   targets,
	}
	t.Cleanup(h.stopIfRunning)
	return h
}

func (h *lifecycleHarness) stopIfRunning() {
	h.sched.Stop()
}

// loadDailyDue installs one daily schedule whose next run is scheduledTime.
func (h *lifecycleHarness) loadDailyDue(scheduledTime time.Time) {
	h.t.Helper()
	require.NoError(h.t, h.sched.AddSchedule(scheduledTime.Hour(), scheduledTime.Minute(), -1, false))
	h.sched.mu.Lock()
	h.sched.schedules[0].NextRun = scheduledTime
	h.sched.mu.Unlock()
}

// loadWeeklyDue installs a daily and a weekly schedule both due at
// scheduledTime, to exercise concurrent plan overlap.
func (h *lifecycleHarness) loadTwoDueSchedules(scheduledTime time.Time) {
	h.t.Helper()
	require.NoError(h.t, h.sched.AddSchedule(scheduledTime.Hour(), scheduledTime.Minute(), -1, false))
	require.NoError(h.t, h.sched.AddSchedule(scheduledTime.Hour(), scheduledTime.Minute(), scheduledTime.Weekday(), true))
	h.sched.mu.Lock()
	h.sched.schedules[0].NextRun = scheduledTime
	h.sched.schedules[1].NextRun = scheduledTime
	h.sched.mu.Unlock()
}

// instanceIDFor returns the deterministic instance ID for a scheduled time.
func instanceIDFor(scheduledTime time.Time) string {
	return PlanInstanceID(&BackupSchedule{}, scheduledTime)
}

// weeklyInstanceIDFor returns the weekly schedule instance ID.
func weeklyInstanceIDFor(scheduledTime time.Time) string {
	return PlanInstanceID(&BackupSchedule{IsWeekly: true, Weekday: scheduledTime.Weekday()}, scheduledTime)
}

// waitForStatus blocks until the instance reaches one of the wanted statuses
// or fails the test on timeout, then returns the stored instance.
func (h *lifecycleHarness) waitForStatus(id string, wanted ...RunStatus) PlanInstance {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	require.Eventually(h.t, func() bool {
		inst, ok := h.state.GetInstance(id)
		if !ok || !inst.Status.IsTerminal() {
			return false
		}
		return slices.Contains(wanted, inst.Status)
	}, time.Until(deadline), 2*time.Millisecond, "instance %s did not reach %v", id, wanted)
	inst, ok := h.state.GetInstance(id)
	require.True(h.t, ok)
	return inst
}

// waitTerminal waits for any terminal status.
func (h *lifecycleHarness) waitTerminal(id string) PlanInstance {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	require.Eventually(h.t, func() bool {
		inst, ok := h.state.GetInstance(id)
		return ok && inst.Status.IsTerminal()
	}, time.Until(deadline), 2*time.Millisecond)
	inst, _ := h.state.GetInstance(id)
	return inst
}

// waitActive waits until a backup holds the run lease.
func (h *lifecycleHarness) waitActive() {
	h.t.Helper()
	require.Eventually(h.t, func() bool {
		return h.sched.Status().Active != nil
	}, 2*time.Second, time.Millisecond)
}

// TestScheduler_DueSchedule_Success covers the happy path: a due schedule runs
// the manager, stores to the target, records one successful instance and
// advances the in-memory next-run time.
func TestScheduler_DueSchedule_Success(t *testing.T) {
	t.Parallel()
	target := &fakeTarget{name: "local"}
	h := newLifecycleHarness(t, harnessConfig{enabled: true}, target)

	scheduled := time.Now().Add(-30 * time.Second).Truncate(time.Second)
	h.loadDailyDue(scheduled)
	h.sched.Start()
	h.sched.checkSchedules(scheduled.Add(30 * time.Second))

	id := instanceIDFor(scheduled)
	inst := h.waitForStatus(id, StatusSuccess)

	assert.Equal(t, targetOutcomeSuccess, inst.Targets["local"].Status)
	ids := target.storedBackupIDs()
	require.Len(t, ids, 1)
	assert.Equal(t, "sqlite-"+scheduled.UTC().Format("20060102-150405"), ids[0])

	// The schedule advanced to a future run.
	h.sched.mu.RLock()
	nextRun := h.sched.schedules[0].NextRun
	h.sched.mu.RUnlock()
	assert.True(t, nextRun.After(scheduled))

	// Success updated statistics but did not append missed-run entries.
	assert.NotEmpty(t, h.state.GetStats())
	assert.Empty(t, h.state.GetMissedBackups())
	assert.Equal(t, 1, h.state.GetTargetState("local").TotalBackups)
}

// TestScheduler_OverlappingPlans_SecondIsSkipped verifies that when two plans
// are due while a backup holds the lease, the second plan gets a unique,
// durable "skipped" instance instead of running concurrently or vanishing
// into a log line.
func TestScheduler_OverlappingPlans_SecondIsSkipped(t *testing.T) {
	t.Parallel()
	gate, release := newGate()
	target := &fakeTarget{name: "local", gate: gate}
	h := newLifecycleHarness(t, harnessConfig{enabled: true}, target)
	t.Cleanup(release)

	scheduled := time.Now().Add(-30 * time.Second).Truncate(time.Second)
	h.loadTwoDueSchedules(scheduled)
	h.sched.Start()
	h.sched.checkSchedules(scheduled.Add(30 * time.Second))

	dailyID := instanceIDFor(scheduled)
	weeklyID := weeklyInstanceIDFor(scheduled)

	skipped := h.waitForStatus(weeklyID, StatusSkipped)
	assert.Equal(t, StatusSkipped, skipped.Status)
	assert.Contains(t, skipped.Error, "skipped")

	h.waitActive()
	release()

	success := h.waitForStatus(dailyID, StatusSuccess)
	assert.Equal(t, StatusSuccess, success.Status)

	// Exactly one backup ran; the skipped plan never touched the target.
	assert.Len(t, target.storedBackupIDs(), 1)

	// The skip is visible through the missed-run view with the correct time.
	missed := h.state.GetMissedBackups()
	require.Len(t, missed, 1)
	assert.Equal(t, scheduled.UTC().Format(time.RFC3339), missed[0].ScheduledTime.UTC().Format(time.RFC3339))
}

// TestScheduler_PartialTargetFailure_MarksInstanceFailed proves a run where
// only some targets succeed can never be recorded as a success: the instance
// is failed, the good target keeps its success, stats/cleanup are skipped and
// the failure is visible through the missed-run view.
func TestScheduler_PartialTargetFailure_MarksInstanceFailed(t *testing.T) {
	t.Parallel()
	good := &fakeTarget{name: "local"}
	bad := &fakeTarget{name: "sftp", failStorage: true}
	h := newLifecycleHarness(t, harnessConfig{enabled: true}, good, bad)

	scheduled := time.Now().Add(-20 * time.Second).Truncate(time.Second)
	h.loadDailyDue(scheduled)
	h.sched.Start()
	h.sched.checkSchedules(scheduled.Add(20 * time.Second))

	inst := h.waitForStatus(instanceIDFor(scheduled), StatusFailed)
	assert.Equal(t, targetOutcomeSuccess, inst.Targets["local"].Status)
	assert.Equal(t, targetOutcomeFailed, inst.Targets["sftp"].Status)

	assert.Len(t, good.storedBackupIDs(), 1)
	assert.Len(t, bad.storedBackupIDs(), 1) // attempted once
	assert.Equal(t, 1, h.state.GetTargetState("local").TotalBackups)
	assert.Equal(t, 0, h.state.GetTargetState("sftp").TotalBackups)
	assert.Equal(t, "failed", h.state.GetTargetState("sftp").LastBackupStatus)

	// Stats and cleanup run only after a fully successful run.
	assert.Empty(t, h.state.GetStats())

	// Failure recorded against the schedule and surfaced as a missed run.
	assert.Equal(t, 1, h.state.GetScheduleState(&BackupSchedule{}).FailureCount)
	require.Len(t, h.state.GetMissedBackups(), 1)
}

// TestScheduler_ContextTimeout_MarksInstanceTimeout verifies a run whose
// context deadline expires is classified timeout and the cancellation reaches
// the target Store call.
func TestScheduler_ContextTimeout_MarksInstanceTimeout(t *testing.T) {
	t.Parallel()
	gate, _ := newGate()
	target := &fakeTarget{name: "local", gate: gate}
	h := newLifecycleHarness(t, harnessConfig{
		enabled:       true,
		backupTimeout: 200 * time.Millisecond,
	}, target)

	scheduled := time.Now().Add(-10 * time.Second).Truncate(time.Second)
	h.loadDailyDue(scheduled)
	h.sched.Start()
	h.sched.checkSchedules(scheduled.Add(10 * time.Second))

	inst := h.waitForStatus(instanceIDFor(scheduled), StatusTimeout)
	assert.Equal(t, StatusTimeout, inst.Status)
	assert.GreaterOrEqual(t, int(target.ctxCanceled.Load()), 1)
}

// TestScheduler_StopCancelsAndWaits_LeavesInstanceRecoverable covers the
// shutdown requirement: Stop cancels the in-flight run, waits for it, and the
// interrupted plan stays in running state so the next process start recovers
// it. Nothing is written after the state store is closed.
func TestScheduler_StopCancelsAndWaits_LeavesInstanceRecoverable(t *testing.T) {
	t.Parallel()
	gate, release := newGate()
	target := &fakeTarget{name: "local", gate: gate, entered: make(chan struct{})}
	h := newLifecycleHarness(t, harnessConfig{enabled: true}, target)
	t.Cleanup(release)

	scheduled := time.Now().Add(-15 * time.Second).Truncate(time.Second)
	h.loadDailyDue(scheduled)
	h.sched.Start()
	h.sched.checkSchedules(scheduled.Add(15 * time.Second))
	h.waitActive()
	// Wait until the target Store call is actually blocked on the gate, so
	// the shutdown cancellation has somewhere to be observed.
	select {
	case <-target.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("target Store never entered")
	}

	stopped := make(chan struct{})
	go func() {
		h.sched.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("Stop did not wait for and cancel the in-flight backup")
	}

	assert.GreaterOrEqual(t, int(target.ctxCanceled.Load()), 1)

	// The instance is still recoverable after reload (simulated restart).
	reloaded, err := newStateManagerAtPath(h.statePath)
	require.NoError(t, err)
	pending := reloaded.RecoverableInstances()
	require.Len(t, pending, 1)
	assert.Equal(t, instanceIDFor(scheduled), pending[0].ID)

	// Scheduler stop alone does not close the state store; service shutdown
	// does that afterwards, and writes then fail.
	assert.False(t, h.state.IsClosed())
	require.NoError(t, h.state.Close())
	assert.True(t, errors.Is(h.state.UpdateStats(nil), ErrStateClosed))
}

// TestScheduler_StopStartInterleave_Idempotent verifies repeated Stop/Start
// calls are safe and a restarted scheduler still executes due plans.
func TestScheduler_StopStartInterleave_Idempotent(t *testing.T) {
	t.Parallel()
	target := &fakeTarget{name: "local"}
	h := newLifecycleHarness(t, harnessConfig{enabled: true}, target)

	scheduled := time.Now().Add(-30 * time.Second).Truncate(time.Second)
	h.loadDailyDue(scheduled)

	h.sched.Start()
	h.sched.Start() // idempotent
	h.sched.Stop()
	h.sched.Stop() // idempotent, must not block

	// Restart the scheduler in-process; ticker-driven runs work again.
	h.sched.Start()
	h.sched.checkSchedules(scheduled.Add(30 * time.Second))
	h.waitForStatus(instanceIDFor(scheduled), StatusSuccess)
	h.sched.Stop()
	assert.Len(t, target.storedBackupIDs(), 1)
}

// TestScheduler_RecoverySkipsCompletedTarget simulates a crash mid-run: the
// state file holds a running instance with one completed target. After
// restart the completed target is never written again while the remaining
// target completes, and a second restart does not re-run the terminal plan.
func TestScheduler_RecoverySkipsCompletedTarget(t *testing.T) {
	t.Parallel()
	scheduled := time.Date(2026, 10, 8, 2, 0, 0, 0, time.UTC)
	backupID := "sqlite-20261008-020000"

	// Phase A: a previous process recorded progress then died.
	stateDir := t.TempDir()
	statePath := filepath.Join(stateDir, "backup-state.json")
	sm, err := newStateManagerAtPath(statePath)
	require.NoError(t, err)
	inst := &PlanInstance{
		ID:            "daily@" + scheduled.UTC().Format("20060102T150405Z"),
		ScheduleKey:   "daily",
		ScheduledTime: scheduled,
		Status:        StatusRunning,
		Trigger:       TriggerSchedule,
		Targets: map[string]TargetRunResult{
			"local": {
				Name:      "local",
				Status:    targetOutcomeSuccess,
				BackupIDs: []string{backupID},
				Size:      123,
			},
		},
	}
	_, _, err = sm.StartInstance(inst)
	require.NoError(t, err)
	require.NoError(t, sm.RecordInstanceBackupIDs(inst.ID, backupID))
	require.NoError(t, sm.Close())

	// Phase B: new process bootstraps manager/scheduler from the state file.
	local := &fakeTarget{name: "local"}
	sftp := &fakeTarget{name: "sftp"}
	h := newLifecycleHarness(t, harnessConfig{enabled: true}, local, sftp)
	// Reuse the crashed state file.
	crashedSM, err := newStateManagerAtPath(statePath)
	require.NoError(t, err)
	h.state = crashedSM
	h.sched.state = crashedSM
	h.manager.stateManager = crashedSM

	require.NoError(t, h.sched.AddSchedule(2, 0, -1, false))
	h.sched.mu.Lock()
	h.sched.schedules[0].NextRun = scheduled
	h.sched.mu.Unlock()

	h.sched.Start()

	recovered := h.waitForStatus(inst.ID, StatusSuccess)
	assert.Equal(t, TriggerRecovery, recovered.Trigger)
	assert.Equal(t, targetOutcomeSuccess, recovered.Targets["local"].Status)
	assert.Equal(t, targetOutcomeSuccess, recovered.Targets["sftp"].Status)

	// The already-completed target was not written to again.
	assert.Empty(t, local.storedBackupIDs())
	sftpIDs := sftp.storedBackupIDs()
	require.Len(t, sftpIDs, 1)
	assert.Equal(t, backupID, sftpIDs[0])
	assert.Equal(t, 0, h.state.GetTargetState("local").TotalBackups, "completed target must not be double counted")

	// Phase C: a further restart finds nothing recoverable and writes nothing.
	h.sched.Stop()
	require.NoError(t, h.state.Close())

	sm3, err := newStateManagerAtPath(statePath)
	require.NoError(t, err)
	assert.Empty(t, sm3.RecoverableInstances())
	settings := &conf.Settings{}
	settings.Backup.Enabled = true
	mgr3, err := NewManager(settings, GetLogger(), sm3, "test")
	require.NoError(t, err)
	require.NoError(t, mgr3.RegisterSource(&fakeSource{name: "sqlite", data: []byte("x")}))
	require.NoError(t, mgr3.RegisterTarget(local))
	require.NoError(t, mgr3.RegisterTarget(sftp))
	sched3, err := NewScheduler(mgr3, GetLogger(), sm3)
	require.NoError(t, err)
	sched3.Start()
	time.Sleep(100 * time.Millisecond) // recovery is synchronous at start; give the goroutine a moment
	sched3.Stop()

	assert.Empty(t, local.storedBackupIDs(), "terminal plan must not be re-executed")
	before := len(sftp.storedBackupIDs())
	assert.Equal(t, 1, before)
}

// TestScheduler_RecoveryWithBackupDisabled_CancelsPlans ensures interrupted
// plans do not linger forever when backup is disabled at startup.
func TestScheduler_RecoveryWithBackupDisabled_CancelsPlans(t *testing.T) {
	t.Parallel()
	scheduled := time.Date(2026, 10, 8, 2, 0, 0, 0, time.UTC)
	statePath := filepath.Join(t.TempDir(), "backup-state.json")
	sm, err := newStateManagerAtPath(statePath)
	require.NoError(t, err)
	_, _, err = sm.StartInstance(&PlanInstance{
		ID:            "daily@" + scheduled.UTC().Format("20060102T150405Z"),
		ScheduleKey:   "daily",
		ScheduledTime: scheduled,
		Status:        StatusRunning,
		Targets:       map[string]TargetRunResult{},
	})
	require.NoError(t, err)
	require.NoError(t, sm.Close())

	target := &fakeTarget{name: "local"}
	h := newLifecycleHarness(t, harnessConfig{enabled: false}, target)
	crashedSM, err := newStateManagerAtPath(statePath)
	require.NoError(t, err)
	h.state = crashedSM
	h.sched.state = crashedSM
	h.manager.stateManager = crashedSM

	h.sched.Start()
	require.Eventually(t, func() bool {
		inst, ok := crashedSM.GetInstance("daily@" + scheduled.UTC().Format("20060102T150405Z"))
		return ok && inst.Status == StatusCanceled
	}, 2*time.Second, 5*time.Millisecond)
	h.sched.Stop()
	assert.Empty(t, target.storedBackupIDs())
}

// TestScheduler_ManualTrigger_Compat verifies manual runs work, cannot
// overlap a scheduled run, and do not create plan instances or schedule
// state.
func TestScheduler_ManualTrigger_Compat(t *testing.T) {
	t.Parallel()
	gate, release := newGate()
	blocking := &fakeTarget{name: "local", gate: gate}
	h := newLifecycleHarness(t, harnessConfig{enabled: true}, blocking)
	t.Cleanup(release)
	h.sched.Start()

	// Manual run while a scheduled run is active is rejected.
	scheduled := time.Now().Add(-30 * time.Second).Truncate(time.Second)
	h.loadDailyDue(scheduled)
	h.sched.checkSchedules(scheduled.Add(30 * time.Second))
	h.waitActive()
	err := h.sched.TriggerBackup(h.t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already in progress")

	release()
	h.waitForStatus(instanceIDFor(scheduled), StatusSuccess)

	// The lease is released only once post-success validation, stats and
	// cleanup also finish; wait for idle before the manual trigger.
	require.Eventually(t, func() bool {
		return h.sched.Status().Active == nil
	}, 2*time.Second, 5*time.Millisecond)

	// A subsequent manual trigger runs standalone through the same manager
	// and target, without creating another plan instance.
	require.NoError(t, h.sched.TriggerBackup(h.t.Context()))
	instances := h.state.GetInstances()
	for _, inst := range instances {
		assert.NotEqual(t, Trigger("manual"), inst.Trigger)
	}
}

// TestScheduler_ManualTrigger_WhenStopped works without a started scheduler
// (historical direct-trigger behavior).
func TestScheduler_ManualTrigger_WhenStopped(t *testing.T) {
	t.Parallel()
	target := &fakeTarget{name: "local"}
	h := newLifecycleHarness(t, harnessConfig{enabled: true}, target)

	require.NoError(t, h.sched.TriggerBackup(t.Context()))
	assert.Len(t, target.storedBackupIDs(), 1)
	assert.Empty(t, h.state.GetInstances())
}

// TestScheduler_StopWaitsForManualRun ensures an application shutdown also
// converges a manual trigger.
func TestScheduler_StopWaitsForManualRun(t *testing.T) {
	t.Parallel()
	gate, release := newGate()
	target := &fakeTarget{name: "local", gate: gate, entered: make(chan struct{})}
	h := newLifecycleHarness(t, harnessConfig{enabled: true}, target)
	t.Cleanup(release)
	h.sched.Start()

	go func() { _ = h.sched.TriggerBackup(t.Context()) }()
	h.waitActive()
	select {
	case <-target.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("target Store never entered")
	}

	done := make(chan struct{})
	go func() { h.sched.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Stop did not wait for the in-flight manual backup")
	}
	assert.GreaterOrEqual(t, int(target.ctxCanceled.Load()), 1)
}

// TestCalculateNextRun_DailyAndWeekly pins the calendar behavior that must
// stay compatible while the lifecycle internals change.
func TestCalculateNextRun_DailyAndWeekly(t *testing.T) {
	t.Parallel()
	s := &Scheduler{}

	loc := time.UTC
	now := time.Date(2026, 10, 8, 12, 0, 0, 0, loc) // Thursday

	// Daily at 02:00 later today? 12:00 is past 02:00 -> tomorrow.
	got := s.calculateNextRun(now, 2, 0, -1, false)
	assert.Equal(t, time.Date(2026, 10, 9, 2, 0, 0, 0, loc), got)

	// Daily at 23:30 today.
	got = s.calculateNextRun(now, 23, 30, -1, false)
	assert.Equal(t, time.Date(2026, 10, 8, 23, 30, 0, 0, loc), got)

	// Weekly Monday 09:00: this week's Monday already passed -> next Monday.
	got = s.calculateNextRun(now, 9, 0, time.Monday, true)
	assert.Equal(t, time.Date(2026, 10, 12, 9, 0, 0, 0, loc), got)

	// Weekly Thursday 09:00 from Thursday noon already passed -> next week.
	got = s.calculateNextRun(now, 9, 0, time.Thursday, true)
	assert.Equal(t, time.Date(2026, 10, 15, 9, 0, 0, 0, loc), got)

	// Weekly Thursday 23:00 is still ahead today.
	got = s.calculateNextRun(now, 23, 0, time.Thursday, true)
	assert.Equal(t, time.Date(2026, 10, 8, 23, 0, 0, 0, loc), got)
}
