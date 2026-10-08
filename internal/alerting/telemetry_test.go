package alerting

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAlertingTelemetry_NilSafe(t *testing.T) {
	t.Parallel()
	// All methods must be no-ops on nil receiver -- no panic.
	var at *AlertingTelemetry
	assert.NotPanics(t, func() { at.ReportInitFailed("seed rules failed") })
	assert.NotPanics(t, func() { at.ReportPanic("test panic", nil) })
	assert.NotPanics(t, func() { at.ReportEventDropped() })
	assert.NotPanics(t, func() { at.ReportEventRejectedAfterStop() })
	assert.NotPanics(t, func() { at.ReportDBWriteFailed("save_history", "connection refused") })
	assert.NotPanics(t, func() { at.ReportDispatchFailed("bell", "notification service unavailable") })
	assert.NotPanics(t, func() { at.ReportBridgeRegistrationFailed("detection alert bridge failed") })
	assert.NotPanics(t, func() { _ = at.RejectedAfterStopCount() })
	assert.NotPanics(t, func() { _ = at.PanicCount() })
	assert.NotPanics(t, func() { _ = at.DroppedEventCount() })
}

func TestNewAlertingTelemetry(t *testing.T) {
	t.Parallel()
	at := NewAlertingTelemetry()
	assert.NotNil(t, at)
}

func TestAlertingTelemetry_ReportEventDropped_Counter(t *testing.T) {
	t.Parallel()
	at := NewAlertingTelemetry()
	for i := int64(1); i <= 15; i++ {
		at.ReportEventDropped()
		assert.Equal(t, i, at.droppedEvents.Load())
	}
}

func TestAlertingTelemetry_ReportEventRejectedAfterStop_Counter(t *testing.T) {
	t.Parallel()
	at := NewAlertingTelemetry()
	for i := int64(1); i <= 10; i++ {
		at.ReportEventRejectedAfterStop()
		assert.Equal(t, i, at.RejectedAfterStopCount())
	}
	// Post-Stop rejection accounting stays separate from buffer-full drops.
	assert.Zero(t, at.DroppedEventCount())
}

func TestAlertingTelemetry_ReportPanic_Counter(t *testing.T) {
	t.Parallel()
	at := NewAlertingTelemetry()
	for i := int64(1); i <= 3; i++ {
		at.ReportPanic("test panic", nil)
		assert.Equal(t, i, at.PanicCount())
	}
}
