package alerting

import (
	"runtime/debug"
	"sync"
	"time"
)

// AlertEvent represents an event that can trigger alert rules.
type AlertEvent struct {
	ObjectType string
	EventName  string         // For event triggers (e.g., "stream.disconnected")
	MetricName string         // For metric triggers (e.g., "system.cpu_usage")
	Properties map[string]any // Event-specific properties for condition evaluation
	Timestamp  time.Time
}

// AlertEventHandler processes alert events.
type AlertEventHandler func(event *AlertEvent)

// Package-level singleton for the alert event bus.
var (
	globalAlertBus *AlertEventBus
	alertBusMu     sync.RWMutex
)

// SetGlobalBus sets the package-level alert event bus singleton.
// Called during initialization.
func SetGlobalBus(bus *AlertEventBus) {
	alertBusMu.Lock()
	defer alertBusMu.Unlock()
	globalAlertBus = bus
}

// GetGlobalBus returns the package-level alert event bus, or nil if not initialized.
func GetGlobalBus() *AlertEventBus {
	alertBusMu.RLock()
	defer alertBusMu.RUnlock()
	return globalAlertBus
}

// TryPublish publishes an event to the global alert bus if initialized.
// Returns false if the bus is not yet available.
func TryPublish(event *AlertEvent) bool {
	bus := GetGlobalBus()
	if bus == nil {
		return false
	}
	bus.Publish(event)
	return true
}

const (
	// eventBusBufferSize is the capacity of the async event channel.
	// Events are dropped if the buffer is full to avoid blocking callers.
	eventBusBufferSize = 1000
)

// AlertEventBus is an async pub/sub for alert events. Publish is non-blocking:
// events are sent to a buffered channel and processed by a worker goroutine,
// so callers (stream managers, monitors) are never blocked by DB writes or
// notification dispatch.
//
// Lifecycle semantics:
//   - Every event that Publish successfully enqueues before Stop is processed
//     to completion before Stop returns.
//   - Every event reaching Publish after Stop is explicitly rejected (never
//     buffered) and reported through telemetry.
//   - Stop is idempotent and concurrent-safe; all callers return only after
//     the worker goroutine has exited.
type AlertEventBus struct {
	handlers []AlertEventHandler
	mu       sync.RWMutex

	eventCh chan *AlertEvent

	// publishMu makes the "already stopped?" decision and the channel write
	// atomic. Publish holds RLock, so concurrent publishes never block each
	// other; Stop holds Lock, therefore it cannot close the stop boundary
	// while a publish that observed a live bus is still enqueuing.
	publishMu sync.RWMutex
	stopped   bool

	stopCh chan struct{}
	// doneCh is closed by the worker after it drains eventCh and exits.
	// Stop blocks on doneCh so callers observe true resource termination.
	doneCh   chan struct{}
	stopOnce sync.Once

	telemetry *AlertingTelemetry // nil-safe, reports engine health
}

// NewAlertEventBus creates a new alert event bus and starts its worker.
func NewAlertEventBus(at *AlertingTelemetry) *AlertEventBus {
	b := &AlertEventBus{
		handlers:  make([]AlertEventHandler, 0),
		eventCh:   make(chan *AlertEvent, eventBusBufferSize),
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
		telemetry: at,
	}
	go b.processLoop()
	return b
}

// Subscribe registers a handler for alert events.
func (b *AlertEventBus) Subscribe(handler AlertEventHandler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.handlers = append(b.handlers, handler)
}

// Publish enqueues an event for async processing. It is always non-blocking:
// if the buffer is full the event is dropped (and counted) to protect callers
// on hot paths. After Stop has been called the event is never enqueued; it is
// explicitly rejected and reported through telemetry instead.
func (b *AlertEventBus) Publish(event *AlertEvent) {
	if event == nil {
		return
	}

	b.publishMu.RLock()
	defer b.publishMu.RUnlock()

	if b.stopped {
		// Stop boundary already closed — explicit, observable rejection.
		b.telemetry.ReportEventRejectedAfterStop()
		return
	}

	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	select {
	case b.eventCh <- event:
	default:
		// Buffer full — drop event to avoid blocking callers.
		b.telemetry.ReportEventDropped()
	}
}

// Stop closes the bus. Events already accepted are drained and fully
// processed; events published afterwards are explicitly rejected. Stop is
// safe for concurrent and repeated use: every caller returns only after the
// worker goroutine has finished draining and exited.
func (b *AlertEventBus) Stop() {
	b.stopOnce.Do(func() {
		b.publishMu.Lock()
		b.stopped = true
		close(b.stopCh)
		b.publishMu.Unlock()
	})
	<-b.doneCh
}

// processLoop dispatches accepted events. Once stopCh closes, no new event
// can enter eventCh (Stop flips stopped under the same lock publishes use),
// so the drain loop observes exactly the set of accepted events and the
// worker exits with nothing left behind.
func (b *AlertEventBus) processLoop() {
	defer close(b.doneCh)

	for {
		select {
		case event := <-b.eventCh:
			b.dispatch(event)
		case <-b.stopCh:
			// Drain remaining accepted events before exiting.
			for {
				select {
				case event := <-b.eventCh:
					b.dispatch(event)
				default:
					return
				}
			}
		}
	}
}

func (b *AlertEventBus) dispatch(event *AlertEvent) {
	b.mu.RLock()
	handlers := make([]AlertEventHandler, len(b.handlers))
	copy(handlers, b.handlers)
	b.mu.RUnlock()

	for _, handler := range handlers {
		b.safeCall(handler, event)
	}
}

// safeCall invokes a handler with panic recovery so a panicking handler
// cannot kill the event bus goroutine.
func (b *AlertEventBus) safeCall(handler AlertEventHandler, event *AlertEvent) {
	defer func() {
		if r := recover(); r != nil {
			b.telemetry.ReportPanic(r, debug.Stack())
		}
	}()
	handler(event)
}
