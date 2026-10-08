package alerting

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tphakala/birdnet-go/internal/testutil"
)

const (
	lifecycleEventCount     = 100
	bufferOverflowCount    = 50
	racePublisherCount     = 20
	raceEventsPerPublisher = 10
	parallelStopCount      = 16
	slowHandlerExtraEvents = 25
	publishReturnTimeout   = time.Second
	stopConvergeTimeout    = 5 * time.Second
)

func TestAlertEventBus_SubscribeAndPublish(t *testing.T) {
	bus := NewAlertEventBus(nil)
	defer bus.Stop()

	var received atomic.Pointer[AlertEvent]

	bus.Subscribe(func(event *AlertEvent) {
		received.Store(event)
	})

	event := &AlertEvent{
		ObjectType: ObjectTypeStream,
		EventName:  EventStreamDisconnected,
		Properties: map[string]any{"stream_name": "backyard-cam"},
		Timestamp:  time.Now(),
	}

	bus.Publish(event)

	require.Eventually(t, func() bool { return received.Load() != nil }, time.Second, 5*time.Millisecond)
	got := received.Load()
	assert.Equal(t, ObjectTypeStream, got.ObjectType)
	assert.Equal(t, EventStreamDisconnected, got.EventName)
	assert.Equal(t, "backyard-cam", got.Properties["stream_name"])
}

func TestAlertEventBus_MultipleHandlers(t *testing.T) {
	bus := NewAlertEventBus(nil)
	defer bus.Stop()

	var count atomic.Int32

	for range 3 {
		bus.Subscribe(func(_ *AlertEvent) {
			count.Add(1)
		})
	}

	bus.Publish(&AlertEvent{ObjectType: ObjectTypeDetection, EventName: EventDetectionNewSpecies})

	assert.Eventually(t, func() bool { return count.Load() == 3 }, time.Second, 5*time.Millisecond)
}

func TestAlertEventBus_PublishWithNoHandlers(t *testing.T) {
	bus := NewAlertEventBus(nil)
	defer bus.Stop()
	// Should not panic
	bus.Publish(&AlertEvent{ObjectType: ObjectTypeSystem, MetricName: MetricCPUUsage})
}

func TestAlertEventBus_PublishSetsTimestamp(t *testing.T) {
	bus := NewAlertEventBus(nil)
	defer bus.Stop()

	var received atomic.Pointer[AlertEvent]

	bus.Subscribe(func(event *AlertEvent) {
		received.Store(event)
	})

	before := time.Now()
	bus.Publish(&AlertEvent{ObjectType: ObjectTypeDetection})

	require.Eventually(t, func() bool { return received.Load() != nil }, time.Second, 5*time.Millisecond)
	got := received.Load()
	assert.False(t, got.Timestamp.IsZero())
	assert.False(t, got.Timestamp.Before(before))
}

func TestAlertEventBus_ConcurrentPublish(t *testing.T) {
	bus := NewAlertEventBus(nil)
	defer bus.Stop()

	var count atomic.Int32

	bus.Subscribe(func(_ *AlertEvent) {
		count.Add(1)
	})

	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			bus.Publish(&AlertEvent{ObjectType: ObjectTypeDetection})
		})
	}
	wg.Wait()

	assert.Eventually(t, func() bool { return count.Load() == 100 }, time.Second, 5*time.Millisecond)
}

// TestAlertEventBus_DrainAcceptedEventsOnStop verifies the normal ordering
// contract: every event successfully enqueued before Stop is fully processed
// before Stop returns.
func TestAlertEventBus_DrainAcceptedEventsOnStop(t *testing.T) {
	at := NewAlertingTelemetry()
	bus := NewAlertEventBus(at)

	var processed atomic.Int32
	bus.Subscribe(func(*AlertEvent) {
		processed.Add(1)
	})

	for range lifecycleEventCount {
		bus.Publish(&AlertEvent{ObjectType: ObjectTypeSystem, MetricName: MetricCPUUsage})
	}

	bus.Stop()

	assert.Equal(t, int32(lifecycleEventCount), processed.Load(),
		"all events accepted before Stop must be processed before Stop returns")
	assert.Zero(t, at.RejectedAfterStopCount())
}

// TestAlertEventBus_RejectEventsAfterStop verifies events arriving after the
// stop boundary are never buffered or delivered, and are explicitly counted.
func TestAlertEventBus_RejectEventsAfterStop(t *testing.T) {
	at := NewAlertingTelemetry()
	bus := NewAlertEventBus(at)

	var processed atomic.Int32
	bus.Subscribe(func(*AlertEvent) {
		processed.Add(1)
	})

	bus.Stop()

	for range bufferOverflowCount {
		bus.Publish(&AlertEvent{ObjectType: ObjectTypeSystem, MetricName: MetricCPUUsage})
	}

	assert.Zero(t, processed.Load(), "no event after Stop must reach handlers")
	assert.Equal(t, int64(bufferOverflowCount), at.RejectedAfterStopCount(),
		"post-Stop events must be counted in telemetry")
	assert.Zero(t, at.DroppedEventCount(), "post-Stop rejection must not be mislabelled as buffer-full drop")

	// Stop again after rejection remains idempotent and non-panicking.
	bus.Stop()
}

// TestAlertEventBus_BufferFullDropsAndStaysNonBlocking preserves the
// buffer-full compatibility behavior: overflow is dropped and counted while
// Publish never blocks; Stop then drains all accepted events. The worker is
// parked in a slow handler so the queue deterministically reaches capacity.
func TestAlertEventBus_BufferFullDropsAndStaysNonBlocking(t *testing.T) {
	at := NewAlertingTelemetry()
	bus := NewAlertEventBus(at)

	release := make(chan struct{})
	handlerEntered := make(chan struct{}, 1)
	var processed atomic.Int32
	bus.Subscribe(func(*AlertEvent) {
		if processed.Load() == 0 {
			handlerEntered <- struct{}{}
			<-release
		}
		processed.Add(1)
	})

	// Park the worker inside the handler on the first accepted event.
	bus.Publish(&AlertEvent{ObjectType: ObjectTypeSystem, MetricName: MetricCPUUsage})
	select {
	case <-handlerEntered:
	case <-time.After(stopConvergeTimeout):
		t.Fatal("worker never parked inside the handler")
	}

	// While parked (the in-flight event is no longer buffered), fill the
	// full buffer and then overflow it.
	for range eventBusBufferSize {
		bus.Publish(&AlertEvent{ObjectType: ObjectTypeSystem, MetricName: MetricCPUUsage})
	}
	for range bufferOverflowCount {
		bus.Publish(&AlertEvent{ObjectType: ObjectTypeSystem, MetricName: MetricCPUUsage})
	}

	assert.Equal(t, int64(bufferOverflowCount), at.DroppedEventCount(),
		"buffer-full drops must keep the existing drop telemetry")
	assert.Zero(t, at.RejectedAfterStopCount(),
		"buffer-full drops before Stop must not be counted as post-Stop rejections")

	close(release)
	bus.Stop()

	// 1 in-flight event (parked worker) plus a full buffer drains.
	assert.Equal(t, int32(eventBusBufferSize+1), processed.Load(),
		"every accepted event must drain, overflow events stay dropped")
}

// TestAlertEventBus_PanickingHandlerDoesNotKillBus verifies a panicking
// handler is recovered per event, the panic is observable through telemetry,
// the worker keeps processing subsequent events, and every handler still
// sees every accepted event.
func TestAlertEventBus_PanickingHandlerDoesNotKillBus(t *testing.T) {
	at := NewAlertingTelemetry()
	bus := NewAlertEventBus(at)

	var firstHandlerSeen atomic.Int32
	bus.Subscribe(func(event *AlertEvent) {
		if event.Properties["panic"] != nil {
			panic("rule handler exploded")
		}
		firstHandlerSeen.Add(1)
	})

	var secondHandlerSeen atomic.Int32
	bus.Subscribe(func(*AlertEvent) {
		secondHandlerSeen.Add(1)
	})

	// Event 1 panics in the first handler; event 2 is delivered afterwards.
	bus.Publish(&AlertEvent{
		ObjectType: ObjectTypeSystem,
		EventName:  EventStreamError,
		Properties: map[string]any{"panic": true},
	})
	bus.Publish(&AlertEvent{
		ObjectType: ObjectTypeSystem,
		EventName:  EventStreamError,
		Properties: map[string]any{},
	})

	bus.Stop()

	assert.Equal(t, int64(1), at.PanicCount(), "the recovered panic must be counted exactly once")
	assert.Equal(t, int32(1), firstHandlerSeen.Load(),
		"the panicking handler must still be invoked for later events")
	assert.Equal(t, int32(2), secondHandlerSeen.Load(),
		"a panic in one handler must not block other handlers or later events")
}

// TestAlertEventBus_StopPublishRaceConverges exercises Stop interleaving with
// concurrent Publish: nothing may disappear into the Stop boundary. Every
// published event is either fully processed or explicitly rejected.
func TestAlertEventBus_StopPublishRaceConverges(t *testing.T) {
	at := NewAlertingTelemetry()
	bus := NewAlertEventBus(at)

	var processed atomic.Int32
	bus.Subscribe(func(*AlertEvent) {
		processed.Add(1)
	})

	start := make(chan struct{})
	var publishers sync.WaitGroup
	for range racePublisherCount {
		publishers.Go(func() {
			<-start
			for range raceEventsPerPublisher {
				bus.Publish(&AlertEvent{ObjectType: ObjectTypeDetection, EventName: EventDetectionOccurred})
			}
		})
	}

	close(start)
	bus.Stop()
	publishers.Wait()

	total := int64(racePublisherCount * raceEventsPerPublisher)
	assert.Equal(t, total, int64(processed.Load())+at.RejectedAfterStopCount(),
		"every event under Stop/Publish interleaving must be processed or explicitly rejected")
	assert.Zero(t, at.DroppedEventCount(), "small event volume must not overflow the buffer")
}

// TestAlertEventBus_ConcurrentRepeatedStopIdempotent verifies many concurrent
// and sequential Stop calls converge on the same shutdown without panicking,
// and accepted events are still drained.
func TestAlertEventBus_ConcurrentRepeatedStopIdempotent(t *testing.T) {
	bus := NewAlertEventBus(nil)

	var processed atomic.Int32
	bus.Subscribe(func(*AlertEvent) {
		processed.Add(1)
	})

	for range lifecycleEventCount {
		bus.Publish(&AlertEvent{ObjectType: ObjectTypeSystem, MetricName: MetricCPUUsage})
	}

	start := make(chan struct{})
	var stoppers sync.WaitGroup
	for range parallelStopCount {
		stoppers.Go(func() {
			<-start
			bus.Stop()
		})
	}

	close(start)
	stoppers.Wait()

	// Sequential repeated Stops after concurrent convergence.
	bus.Stop()
	bus.Stop()

	assert.Equal(t, int32(lifecycleEventCount), processed.Load())
}

// TestAlertEventBus_RestartIsolation verifies a new bus instance never
// observes a previous instance's events: after the old bus is stopped and
// rejected, the new bus independently processes only its own accepted events.
func TestAlertEventBus_RestartIsolation(t *testing.T) {
	oldTelemetry := NewAlertingTelemetry()
	oldBus := NewAlertEventBus(oldTelemetry)

	var oldSeen atomic.Int32
	oldBus.Subscribe(func(*AlertEvent) {
		oldSeen.Add(1)
	})

	oldBus.Publish(&AlertEvent{ObjectType: ObjectTypeSystem, MetricName: MetricCPUUsage})
	oldBus.Stop()
	// Residual publisher traffic against the old reference is rejected there.
	oldBus.Publish(&AlertEvent{ObjectType: ObjectTypeSystem, MetricName: MetricCPUUsage})

	newTelemetry := NewAlertingTelemetry()
	newBus := NewAlertEventBus(newTelemetry)

	var newSeen atomic.Int32
	newBus.Subscribe(func(*AlertEvent) {
		newSeen.Add(1)
	})

	newBus.Publish(&AlertEvent{ObjectType: ObjectTypeSystem, MetricName: MetricCPUUsage})
	newBus.Stop()

	assert.Equal(t, int32(1), oldSeen.Load(), "old bus must process only its pre-Stop event")
	assert.Equal(t, int64(1), oldTelemetry.RejectedAfterStopCount())
	assert.Equal(t, int32(1), newSeen.Load(), "new bus must not see residual events from the old instance")
	assert.Zero(t, newTelemetry.RejectedAfterStopCount())
	assert.Zero(t, newTelemetry.DroppedEventCount())
}

// TestAlertEventBus_GoroutineConvergence verifies Stop returns only after the
// worker goroutine truly exits (doneCh closed) and leaves no leaked
// goroutines.
func TestAlertEventBus_GoroutineConvergence(t *testing.T) {
	testutil.VerifyNoLeaks(t)

	at := NewAlertingTelemetry()
	bus := NewAlertEventBus(at)
	bus.Publish(&AlertEvent{ObjectType: ObjectTypeSystem, MetricName: MetricCPUUsage})

	bus.Stop()

	select {
	case <-bus.doneCh:
	default:
		t.Fatal("worker doneCh must be closed when Stop returns")
	}
}

// TestAlertEventBus_SlowHandlerDoesNotBlockPublishers verifies a handler
// blocked on slow work cannot stall Publish callers (non-blocking enqueue),
// while Stop still waits for every accepted event including the slow one.
func TestAlertEventBus_SlowHandlerDoesNotBlockPublishers(t *testing.T) {
	bus := NewAlertEventBus(nil)

	release := make(chan struct{})
	handlerEntered := make(chan struct{}, 1)
	var processed atomic.Int32
	bus.Subscribe(func(*AlertEvent) {
		if processed.Load() == 0 {
			handlerEntered <- struct{}{}
			<-release
		}
		processed.Add(1)
	})

	// The first accepted event parks the worker inside the handler.
	bus.Publish(&AlertEvent{ObjectType: ObjectTypeSystem, MetricName: MetricCPUUsage})
	select {
	case <-handlerEntered:
	case <-time.After(stopConvergeTimeout):
		t.Fatal("slow handler never started processing the first event")
	}

	// While the handler is blocked, every Publish must still return promptly.
	returned := make(chan struct{}, slowHandlerExtraEvents)
	for range slowHandlerExtraEvents {
		go func() {
			bus.Publish(&AlertEvent{ObjectType: ObjectTypeSystem, MetricName: MetricCPUUsage})
			returned <- struct{}{}
		}()
	}
	for range slowHandlerExtraEvents {
		select {
		case <-returned:
		case <-time.After(publishReturnTimeout):
			t.Fatal("Publish must stay non-blocking while a handler is slow")
		}
	}

	close(release)
	bus.Stop()

	assert.Equal(t, int32(slowHandlerExtraEvents+1), processed.Load(),
		"Stop must wait for the slow handler and all queued events to finish")
}
