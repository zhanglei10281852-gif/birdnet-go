package backup

import (
	"bytes"
	"context"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// writeTestFile writes data to path, creating parent directories.
func writeTestFile(path string, data []byte) error {
	return os.WriteFile(path, data, PermSecureFile)
}

// fakeStoreCall records one target Store invocation.
type fakeStoreCall struct {
	backupID string
	at       time.Time
}

// fakeSource is a controllable Source for lifecycle tests.
type fakeSource struct {
	name      string
	data      []byte
	mu        sync.Mutex
	callCount int
	gate      <-chan struct{}
	entered   chan struct{}
	once      sync.Once
}

func (s *fakeSource) Name() string { return s.name }

func (s *fakeSource) Backup(ctx context.Context) (io.ReadCloser, error) {
	s.mu.Lock()
	s.callCount++
	s.mu.Unlock()
	if s.entered != nil {
		s.once.Do(func() { close(s.entered) })
	}
	if s.gate != nil {
		select {
		case <-s.gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return seekableReadCloser{Reader: bytes.NewReader(s.data)}, nil
}

func (s *fakeSource) Validate() error { return nil }

// seekableReadCloser exposes Seek through to the manager so the tar archiver
// can set the correct entry size.
type seekableReadCloser struct {
	*bytes.Reader
}

func (seekableReadCloser) Close() error { return nil }

func (s *fakeSource) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.callCount
}

// fakeTarget is a controllable Target for lifecycle tests.
type fakeTarget struct {
	name string

	mu          sync.Mutex
	calls       []fakeStoreCall
	failStorage bool

	gate    <-chan struct{} // when set, Store blocks until gate closes or ctx cancels
	entered chan struct{}
	once    sync.Once

	ctxCanceled atomic.Int32
}

func (t *fakeTarget) Name() string { return t.name }

func (t *fakeTarget) Store(ctx context.Context, _ string, metadata *Metadata) error {
	t.mu.Lock()
	t.calls = append(t.calls, fakeStoreCall{backupID: metadata.ID, at: time.Now()})
	t.mu.Unlock()
	if t.entered != nil {
		t.once.Do(func() { close(t.entered) })
	}
	if t.gate != nil {
		select {
		case <-t.gate:
		case <-ctx.Done():
			t.ctxCanceled.Add(1)
			return ctx.Err()
		}
	}
	if t.failStorage {
		return errFakeTargetFailure
	}
	return nil
}

func (t *fakeTarget) List(_ context.Context) ([]BackupInfo, error) { return nil, nil }
func (t *fakeTarget) Delete(_ context.Context, _ string) error     { return nil }
func (t *fakeTarget) Validate() error                              { return nil }

func (t *fakeTarget) storeCalls() []fakeStoreCall {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]fakeStoreCall, len(t.calls))
	copy(out, t.calls)
	return out
}

// storedBackupIDs returns the distinct backup IDs this target was asked to
// store, in encounter order.
func (t *fakeTarget) storedBackupIDs() []string {
	calls := t.storeCalls()
	seen := make(map[string]struct{}, len(calls))
	ids := make([]string, 0, len(calls))
	for _, call := range calls {
		if _, ok := seen[call.backupID]; ok {
			continue
		}
		seen[call.backupID] = struct{}{}
		ids = append(ids, call.backupID)
	}
	return ids
}

// newGate returns a blocking channel plus an idempotent release function.
// Tests release explicitly and also register release as cleanup, so double
// closes never panic.
func newGate() (gate <-chan struct{}, release func()) {
	gateCh := make(chan struct{})
	var once sync.Once
	release = func() {
		once.Do(func() { close(gateCh) })
	}
	return gateCh, release
}
