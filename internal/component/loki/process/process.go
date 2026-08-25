package process

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrDownstreamClosed  = errors.New("downstream receiver is closed")
	ErrDownstreamBlocked = errors.New("downstream receiver is blocked or timed out")
)

// Entry represents a log entry passing through the pipeline.
type Entry struct {
	Labels    map[string]string
	Line      string
	Timestamp time.Time
}

// Stage defines the interface for an individual processing stage.
type Stage interface {
	Name() string
	Process(ctx context.Context, entry *Entry) (*Entry, error)
}

// StageError wraps an error that occurred during a specific pipeline stage.
type StageError struct {
	StageIndex int
	StageName  string
	Err        error
}

func (e *StageError) Error() string {
	return fmt.Sprintf("stage %d (%s) failed: %v", e.StageIndex, e.StageName, e.Err)
}

func (e *StageError) Unwrap() error {
	return e.Err
}

// Receiver represents downstream consumer of processed log entries.
type Receiver interface {
	Send(ctx context.Context, entry Entry) error
}

// ChannelReceiver wraps a Go channel receiver with safe concurrency handling.
type ChannelReceiver struct {
	ch     chan Entry
	mu     sync.RWMutex
	closed bool
}

// NewChannelReceiver creates a new safe channel receiver.
func NewChannelReceiver(ch chan Entry) *ChannelReceiver {
	return &ChannelReceiver{ch: ch}
}

// Close safely marks the channel receiver as closed.
func (r *ChannelReceiver) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.closed {
		r.closed = true
		close(r.ch)
	}
}

// Send sends an entry downstream safely without panicking if closed.
func (r *ChannelReceiver) Send(ctx context.Context, entry Entry) (err error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.closed {
		return ErrDownstreamClosed
	}

	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("%w: %v", ErrDownstreamClosed, rec)
		}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case r.ch <- entry:
		return nil
	default:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case r.ch <- entry:
			return nil
		case <-time.After(100 * time.Millisecond):
			return ErrDownstreamBlocked
		}
	}
}

// Logger defines a logging interface for diagnostics.
type Logger interface {
	Error(msg string, keyvals ...any)
	Warn(msg string, keyvals ...any)
	Info(msg string, keyvals ...any)
}

type defaultLogger struct{}

func (l *defaultLogger) Error(msg string, keyvals ...any) {}
func (l *defaultLogger) Warn(msg string, keyvals ...any)  {}
func (l *defaultLogger) Info(msg string, keyvals ...any)  {}

// Metrics tracks pipeline and stage-level execution statistics.
type Metrics struct {
	mu             sync.RWMutex
	StageFailures  map[string]*uint64
	LinesDropped   map[string]*uint64
	TotalProcessed uint64
	TotalFailed    uint64
	DownstreamFail uint64
}

// NewMetrics initializes a new Metrics instance.
func NewMetrics() *Metrics {
	return &Metrics{
		StageFailures: make(map[string]*uint64),
		LinesDropped:  make(map[string]*uint64),
	}
}

func (m *Metrics) IncStageFailure(stageName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, exists := m.StageFailures[stageName]
	if !exists {
		var val uint64
		c = &val
		m.StageFailures[stageName] = c
	}
	atomic.AddUint64(c, 1)
	atomic.AddUint64(&m.TotalFailed, 1)
}

func (m *Metrics) IncLinesDropped(stageName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, exists := m.LinesDropped[stageName]
	if !exists {
		var val uint64
		c = &val
		m.LinesDropped[stageName] = c
	}
	atomic.AddUint64(c, 1)
}

func (m *Metrics) IncDownstreamFailure() {
	atomic.AddUint64(&m.DownstreamFail, 1)
}

func (m *Metrics) GetStageFailures(stageName string) uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if c, exists := m.StageFailures[stageName]; exists {
		return atomic.LoadUint64(c)
	}
	return 0
}

func (m *Metrics) GetLinesDropped(stageName string) uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if c, exists := m.LinesDropped[stageName]; exists {
		return atomic.LoadUint64(c)
	}
	return 0
}

// PipelineConfig holds the configuration for a pipeline processor.
type PipelineConfig struct {
	Stages         []Stage
	Receiver       Receiver
	Logger         Logger
	ForwardOnError bool
}

// Pipeline coordinates processing of log entries through stages to downstream receivers.
type Pipeline struct {
	stages         []Stage
	receiver       Receiver
	logger         Logger
	metrics        *Metrics
	forwardOnError bool
}

// NewPipeline constructs a new Pipeline.
func NewPipeline(cfg PipelineConfig) *Pipeline {
	logger := cfg.Logger
	if logger == nil {
		logger = &defaultLogger{}
	}
	return &Pipeline{
		stages:         cfg.Stages,
		receiver:       cfg.Receiver,
		logger:         logger,
		metrics:        NewMetrics(),
		forwardOnError: cfg.ForwardOnError,
	}
}

// Metrics returns the pipeline's metrics collector.
func (p *Pipeline) Metrics() *Metrics {
	return p.metrics
}

// Process processes a single entry through all configured stages and forwards downstream.
func (p *Pipeline) Process(ctx context.Context, entry Entry) (Entry, error) {
	atomic.AddUint64(&p.metrics.TotalProcessed, 1)
	var stageErrs []error
	current := &entry

	for idx, stage := range p.stages {
		next, err := stage.Process(ctx, current)
		if err != nil {
			stageErr := &StageError{
				StageIndex: idx,
				StageName:  stage.Name(),
				Err:        err,
			}
			stageErrs = append(stageErrs, stageErr)
			p.metrics.IncStageFailure(stage.Name())

			p.logger.Error("stage processing failed",
				"stage_index", idx,
				"stage_name", stage.Name(),
				"error", err,
			)

			if next == nil {
				p.metrics.IncLinesDropped(stage.Name())
				break
			}
		}
		if next != nil {
			current = next
		}
	}

	var downstreamErr error
	if p.receiver != nil && (len(stageErrs) == 0 || p.forwardOnError) && current != nil {
		if err := p.receiver.Send(ctx, *current); err != nil {
			downstreamErr = fmt.Errorf("downstream forward failed: %w", err)
			p.metrics.IncDownstreamFailure()
			p.logger.Error("downstream receiver failed to accept entry",
				"error", downstreamErr,
				"stage_errors_count", len(stageErrs),
			)
		}
	}

	if len(stageErrs) > 0 || downstreamErr != nil {
		allErrs := make([]error, 0, len(stageErrs)+1)
		allErrs = append(allErrs, stageErrs...)
		if downstreamErr != nil {
			allErrs = append(allErrs, downstreamErr)
		}

		combined := errors.Join(allErrs...)
		var res Entry
		if current != nil {
			res = *current
		}
		return res, combined
	}

	return *current, nil
}
