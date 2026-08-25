package process_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"alloy/internal/component/loki/process"
)

type mockStage struct {
	name       string
	processFunc func(ctx context.Context, entry *process.Entry) (*process.Entry, error)
}

func (m *mockStage) Name() string {
	return m.name
}

func (m *mockStage) Process(ctx context.Context, entry *process.Entry) (*process.Entry, error) {
	if m.processFunc != nil {
		return m.processFunc(ctx, entry)
	}
	return entry, nil
}

type mockLogger struct {
	mu   sync.Mutex
	logs []string
}

func (l *mockLogger) Error(msg string, keyvals ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.logs = append(l.logs, msg)
}
func (l *mockLogger) Warn(msg string, keyvals ...any)  {}
func (l *mockLogger) Info(msg string, keyvals ...any)  {}

var errInvalidJSON = errors.New("invalid json payload")
var errRegexMismatch = errors.New("regex pattern mismatch")

func TestPipeline_PreservesUpstreamErrorOnDownstreamFailure(t *testing.T) {
	stage1 := &mockStage{
		name: "json_parser",
		processFunc: func(ctx context.Context, entry *process.Entry) (*process.Entry, error) {
			return entry, errInvalidJSON
		},
	}

	ch := make(chan process.Entry, 1)
	rcvr := process.NewChannelReceiver(ch)
	rcvr.Close() // Downstream is closed

	p := process.NewPipeline(process.PipelineConfig{
		Stages:         []process.Stage{stage1},
		Receiver:       rcvr,
		ForwardOnError: true,
	})

	entry := process.Entry{Line: "{invalid-json", Timestamp: time.Now()}
	_, err := p.Process(context.Background(), entry)

	if err == nil {
		t.Fatalf("expected error, got nil")
	}

	// Check that root cause from upstream stage is preserved via errors.Is
	if !errors.Is(err, errInvalidJSON) {
		t.Errorf("expected error to wrap errInvalidJSON, got: %v", err)
	}

	// Check that downstream error is also present
	if !errors.Is(err, process.ErrDownstreamClosed) {
		t.Errorf("expected error to wrap ErrDownstreamClosed, got: %v", err)
	}

	// Check StageError attribution
	var stageErr *process.StageError
	if !errors.As(err, &stageErr) {
		t.Fatalf("expected error to contain *StageError")
	}
	if stageErr.StageName != "json_parser" || stageErr.StageIndex != 0 {
		t.Errorf("unexpected stage attribution: name=%s, index=%d", stageErr.StageName, stageErr.StageIndex)
	}

	// Verify metrics
	if p.Metrics().GetStageFailures("json_parser") != 1 {
		t.Errorf("expected 1 failure for json_parser, got %d", p.Metrics().GetStageFailures("json_parser"))
	}
}

func TestPipeline_AggregatesMultipleStageFailures(t *testing.T) {
	stage1 := &mockStage{
		name: "json",
		processFunc: func(ctx context.Context, entry *process.Entry) (*process.Entry, error) {
			return entry, errInvalidJSON
		},
	}
	stage2 := &mockStage{
		name: "regex",
		processFunc: func(ctx context.Context, entry *process.Entry) (*process.Entry, error) {
			return entry, errRegexMismatch
		},
	}

	p := process.NewPipeline(process.PipelineConfig{
		Stages: []process.Stage{stage1, stage2},
	})

	entry := process.Entry{Line: "bad-entry", Timestamp: time.Now()}
	_, err := p.Process(context.Background(), entry)

	if err == nil {
		t.Fatalf("expected aggregated error, got nil")
	}

	if !errors.Is(err, errInvalidJSON) {
		t.Errorf("expected error to wrap errInvalidJSON")
	}
	if !errors.Is(err, errRegexMismatch) {
		t.Errorf("expected error to wrap errRegexMismatch")
	}

	if p.Metrics().GetStageFailures("json") != 1 {
		t.Errorf("expected 1 failure for json stage")
	}
	if p.Metrics().GetStageFailures("regex") != 1 {
		t.Errorf("expected 1 failure for regex stage")
	}
}

func TestPipeline_DroppedLineMetrics(t *testing.T) {
	dropStage := &mockStage{
		name: "filter",
		processFunc: func(ctx context.Context, entry *process.Entry) (*process.Entry, error) {
			return nil, errors.New("dropped by filter")
		},
	}

	p := process.NewPipeline(process.PipelineConfig{
		Stages: []process.Stage{dropStage},
	})

	_, err := p.Process(context.Background(), process.Entry{Line: "to-drop"})
	if err == nil {
		t.Fatalf("expected error for dropped line")
	}

	if p.Metrics().GetLinesDropped("filter") != 1 {
		t.Errorf("expected 1 dropped line for filter stage, got %d", p.Metrics().GetLinesDropped("filter"))
	}
}
