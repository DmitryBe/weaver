package pipeline_test

import (
	"testing"
	"time"

	dslcache "github.com/dmitryBe/weaver/internal/dsl/cache"
	"github.com/dmitryBe/weaver/internal/dsl/expr"
	"github.com/dmitryBe/weaver/internal/dsl/input"
	"github.com/dmitryBe/weaver/internal/dsl/op"
	"github.com/dmitryBe/weaver/internal/dsl/pipeline"
	"github.com/dmitryBe/weaver/internal/dsl/resilient"
	"github.com/dmitryBe/weaver/internal/dsl/retrieve"
)

func TestSpecPreservesStageOrder(t *testing.T) {
	spec := pipeline.New("demo").
		Input(input.String("user_id")).
		Context("ctx", op.Drop("unused")).
		Retrieve("retrieve", retrieve.Qdrant("brands").TopK(25)).
		Candidates("rank", op.Take(10))

	if got, want := len(spec.Stages), 3; got != want {
		t.Fatalf("unexpected stage count: got %d want %d", got, want)
	}

	if got, want := spec.Stages[0].Name, "ctx"; got != want {
		t.Fatalf("unexpected first stage: got %q want %q", got, want)
	}

	if got, want := spec.Stages[1].Kind, pipeline.StageRetrieve; got != want {
		t.Fatalf("unexpected second stage kind: got %q want %q", got, want)
	}

	if got, want := spec.Stages[2].Name, "rank"; got != want {
		t.Fatalf("unexpected third stage: got %q want %q", got, want)
	}
}

func TestValidateRejectsMixedOpKinds(t *testing.T) {
	spec := pipeline.New("demo").
		Context(
			"ctx",
			op.Fetch("user/segment").Key(expr.Context("user_id")).Into("segment"),
			op.Drop("unused"),
		)

	err := pipeline.Validate(spec)
	if err == nil {
		t.Fatal("expected validation error for mixed op kinds")
	}
}

func TestSpecPreservesCacheConfig(t *testing.T) {
	spec := pipeline.New("demo").
		Input(input.String("user_id")).
		Cache(
			dslcache.Key(expr.Tuple(expr.Const("demo"), expr.Context("user_id"))).
				TTL(5*time.Minute),
		).
		Candidates("rank", op.Take(10))

	if spec.CacheSpec == nil {
		t.Fatal("expected cache spec")
	}
	if got, want := spec.CacheSpec.TTLValue, 5*time.Minute; got != want {
		t.Fatalf("unexpected cache ttl: got %s want %s", got, want)
	}
}

func TestValidateRejectsRetrieveWithoutMerge(t *testing.T) {
	spec := pipeline.New("demo").
		Retrieve(
			"retrieve",
			retrieve.Parallel(
				retrieve.Qdrant("brands").TopK(25),
			),
		)

	err := pipeline.Validate(spec)
	if err == nil {
		t.Fatal("expected validation error for missing merge")
	}
}

func TestSpecPreservesResilienceDefaults(t *testing.T) {
	spec := pipeline.New("demo").
		Resilience(resilient.Default(2*time.Second, 3)).
		Candidates("rank", op.Take(10))

	if got, want := len(spec.DefaultOptions), 1; got != want {
		t.Fatalf("unexpected default option count: got %d want %d", got, want)
	}

	cfg, ok := spec.DefaultOptions[0].Value.(resilient.Config)
	if !ok {
		t.Fatalf("expected resilient.Config, got %T", spec.DefaultOptions[0].Value)
	}
	if got, want := cfg.MaxLatency, 2*time.Second; got != want {
		t.Fatalf("unexpected max latency: got %s want %s", got, want)
	}
	if got, want := cfg.MaxRetries, 3; got != want {
		t.Fatalf("unexpected max retries: got %d want %d", got, want)
	}
}

func TestValidateRejectsInvalidResilienceConfig(t *testing.T) {
	spec := pipeline.New("demo").
		Resilience(resilient.Default(0, 1)).
		Candidates("rank", op.Take(10))

	err := pipeline.Validate(spec)
	if err == nil {
		t.Fatal("expected validation error for invalid resilience config")
	}
}
