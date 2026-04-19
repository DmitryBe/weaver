package metrics

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	collectormetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/proto"
)

func TestNormalizeConfigDefaultsAndAllowlist(t *testing.T) {
	cfg, err := NormalizeConfig(Config{
		Backend: " OTLP ",
		OTLP: OTLPConfig{
			Endpoint: "http://localhost:4318/v1/metrics",
		},
		Cardinality: CardinalityConfig{
			FeatureDimensionAllowlist: []string{" User/Segment ", "user/segment", "brand/rating"},
		},
	})
	if err != nil {
		t.Fatalf("NormalizeConfig failed: %v", err)
	}

	if got, want := cfg.Backend, BackendOTLP; got != want {
		t.Fatalf("unexpected backend: got %q want %q", got, want)
	}
	if got, want := cfg.OTLP.Protocol, "http/protobuf"; got != want {
		t.Fatalf("unexpected protocol: got %q want %q", got, want)
	}
	if got, want := cfg.OTLP.Temporality, "delta"; got != want {
		t.Fatalf("unexpected temporality: got %q want %q", got, want)
	}
	if got, want := len(cfg.Cardinality.FeatureDimensionAllowlist), 2; got != want {
		t.Fatalf("unexpected allowlist length: got %d want %d", got, want)
	}
}

func TestOTLPBundleExportsDeltaMetrics(t *testing.T) {
	var (
		gotAuth string
		gotReq  *collectormetricspb.ExportMetricsServiceRequest
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		req := &collectormetricspb.ExportMetricsServiceRequest{}
		if err := proto.Unmarshal(body, req); err != nil {
			t.Fatalf("unmarshal otlp payload: %v", err)
		}
		gotReq = req
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	bundle, err := New(context.Background(), Config{
		Backend:     BackendOTLP,
		ServiceName: "weaver-test",
		OTLP: OTLPConfig{
			Endpoint:       server.URL,
			Headers:        map[string]string{"Authorization": "Api-Token test-token"},
			Protocol:       "http/protobuf",
			Temporality:    "delta",
			ExportInterval: time.Hour,
		},
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer bundle.Shutdown(context.Background())

	bundle.Recorder.RecordRequest(context.Background(), RequestMetric{
		Pipeline: "test.pipeline",
		Status:   StatusOK,
		Duration: 75 * time.Millisecond,
	})

	if err := bundle.ForceFlush(context.Background()); err != nil {
		t.Fatalf("ForceFlush failed: %v", err)
	}

	if gotAuth != "Api-Token test-token" {
		t.Fatalf("unexpected Authorization header: got %q", gotAuth)
	}
	if gotReq == nil {
		t.Fatal("expected OTLP metrics request")
	}
	if len(gotReq.ResourceMetrics) == 0 {
		t.Fatal("expected resource metrics")
	}

	var (
		foundTotal    bool
		foundDuration bool
	)
	for _, resourceMetric := range gotReq.ResourceMetrics {
		for _, scopeMetric := range resourceMetric.ScopeMetrics {
			for _, metric := range scopeMetric.Metrics {
				switch metric.Name {
				case "weaver.request.total":
					foundTotal = true
					sum := metric.GetSum()
					if sum == nil || sum.AggregationTemporality != metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA {
						t.Fatalf("expected delta temporality for request total, got %#v", sum)
					}
				case "weaver.request.duration":
					foundDuration = true
					histogram := metric.GetHistogram()
					if histogram == nil || histogram.AggregationTemporality != metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA {
						t.Fatalf("expected delta temporality for request duration, got %#v", histogram)
					}
				}
			}
		}
	}

	if !foundTotal {
		t.Fatal("expected weaver.request.total in OTLP payload")
	}
	if !foundDuration {
		t.Fatal("expected weaver.request.duration in OTLP payload")
	}
}

func TestTestRecorderNormalizesResilienceMetric(t *testing.T) {
	recorder := NewTestRecorder(nil)
	recorder.RecordResilience(context.Background(), ResilienceMetric{
		Pipeline:  " Test.Pipeline ",
		StageName: " Cand_Score ",
		NodeKind:  " Candidates_Model ",
		Status:    "ERROR",
		Reason:    " Timeout ",
		Attempts:  2,
	})

	if got, want := len(recorder.Resilience), 1; got != want {
		t.Fatalf("unexpected resilience metric count: got %d want %d", got, want)
	}

	metric := recorder.Resilience[0]
	if got, want := metric.Pipeline, "test.pipeline"; got != want {
		t.Fatalf("unexpected pipeline: got %q want %q", got, want)
	}
	if got, want := metric.StageName, "cand_score"; got != want {
		t.Fatalf("unexpected stage name: got %q want %q", got, want)
	}
	if got, want := metric.NodeKind, "candidates_model"; got != want {
		t.Fatalf("unexpected node kind: got %q want %q", got, want)
	}
	if got, want := metric.Status, StatusError; got != want {
		t.Fatalf("unexpected status: got %q want %q", got, want)
	}
	if got, want := metric.Reason, ReasonTimeout; got != want {
		t.Fatalf("unexpected reason: got %q want %q", got, want)
	}
	if got, want := metric.Attempts, 2; got != want {
		t.Fatalf("unexpected attempts: got %d want %d", got, want)
	}
}
