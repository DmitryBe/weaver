package metrics

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	promclient "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	otelmetric "go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
)

const (
	BackendNoop          = "noop"
	BackendOTLP          = "otlp"
	BackendPrometheus    = "prometheus"
	InstrumentationScope = "weaver"

	StatusOK    = "ok"
	StatusError = "error"

	ReasonMissing             = "missing"
	ReasonNull                = "null"
	ReasonTimeout             = "timeout"
	ReasonRetriesExhausted    = "retries_exhausted"
	ReasonRetriedSuccess      = "retried_success"
	ReasonFirstAttemptSuccess = "first_attempt_success"
	ReasonCanceled            = "canceled"

	ScopeContext    = "context"
	ScopeCandidates = "candidates"

	StageCategoryRetrieveCandidates = "retrieve_candidates"
	StageCategoryFetchCandidates    = "fetch_candidates"
	StageCategoryModelCall          = "model_call"
)

type Config struct {
	Backend     string            `yaml:"backend" json:"backend"`
	ServiceName string            `yaml:"service_name" json:"service_name"`
	OTLP        OTLPConfig        `yaml:"otlp" json:"otlp"`
	Prometheus  PrometheusConfig  `yaml:"prometheus" json:"prometheus"`
	Cardinality CardinalityConfig `yaml:"cardinality" json:"cardinality"`
}

type OTLPConfig struct {
	Endpoint       string            `yaml:"endpoint" json:"endpoint"`
	Headers        map[string]string `yaml:"headers" json:"headers"`
	Protocol       string            `yaml:"protocol" json:"protocol"`
	Temporality    string            `yaml:"temporality" json:"temporality"`
	Insecure       bool              `yaml:"insecure" json:"insecure"`
	ExportInterval time.Duration     `yaml:"export_interval" json:"export_interval"`
}

type PrometheusConfig struct {
	Path string `yaml:"path" json:"path"`
}

type CardinalityConfig struct {
	FeatureDimensionAllowlist []string `yaml:"feature_dimension_allowlist" json:"feature_dimension_allowlist"`
}

type Recorder interface {
	RecordRequest(ctx context.Context, metric RequestMetric)
	RecordNode(ctx context.Context, metric NodeMetric)
	RecordStage(ctx context.Context, metric StageMetric)
	RecordRetriever(ctx context.Context, metric RetrieverMetric)
	RecordFeatureFetch(ctx context.Context, metric FeatureFetchMetric)
	RecordFeatureDefault(ctx context.Context, metric FeatureDefaultMetric)
	RecordResilience(ctx context.Context, metric ResilienceMetric)
}

type Bundle struct {
	Recorder    Recorder
	Handler     http.Handler
	HandlerPath string

	forceFlush func(context.Context) error
	shutdown   func(context.Context) error
}

func (b *Bundle) ForceFlush(ctx context.Context) error {
	if b == nil || b.forceFlush == nil {
		return nil
	}
	return b.forceFlush(ctx)
}

func (b *Bundle) Shutdown(ctx context.Context) error {
	if b == nil || b.shutdown == nil {
		return nil
	}
	return b.shutdown(ctx)
}

type RequestMetric struct {
	Pipeline string
	Status   string
	Duration time.Duration
}

type NodeMetric struct {
	Pipeline  string
	StageName string
	NodeKind  string
	Status    string
	Duration  time.Duration
}

type StageMetric struct {
	Pipeline      string
	StageName     string
	StageCategory string
	Status        string
	Duration      time.Duration
}

type RetrieverMetric struct {
	Pipeline   string
	StageName  string
	SourceType string
	Status     string
	Duration   time.Duration
	Candidates int
}

type FeatureFetchMetric struct {
	Pipeline  string
	StageName string
	Scope     string
	Entity    string
	Status    string
	Duration  time.Duration
}

type FeatureDefaultMetric struct {
	Pipeline  string
	StageName string
	Scope     string
	Entity    string
	Feature   string
	Reason    string
}

type ResilienceMetric struct {
	Pipeline  string
	StageName string
	NodeKind  string
	Status    string
	Reason    string
	Attempts  int
}

type noopRecorder struct{}

type otelRecorder struct {
	requestTotal             otelmetric.Int64Counter
	requestDuration          otelmetric.Float64Histogram
	nodeTotal                otelmetric.Int64Counter
	nodeDuration             otelmetric.Float64Histogram
	stageDuration            otelmetric.Float64Histogram
	retrieverDuration        otelmetric.Float64Histogram
	retrieverCandidates      otelmetric.Int64Histogram
	retrieverEmptyTotal      otelmetric.Int64Counter
	featureFetchDuration     otelmetric.Float64Histogram
	featureFetchDefault      otelmetric.Int64Counter
	featureFetchByFeature    otelmetric.Int64Counter
	resilienceTotal          otelmetric.Int64Counter
	allowedFeatureDimensions map[string]struct{}
}

type TestRecorder struct {
	mu              sync.Mutex
	Requests        []RequestMetric
	Nodes           []NodeMetric
	Stages          []StageMetric
	Retrievers      []RetrieverMetric
	FeatureFetches  []FeatureFetchMetric
	FeatureDefaults []FeatureDefaultMetric
	Resilience      []ResilienceMetric
	allowedFeatures map[string]struct{}
}

func New(ctx context.Context, cfg Config) (*Bundle, error) {
	normalized, err := NormalizeConfig(cfg)
	if err != nil {
		return nil, err
	}

	switch normalized.Backend {
	case BackendNoop:
		return &Bundle{Recorder: Noop(), HandlerPath: normalized.Prometheus.Path}, nil
	case BackendOTLP:
		return newOTLPBundle(ctx, normalized)
	case BackendPrometheus:
		return newPrometheusBundle(normalized)
	default:
		return nil, fmt.Errorf("unsupported metrics backend %q", normalized.Backend)
	}
}

func NormalizeConfig(cfg Config) (Config, error) {
	cfg.Backend = normalizeBackend(cfg.Backend)
	if cfg.Backend == "" {
		cfg.Backend = BackendNoop
	}
	if cfg.ServiceName == "" {
		cfg.ServiceName = "weaver"
	}
	if cfg.Prometheus.Path == "" {
		cfg.Prometheus.Path = "/metrics"
	}
	if cfg.OTLP.Protocol == "" {
		cfg.OTLP.Protocol = "http/protobuf"
	}
	if cfg.OTLP.Temporality == "" {
		cfg.OTLP.Temporality = "delta"
	}
	if cfg.OTLP.ExportInterval <= 0 {
		cfg.OTLP.ExportInterval = 10 * time.Second
	}
	cfg.Cardinality.FeatureDimensionAllowlist = normalizeAllowlist(cfg.Cardinality.FeatureDimensionAllowlist)

	switch cfg.Backend {
	case BackendNoop, BackendPrometheus:
		return cfg, nil
	case BackendOTLP:
		if strings.TrimSpace(cfg.OTLP.Endpoint) == "" {
			return Config{}, fmt.Errorf("metrics.otlp.endpoint must not be empty when backend=otlp")
		}
		if cfg.OTLP.Protocol != "http/protobuf" {
			return Config{}, fmt.Errorf("metrics.otlp.protocol must be %q, got %q", "http/protobuf", cfg.OTLP.Protocol)
		}
		if cfg.OTLP.Temporality != "delta" {
			return Config{}, fmt.Errorf("metrics.otlp.temporality must be %q, got %q", "delta", cfg.OTLP.Temporality)
		}
		return cfg, nil
	default:
		return Config{}, fmt.Errorf("unsupported metrics backend %q", cfg.Backend)
	}
}

func Noop() Recorder {
	return noopRecorder{}
}

func NewTestRecorder(allowlist []string) *TestRecorder {
	return &TestRecorder{allowedFeatures: allowlistSet(allowlist)}
}

func StageCategoryForNodeKind(kind string) string {
	switch normalizeValue(kind) {
	case "retrieve_source", "retrieve_merge":
		return StageCategoryRetrieveCandidates
	case "candidates_fetch":
		return StageCategoryFetchCandidates
	case "context_model", "candidates_model":
		return StageCategoryModelCall
	default:
		return ""
	}
}

func normalizeBackend(value string) string {
	switch normalizeValue(value) {
	case "", BackendNoop:
		return BackendNoop
	case BackendOTLP:
		return BackendOTLP
	case BackendPrometheus:
		return BackendPrometheus
	default:
		return normalizeValue(value)
	}
}

func normalizeAllowlist(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		normalized := normalizeValue(value)
		if normalized == "" {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	sort.Strings(out)
	return out
}

func allowlistSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range normalizeAllowlist(values) {
		out[value] = struct{}{}
	}
	return out
}

func normalizeValue(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(value))
	lastUnderscore := false
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
			lastUnderscore = false
		case r >= '0' && r <= '9':
			b.WriteRune(r)
			lastUnderscore = false
		case r == '.' || r == '/' || r == '-':
			b.WriteRune(r)
			lastUnderscore = false
		default:
			if !lastUnderscore {
				b.WriteByte('_')
				lastUnderscore = true
			}
		}
	}
	return strings.Trim(b.String(), "_")
}

func normalizeStatus(value string) string {
	if normalizeValue(value) == StatusError {
		return StatusError
	}
	return StatusOK
}

func durationMillis(value time.Duration) float64 {
	return float64(value) / float64(time.Millisecond)
}

func attrs(kv ...string) []attribute.KeyValue {
	out := make([]attribute.KeyValue, 0, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		out = append(out, attribute.String(kv[i], normalizeValue(kv[i+1])))
	}
	return out
}

func newMeterProviderResource(serviceName string) *resource.Resource {
	return resource.NewWithAttributes("", semconv.ServiceNameKey.String(serviceName))
}

func newPrometheusBundle(cfg Config) (*Bundle, error) {
	registry := promclient.NewRegistry()
	exporter, err := otelprom.New(
		otelprom.WithRegisterer(registry),
		otelprom.WithoutScopeInfo(),
		otelprom.WithoutTargetInfo(),
	)
	if err != nil {
		return nil, err
	}

	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(newMeterProviderResource(cfg.ServiceName)),
		sdkmetric.WithReader(exporter),
	)

	recorder, err := newOTelRecorder(provider, cfg.Cardinality.FeatureDimensionAllowlist)
	if err != nil {
		return nil, err
	}

	return &Bundle{
		Recorder:    recorder,
		Handler:     promhttp.HandlerFor(registry, promhttp.HandlerOpts{}),
		HandlerPath: cfg.Prometheus.Path,
		forceFlush:  provider.ForceFlush,
		shutdown:    provider.Shutdown,
	}, nil
}

func newOTLPBundle(ctx context.Context, cfg Config) (*Bundle, error) {
	options := []otlpmetrichttp.Option{
		otlpmetrichttp.WithEndpointURL(cfg.OTLP.Endpoint),
		otlpmetrichttp.WithTemporalitySelector(sdkmetric.DeltaTemporalitySelector),
		otlpmetrichttp.WithHeaders(cfg.OTLP.Headers),
	}
	if cfg.OTLP.Insecure || strings.HasPrefix(strings.ToLower(cfg.OTLP.Endpoint), "http://") {
		options = append(options, otlpmetrichttp.WithInsecure())
	}

	exporter, err := otlpmetrichttp.New(ctx, options...)
	if err != nil {
		return nil, err
	}

	reader := sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(cfg.OTLP.ExportInterval))
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(newMeterProviderResource(cfg.ServiceName)),
		sdkmetric.WithReader(reader),
	)

	recorder, err := newOTelRecorder(provider, cfg.Cardinality.FeatureDimensionAllowlist)
	if err != nil {
		return nil, err
	}

	return &Bundle{
		Recorder:    recorder,
		HandlerPath: cfg.Prometheus.Path,
		forceFlush:  provider.ForceFlush,
		shutdown:    provider.Shutdown,
	}, nil
}

func newOTelRecorder(provider *sdkmetric.MeterProvider, allowlist []string) (*otelRecorder, error) {
	meter := provider.Meter(InstrumentationScope)

	requestTotal, err := meter.Int64Counter("weaver.request.total")
	if err != nil {
		return nil, err
	}
	requestDuration, err := meter.Float64Histogram("weaver.request.duration", otelmetric.WithUnit("ms"))
	if err != nil {
		return nil, err
	}
	nodeTotal, err := meter.Int64Counter("weaver.node.total")
	if err != nil {
		return nil, err
	}
	nodeDuration, err := meter.Float64Histogram("weaver.node.duration", otelmetric.WithUnit("ms"))
	if err != nil {
		return nil, err
	}
	stageDuration, err := meter.Float64Histogram("weaver.stage.duration", otelmetric.WithUnit("ms"))
	if err != nil {
		return nil, err
	}
	retrieverDuration, err := meter.Float64Histogram("weaver.retriever.duration", otelmetric.WithUnit("ms"))
	if err != nil {
		return nil, err
	}
	retrieverCandidates, err := meter.Int64Histogram("weaver.retriever.candidates")
	if err != nil {
		return nil, err
	}
	retrieverEmptyTotal, err := meter.Int64Counter("weaver.retriever.empty_total")
	if err != nil {
		return nil, err
	}
	featureFetchDuration, err := meter.Float64Histogram("weaver.feature_fetch.duration", otelmetric.WithUnit("ms"))
	if err != nil {
		return nil, err
	}
	featureFetchDefault, err := meter.Int64Counter("weaver.feature_fetch.default_total")
	if err != nil {
		return nil, err
	}
	featureFetchByFeature, err := meter.Int64Counter("weaver.feature_fetch.default_by_feature_total")
	if err != nil {
		return nil, err
	}
	resilienceTotal, err := meter.Int64Counter("weaver.resilience.total")
	if err != nil {
		return nil, err
	}

	return &otelRecorder{
		requestTotal:             requestTotal,
		requestDuration:          requestDuration,
		nodeTotal:                nodeTotal,
		nodeDuration:             nodeDuration,
		stageDuration:            stageDuration,
		retrieverDuration:        retrieverDuration,
		retrieverCandidates:      retrieverCandidates,
		retrieverEmptyTotal:      retrieverEmptyTotal,
		featureFetchDuration:     featureFetchDuration,
		featureFetchDefault:      featureFetchDefault,
		featureFetchByFeature:    featureFetchByFeature,
		resilienceTotal:          resilienceTotal,
		allowedFeatureDimensions: allowlistSet(allowlist),
	}, nil
}

func (noopRecorder) RecordRequest(context.Context, RequestMetric)               {}
func (noopRecorder) RecordNode(context.Context, NodeMetric)                     {}
func (noopRecorder) RecordStage(context.Context, StageMetric)                   {}
func (noopRecorder) RecordRetriever(context.Context, RetrieverMetric)           {}
func (noopRecorder) RecordFeatureFetch(context.Context, FeatureFetchMetric)     {}
func (noopRecorder) RecordFeatureDefault(context.Context, FeatureDefaultMetric) {}
func (noopRecorder) RecordResilience(context.Context, ResilienceMetric)         {}

func (r *otelRecorder) RecordRequest(ctx context.Context, metric RequestMetric) {
	options := otelmetric.WithAttributes(attrs(
		"pipeline", metric.Pipeline,
		"status", normalizeStatus(metric.Status),
	)...)
	r.requestTotal.Add(ctx, 1, options)
	r.requestDuration.Record(ctx, durationMillis(metric.Duration), options)
}

func (r *otelRecorder) RecordNode(ctx context.Context, metric NodeMetric) {
	options := otelmetric.WithAttributes(attrs(
		"pipeline", metric.Pipeline,
		"stage_name", metric.StageName,
		"node_kind", metric.NodeKind,
		"status", normalizeStatus(metric.Status),
	)...)
	r.nodeTotal.Add(ctx, 1, options)
	r.nodeDuration.Record(ctx, durationMillis(metric.Duration), options)
}

func (r *otelRecorder) RecordStage(ctx context.Context, metric StageMetric) {
	r.stageDuration.Record(ctx, durationMillis(metric.Duration), otelmetric.WithAttributes(attrs(
		"pipeline", metric.Pipeline,
		"stage_name", metric.StageName,
		"stage_category", metric.StageCategory,
		"status", normalizeStatus(metric.Status),
	)...))
}

func (r *otelRecorder) RecordRetriever(ctx context.Context, metric RetrieverMetric) {
	options := otelmetric.WithAttributes(attrs(
		"pipeline", metric.Pipeline,
		"stage_name", metric.StageName,
		"source_type", metric.SourceType,
		"status", normalizeStatus(metric.Status),
	)...)
	r.retrieverDuration.Record(ctx, durationMillis(metric.Duration), options)
	r.retrieverCandidates.Record(ctx, int64(metric.Candidates), otelmetric.WithAttributes(attrs(
		"pipeline", metric.Pipeline,
		"stage_name", metric.StageName,
		"source_type", metric.SourceType,
	)...))
	if metric.Candidates == 0 {
		r.retrieverEmptyTotal.Add(ctx, 1, otelmetric.WithAttributes(attrs(
			"pipeline", metric.Pipeline,
			"stage_name", metric.StageName,
			"source_type", metric.SourceType,
		)...))
	}
}

func (r *otelRecorder) RecordFeatureFetch(ctx context.Context, metric FeatureFetchMetric) {
	r.featureFetchDuration.Record(ctx, durationMillis(metric.Duration), otelmetric.WithAttributes(attrs(
		"pipeline", metric.Pipeline,
		"stage_name", metric.StageName,
		"scope", metric.Scope,
		"entity", metric.Entity,
		"status", normalizeStatus(metric.Status),
	)...))
}

func (r *otelRecorder) RecordFeatureDefault(ctx context.Context, metric FeatureDefaultMetric) {
	baseAttrs := attrs(
		"pipeline", metric.Pipeline,
		"stage_name", metric.StageName,
		"scope", metric.Scope,
		"entity", metric.Entity,
		"reason", metric.Reason,
	)
	r.featureFetchDefault.Add(ctx, 1, otelmetric.WithAttributes(baseAttrs...))

	feature := normalizeValue(metric.Feature)
	if _, ok := r.allowedFeatureDimensions[feature]; !ok {
		return
	}
	featureAttrs := append(baseAttrs, attribute.String("feature", feature))
	r.featureFetchByFeature.Add(ctx, 1, otelmetric.WithAttributes(featureAttrs...))
}

func (r *otelRecorder) RecordResilience(ctx context.Context, metric ResilienceMetric) {
	r.resilienceTotal.Add(ctx, 1, otelmetric.WithAttributes(attrs(
		"pipeline", metric.Pipeline,
		"stage_name", metric.StageName,
		"node_kind", metric.NodeKind,
		"status", normalizeStatus(metric.Status),
		"reason", metric.Reason,
		"attempts", strconv.Itoa(metric.Attempts),
	)...))
}

func (r *TestRecorder) RecordRequest(_ context.Context, metric RequestMetric) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Requests = append(r.Requests, normalizeRequestMetric(metric))
}

func (r *TestRecorder) RecordNode(_ context.Context, metric NodeMetric) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Nodes = append(r.Nodes, normalizeNodeMetric(metric))
}

func (r *TestRecorder) RecordStage(_ context.Context, metric StageMetric) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Stages = append(r.Stages, normalizeStageMetric(metric))
}

func (r *TestRecorder) RecordRetriever(_ context.Context, metric RetrieverMetric) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Retrievers = append(r.Retrievers, normalizeRetrieverMetric(metric))
}

func (r *TestRecorder) RecordFeatureFetch(_ context.Context, metric FeatureFetchMetric) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.FeatureFetches = append(r.FeatureFetches, normalizeFeatureFetchMetric(metric))
}

func (r *TestRecorder) RecordFeatureDefault(_ context.Context, metric FeatureDefaultMetric) {
	r.mu.Lock()
	defer r.mu.Unlock()
	normalized := normalizeFeatureDefaultMetric(metric)
	if normalized.Feature != "" {
		if _, ok := r.allowedFeatures[normalized.Feature]; !ok {
			normalized.Feature = ""
		}
	}
	r.FeatureDefaults = append(r.FeatureDefaults, normalized)
}

func (r *TestRecorder) RecordResilience(_ context.Context, metric ResilienceMetric) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Resilience = append(r.Resilience, normalizeResilienceMetric(metric))
}

func normalizeRequestMetric(metric RequestMetric) RequestMetric {
	metric.Pipeline = normalizeValue(metric.Pipeline)
	metric.Status = normalizeStatus(metric.Status)
	return metric
}

func normalizeNodeMetric(metric NodeMetric) NodeMetric {
	metric.Pipeline = normalizeValue(metric.Pipeline)
	metric.StageName = normalizeValue(metric.StageName)
	metric.NodeKind = normalizeValue(metric.NodeKind)
	metric.Status = normalizeStatus(metric.Status)
	return metric
}

func normalizeStageMetric(metric StageMetric) StageMetric {
	metric.Pipeline = normalizeValue(metric.Pipeline)
	metric.StageName = normalizeValue(metric.StageName)
	metric.StageCategory = normalizeValue(metric.StageCategory)
	metric.Status = normalizeStatus(metric.Status)
	return metric
}

func normalizeRetrieverMetric(metric RetrieverMetric) RetrieverMetric {
	metric.Pipeline = normalizeValue(metric.Pipeline)
	metric.StageName = normalizeValue(metric.StageName)
	metric.SourceType = normalizeValue(metric.SourceType)
	metric.Status = normalizeStatus(metric.Status)
	return metric
}

func normalizeFeatureFetchMetric(metric FeatureFetchMetric) FeatureFetchMetric {
	metric.Pipeline = normalizeValue(metric.Pipeline)
	metric.StageName = normalizeValue(metric.StageName)
	metric.Scope = normalizeValue(metric.Scope)
	metric.Entity = normalizeValue(metric.Entity)
	metric.Status = normalizeStatus(metric.Status)
	return metric
}

func normalizeFeatureDefaultMetric(metric FeatureDefaultMetric) FeatureDefaultMetric {
	metric.Pipeline = normalizeValue(metric.Pipeline)
	metric.StageName = normalizeValue(metric.StageName)
	metric.Scope = normalizeValue(metric.Scope)
	metric.Entity = normalizeValue(metric.Entity)
	metric.Feature = normalizeValue(metric.Feature)
	metric.Reason = normalizeValue(metric.Reason)
	return metric
}

func normalizeResilienceMetric(metric ResilienceMetric) ResilienceMetric {
	metric.Pipeline = normalizeValue(metric.Pipeline)
	metric.StageName = normalizeValue(metric.StageName)
	metric.NodeKind = normalizeValue(metric.NodeKind)
	metric.Status = normalizeStatus(metric.Status)
	metric.Reason = normalizeValue(metric.Reason)
	if metric.Attempts < 0 {
		metric.Attempts = 0
	}
	return metric
}
