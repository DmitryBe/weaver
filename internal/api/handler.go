package api

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/dmitryBe/weaver/internal/metrics"
	"github.com/dmitryBe/weaver/internal/runner"
	"github.com/dmitryBe/weaver/internal/runtime"
)

// Server holds shared dependencies for all handlers.
type Server struct {
	runner *runner.Runner
	mux    *http.ServeMux
}

// NewServer wires the HTTP handlers against a pre-built pipeline registry.
func NewServer(current *runner.Runner) *Server {
	server := &Server{
		runner: current,
		mux:    http.NewServeMux(),
	}

	server.mux.HandleFunc("POST /v1/surface/{name...}", server.serveSurface)
	server.mux.HandleFunc("GET /v1/registry", server.serveRegistry)
	server.mux.HandleFunc("GET /v1/health", server.serveHealth)
	if current != nil && current.Env != nil && current.Env.MetricsHandler != nil {
		path := current.Env.MetricsPath
		if path == "" {
			path = "/metrics"
		}
		server.mux.Handle(path, current.Env.MetricsHandler)
	}
	return server
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	recoveryMiddleware(s.mux).ServeHTTP(w, r)
}

func (s *Server) serveSurface(w http.ResponseWriter, r *http.Request) {
	fullName := r.PathValue("name")
	debugEnabled := r.URL.Query().Get("debug") == "true"

	var req SurfaceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid_argument", "malformed request body", "")
		return
	}

	requestID := req.RequestID
	startedAt := time.Now()
	result, err := s.runner.Execute(r.Context(), runner.ExecuteRequest{
		Pipeline:       fullName,
		RequestID:      req.RequestID,
		Context:        req.Context,
		Candidates:     req.Candidates,
		NormalizeInput: false,
		Debug:          debugEnabled,
	})
	if err != nil {
		if runner.IsPipelineNotFound(err) {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "not_found", "surface not found", requestID)
			return
		}
		if runner.IsInvalidInput(err) {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", string(runtime.CategoryInvalidArgument), err.Error(), requestID)
			return
		}
		status, code, category, message := classifyExecutionError(err)
		s.runner.Env.MetricRecorder().RecordRequest(r.Context(), metrics.RequestMetric{
			Pipeline: fullName,
			Status:   metrics.StatusError,
			Duration: time.Since(startedAt),
		})
		log.Printf("pipeline error request_id=%s pipeline=%s category=%s code=%s: %v", requestID, fullName, category, code, err)
		writeError(w, status, code, category, message, requestID)
		return
	}
	s.runner.Env.MetricRecorder().RecordRequest(r.Context(), metrics.RequestMetric{
		Pipeline: result.Pipeline,
		Status:   metrics.StatusOK,
		Duration: time.Since(startedAt),
	})

	writeJSON(w, http.StatusOK, SurfaceResponse{
		RequestID:  result.RequestID,
		Pipeline:   result.Pipeline,
		Candidates: toResponse(result.State.Candidates),
		CacheHit:   result.CacheHit,
		DebugInfo:  result.DebugInfo,
	})
}

func (s *Server) serveRegistry(w http.ResponseWriter, r *http.Request) {
	pipelines := s.runner.Registry.List()
	infos := make([]PipelineInfo, 0, len(pipelines))
	for _, current := range pipelines {
		infos = append(infos, PipelineInfo{
			FullName: current.FullName,
			Domain:   current.Domain,
			Name:     current.Name,
		})
	}
	writeJSON(w, http.StatusOK, RegistryResponse{Pipelines: infos})
}

func (s *Server) serveHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
func toResponse(in []runtime.Candidate) []map[string]any {
	out := make([]map[string]any, len(in))
	for i, candidate := range in {
		if candidate == nil {
			out[i] = nil
			continue
		}
		out[i] = copyMap(map[string]any(candidate))
	}
	return out
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("writeJSON encode error: %v", err)
	}
}

func classifyExecutionError(err error) (status int, code string, category string, message string) {
	if execErr, ok := runtime.AsExecutionError(err); ok {
		switch execErr.Category {
		case runtime.CategoryInvalidArgument:
			return http.StatusBadRequest, "INVALID_ARGUMENT", string(execErr.Category), execErr.SafeMessage
		case runtime.CategoryDependencyFailed:
			return http.StatusBadGateway, "DEPENDENCY_FAILED", string(execErr.Category), execErr.SafeMessage
		case runtime.CategoryDependencyTimeout:
			return http.StatusGatewayTimeout, "DEPENDENCY_TIMEOUT", string(execErr.Category), execErr.SafeMessage
		case runtime.CategoryDataUnavailable:
			return http.StatusServiceUnavailable, "DATA_UNAVAILABLE", string(execErr.Category), execErr.SafeMessage
		case runtime.CategoryPipelineMisconfigured:
			fallthrough
		default:
			return http.StatusInternalServerError, "PIPELINE_MISCONFIGURED", string(runtime.CategoryPipelineMisconfigured), execErr.SafeMessage
		}
	}
	return http.StatusInternalServerError, "PIPELINE_MISCONFIGURED", string(runtime.CategoryPipelineMisconfigured), "pipeline misconfigured: execution failed"
}

func writeError(w http.ResponseWriter, status int, code, category, msg, requestID string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(ErrorResponse{
		RequestID: requestID,
		Error:     msg,
		Code:      code,
		Category:  category,
	}); err != nil {
		log.Printf("writeError encode error: %v", err)
	}
}

func copyMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
