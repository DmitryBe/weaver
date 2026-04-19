package api

import "github.com/dmitryBe/weaver/internal/runtime"

// SurfaceRequest is the POST /v1/surface/{name...} request body.
type SurfaceRequest struct {
	RequestID  string           `json:"request_id,omitempty"`
	Context    map[string]any   `json:"context"`
	Candidates []map[string]any `json:"candidates,omitempty"`
}

// SurfaceResponse is the POST /v1/surface/{name...} success response body.
type SurfaceResponse struct {
	RequestID  string             `json:"request_id"`
	Pipeline   string             `json:"pipeline"`
	Candidates []map[string]any   `json:"candidates"`
	CacheHit   bool               `json:"cache_hit"`
	DebugInfo  *runtime.DebugInfo `json:"debug_info,omitempty"`
}

// ErrorResponse is returned on all error paths.
type ErrorResponse struct {
	RequestID string `json:"request_id,omitempty"`
	Error     string `json:"error"`
	Code      string `json:"code"`
	Category  string `json:"category"`
}

// PipelineInfo is one entry in GET /v1/registry response.
type PipelineInfo struct {
	FullName string `json:"full_name"`
	Domain   string `json:"domain"`
	Name     string `json:"name"`
}

// RegistryResponse is the GET /v1/registry response body.
type RegistryResponse struct {
	Pipelines []PipelineInfo `json:"pipelines"`
}
