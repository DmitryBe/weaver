package sandbox

import (
	"time"

	"github.com/dmitryBe/weaver/internal/dsl/config"
	"github.com/dmitryBe/weaver/internal/dsl/expr"
	"github.com/dmitryBe/weaver/internal/dsl/expr/request"
	"github.com/dmitryBe/weaver/internal/dsl/expr/response"
	"github.com/dmitryBe/weaver/internal/dsl/input"
	"github.com/dmitryBe/weaver/internal/dsl/op"
	"github.com/dmitryBe/weaver/internal/dsl/pipeline"
	"github.com/dmitryBe/weaver/internal/dsl/resilient"
)

func init() {
	pipeline.DefaultRegistry.Register("sandbox", "simple_embed", buildSimpleEmbed)
}

func buildSimpleEmbed() *pipeline.Spec {
	return pipeline.New().
		Resilience(resilient.Default(100*time.Millisecond, 3)).
		Input(
			input.String("user_id"),
			input.String("text"),
		).
		Context(
			"ctx_embed",
			op.Model("embed_user").
				Endpoint(config.Val("http://localhost:8001/embed")).
				Request(
					request.Object(
						request.Field("user_id", expr.Context("user_id")),
						request.Field("text", expr.Context("text")),
					),
				).
				Response(response.Path("response.0")).
				Into("embedding"),
		)
}
