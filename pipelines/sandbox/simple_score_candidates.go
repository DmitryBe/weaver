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
	pipeline.DefaultRegistry.Register("sandbox", "simple_score_candidates", buildSimpleScoreCandidates)
}

// simple_score_candidates shows how to score candidates that are supplied
// directly in the request instead of being retrieved by the pipeline.
func buildSimpleScoreCandidates() *pipeline.Spec {
	return pipeline.New().
		Resilience(resilient.Default(100*time.Millisecond, 3)).
		Input(
			input.String("user_id"),
			input.Candidates(
				input.Int("id"),
				input.Float("rating"),
			),
		).
		Candidates(
			"cand_score",
			op.Model("rank_brands").
				Endpoint(config.Val("http://localhost:8001/score")).
				Request(
					request.Object(
						request.Field("context.user_id", expr.Context("user_id")),
						request.Field(
							"candidates",
							request.ForEachCandidate(
								request.Object(
									request.Field("id", expr.Candidate("id")),
									request.Field("rating", expr.Candidate("rating")),
								),
							),
						),
					),
				).
				Response(response.Scores()).
				Into("score"),
		).
		Candidates(
			"post_rank",
			op.SortBy("score", op.Desc),
			op.Take(10),
			op.DropExcept("id", "score"),
		)
}
