package sandbox

import (
	"time"

	"github.com/dmitryBe/weaver/internal/dsl/cache"
	"github.com/dmitryBe/weaver/internal/dsl/config"
	"github.com/dmitryBe/weaver/internal/dsl/expr"
	"github.com/dmitryBe/weaver/internal/dsl/expr/request"
	"github.com/dmitryBe/weaver/internal/dsl/expr/response"
	"github.com/dmitryBe/weaver/internal/dsl/input"
	"github.com/dmitryBe/weaver/internal/dsl/merge"
	"github.com/dmitryBe/weaver/internal/dsl/op"
	"github.com/dmitryBe/weaver/internal/dsl/pipeline"
	"github.com/dmitryBe/weaver/internal/dsl/resilient"
	"github.com/dmitryBe/weaver/internal/dsl/retrieve"
)

func init() {
	pipeline.DefaultRegistry.Register("sandbox", "simple_e2e", buildSimpleE2E)
}

// simple_e2e is the smallest end-to-end example that shows
// context enrichment, retrieval, candidate enrichment, candidate mutation,
// scoring, and post-ranking in one readable pipeline.
func buildSimpleE2E() *pipeline.Spec {
	return pipeline.New().
		Resilience(resilient.Default(100*time.Millisecond, 3)).
		Cache(
			cache.Key(expr.Tuple(
				expr.Const("city"),
				expr.Const("vertical"),
				expr.Const("user_id"),
			)).TTL(10*time.Second),
		).
		Input(
			input.String("city"),
			input.String("vertical"),
			input.String("user_id"),
		).
		Context(
			"ctx_fetch",
			op.Fetch("user/segment").
				Key(expr.Context("user_id")).
				Into("user_segment").
				Float().
				Default(0),
		).
		Context(
			"ctx_embed",
			op.Model("embed_user").
				Endpoint(config.Val("http://localhost:8001/embed")).
				Request(
					request.Object(
						request.Field("user_id", expr.Context("user_id")),
					),
				).
				Response(response.Path("response.0")).
				Into("user_embedding"),
		).
		Retrieve(
			"retrieve_candidates",
			retrieve.Parallel(
				retrieve.KV("trending/brands").
					Key(
						expr.Tuple(
							expr.Context("city"),
							expr.Context("vertical"),
						),
					).
					TopK(20),
			).Merge(
				merge.Default().DedupBy("brand_id"), // .SortByScore("score"),
			),
		).
		Candidates(
			"cand_fetch",
			op.Fetch("brand/rating").
				Key(expr.Candidate("brand_id")).
				Into("rating").
				Float().
				Default(0),
		).
		Candidates(
			"cand_prepare",
			op.Set("user_segment", expr.Context("user_segment")).Float(),
			op.Set("ann_score", expr.Candidate("score")),
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
									request.Field("id", expr.Candidate("brand_id")),
									request.Field("rating", expr.Candidate("rating")),
									request.Field("user_segment", expr.Candidate("user_segment")),
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
		)
}
