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
	"github.com/dmitryBe/weaver/internal/dsl/param"
	"github.com/dmitryBe/weaver/internal/dsl/pipeline"
	"github.com/dmitryBe/weaver/internal/dsl/resilient"
	"github.com/dmitryBe/weaver/internal/dsl/retrieve"
	"github.com/dmitryBe/weaver/internal/dsl/types"
)

func init() {
	pipeline.DefaultRegistry.Register("sandbox", "trending", buildTrending)
}

func buildTrending() *pipeline.Spec {
	return pipeline.New().
		Input(
			input.String("city"),
			input.String("user_id"),
			input.String("vertical"),
		).
		Resilience(resilient.Standard()).
		Cache(
			cache.Key(expr.Tuple(
				expr.Const("trending"),
				expr.Context("user_id"),
			)).TTL(5*time.Minute),
		).
		Context(
			"ctx_fetch",
			op.Fetch("user/history_brands").
				Key(expr.Context("user_id")).
				Into("user_history_brand_ids"),
			op.Fetch("user/history_actions").
				Key(expr.Context("user_id")).
				Into("user_history_actions"),
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
						request.Field("history_brand_ids", expr.Context("user_history_brand_ids")),
						request.Field("history_actions", expr.Context("user_history_actions")),
					),
				).
				Response(response.Path("response.0")).
				Into("user_embedding"),
		).
		Context(
			"ctx_prepare",
			op.Set("u_segment", expr.Context("user_segment")).Float(),
			op.Set("city_copy", expr.Context("city")).String(),
			op.Set(
				"user_history_actions",
				expr.PadLeft(expr.Context("user_history_actions"), 10, 0),
			).AsType(types.Array(types.Int)),
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
					TopK(100),
				retrieve.KG("queries/kg/food/popular_by_city.cql").
					Params(
						param.String("city_id", expr.Context("city")),
						param.Int("limit", 100),
					).
					Cluster("main"),
			).Merge(
				merge.Default().DedupBy("brand_id"),
			),
		).
		Candidates(
			"cand_fetch",
			op.Fetch("brand/rating").
				Key(expr.Candidate("brand_id")).
				Into("rating").
				Float().
				Default(0),
			op.Fetch("brand/price_tier").
				Key(expr.Candidate("brand_id")).
				Into("price_tier").
				Int().
				Default(0),
			op.Fetch("brand_user/score").
				Key(
					expr.Tuple(
						expr.Candidate("brand_id"),
						expr.Context("user_id"),
					),
				).
				Into("brand_user_score").
				Float().
				Default(0),
		).
		Candidates(
			"cand_prepare",
			op.Set("user_id", expr.Context("user_id")).String(),
		).
		Candidates(
			"cand_score",
			op.Model("rank_brands").
				Endpoint(config.Val("http://localhost:8001/score")).
				Request(
					request.Object(
						request.Field("context.user_id", expr.Context("user_id")),
						request.Field(
							"request",
							request.ForEachCandidate(
								request.Object(
									request.Field("id", expr.Candidate("id")),
									request.Field("rating", expr.Candidate("rating")),
									request.Field("price_tier", expr.Candidate("price_tier")),
									request.Field("brand_user_score", expr.Candidate("brand_user_score")),
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
