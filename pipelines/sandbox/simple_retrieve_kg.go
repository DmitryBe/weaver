package sandbox

import (
	"time"

	"github.com/dmitryBe/weaver/internal/dsl/cache"
	"github.com/dmitryBe/weaver/internal/dsl/expr"
	"github.com/dmitryBe/weaver/internal/dsl/input"
	"github.com/dmitryBe/weaver/internal/dsl/merge"
	"github.com/dmitryBe/weaver/internal/dsl/param"
	"github.com/dmitryBe/weaver/internal/dsl/pipeline"
	"github.com/dmitryBe/weaver/internal/dsl/retrieve"
)

func init() {
	pipeline.DefaultRegistry.Register("sandbox", "simple_retrieve_kg", buildSimpleRetrieveKG)
}

func buildSimpleRetrieveKG() *pipeline.Spec {
	return pipeline.New().
		Cache(
			cache.Key(expr.Tuple(
				expr.Const("city"),
			)).TTL(10*time.Second),
		).
		Input(
			input.Int("city"),
		).
		Retrieve(
			"retrieve_candidates",
			retrieve.Parallel(
				retrieve.KG("queries/kg/food/popular_by_city.cql").
					Params(
						param.String("city_id", expr.Context("city")),
					).
					TopK(25),
			).Merge(
				merge.Default().DedupBy("brand_id").SortByScore("score"),
			),
		)
}
