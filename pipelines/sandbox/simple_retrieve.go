package sandbox

import (
	"github.com/dmitryBe/weaver/internal/dsl/expr"
	"github.com/dmitryBe/weaver/internal/dsl/input"
	"github.com/dmitryBe/weaver/internal/dsl/merge"
	"github.com/dmitryBe/weaver/internal/dsl/pipeline"
	"github.com/dmitryBe/weaver/internal/dsl/retrieve"
)

func init() {
	pipeline.DefaultRegistry.Register("sandbox", "simple_retrieve", buildSimpleRetrieve)
}

func buildSimpleRetrieve() *pipeline.Spec {
	return pipeline.New().
		Input(
			input.String("city"),
			input.String("vertical"),
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
				merge.Default().DedupBy("brand_id").SortByScore("score"),
			),
		)
}
