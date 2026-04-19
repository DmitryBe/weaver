package neo4j

import (
	"context"

	driverneo4j "github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

type ClientConfig struct {
	URI      string
	Username string
	Password string
	Database string // optional; empty means driver's default database
}

type BoltClient struct {
	driver   driverneo4j.DriverWithContext
	database string
}

// NewBoltClient opens a Neo4j driver connection and verifies connectivity.
// Supported URI examples:
//   - "bolt://localhost:7687"
//   - "neo4j://localhost:7687"
//   - "neo4j+s://xxxx.databases.neo4j.io"
func NewBoltClient(ctx context.Context, cfg ClientConfig) (*BoltClient, error) {
	driver, err := driverneo4j.NewDriverWithContext(
		cfg.URI,
		driverneo4j.BasicAuth(cfg.Username, cfg.Password, ""),
	)
	if err != nil {
		return nil, err
	}

	if err := driver.VerifyConnectivity(ctx); err != nil {
		_ = driver.Close(ctx)
		return nil, err
	}

	return &BoltClient{
		driver:   driver,
		database: cfg.Database,
	}, nil
}

func (c *BoltClient) Query(ctx context.Context, query string, params map[string]any) ([]map[string]any, error) {
	var options []driverneo4j.ExecuteQueryConfigurationOption
	if c.database != "" {
		options = append(options, driverneo4j.ExecuteQueryWithDatabase(c.database))
	}

	result, err := driverneo4j.ExecuteQuery(
		ctx,
		c.driver,
		query,
		params,
		driverneo4j.EagerResultTransformer,
		options...,
	)
	if err != nil {
		return nil, err
	}

	rows := make([]map[string]any, len(result.Records))
	for i, rec := range result.Records {
		row := make(map[string]any, len(rec.Keys))
		for _, k := range rec.Keys {
			v, _ := rec.Get(k)
			row[k] = v
		}
		rows[i] = row
	}

	return rows, nil
}

// Close releases the underlying driver.
func (c *BoltClient) Close(ctx context.Context) error {
	return c.driver.Close(ctx)
}
