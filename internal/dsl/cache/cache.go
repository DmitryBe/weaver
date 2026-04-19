package cache

import (
	"time"

	"github.com/dmitryBe/weaver/internal/dsl/expr"
)

type Config struct {
	KeyExpr expr.Expr
	TTLValue time.Duration
}

func Key(key expr.Expr) *Config {
	return &Config{KeyExpr: key}
}

func (c *Config) TTL(ttl time.Duration) *Config {
	c.TTLValue = ttl
	return c
}
