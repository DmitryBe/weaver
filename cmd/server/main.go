package main

import (
	"context"
	"log"
	"net/http"
	"strconv"

	"github.com/dmitryBe/weaver/internal/api"
	"github.com/dmitryBe/weaver/internal/runner"
)

func main() {
	ctx := context.Background()

	current, err := runner.New(ctx, "")
	if err != nil {
		log.Fatalf("build runner: %v", err)
	}
	defer current.Close()

	addr := ":" + strconv.Itoa(current.Config.Port)
	log.Printf("weaver listening on %s", addr)
	if err := http.ListenAndServe(addr, api.NewServer(current)); err != nil {
		log.Fatalf("serve http: %v", err)
	}
}
