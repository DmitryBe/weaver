package nodeutil

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/dmitryBe/weaver/internal/runtime"
)

func CallRESTModel(ctx context.Context, endpoint any, payload any) (any, error) {
	url := fmt.Sprint(NormalizeEndpoint(endpoint))
	if url == "" {
		return nil, runtime.NewExecutionError(
			runtime.CategoryPipelineMisconfigured,
			"pipeline misconfigured: model endpoint is required",
			fmt.Errorf("model endpoint is required"),
		)
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, runtime.NewExecutionError(
			runtime.CategoryPipelineMisconfigured,
			"pipeline misconfigured: model request could not be encoded",
			fmt.Errorf("encode model request: %w", err),
		)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, runtime.NewExecutionError(
			runtime.CategoryPipelineMisconfigured,
			"pipeline misconfigured: model endpoint request is invalid",
			fmt.Errorf("build model request: %w", err),
		)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		category := runtime.CategoryDependencyFailed
		safeMessage := "upstream model endpoint unavailable"
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			category = runtime.CategoryDependencyTimeout
			safeMessage = "upstream model request timed out"
		}
		return nil, runtime.NewExecutionError(
			category,
			safeMessage,
			fmt.Errorf("call model endpoint %q: %w", url, err),
		)
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		contents, readErr := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if readErr != nil {
			return nil, runtime.NewExecutionError(
				runtime.CategoryDependencyFailed,
				"upstream model endpoint returned an error",
				fmt.Errorf("model endpoint %q returned status %d", url, resp.StatusCode),
			)
		}
		return nil, runtime.NewExecutionError(
			runtime.CategoryDependencyFailed,
			"upstream model endpoint returned an error",
			fmt.Errorf("model endpoint %q returned status %d: %s", url, resp.StatusCode, bytes.TrimSpace(contents)),
		)
	}

	var decoded any
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return nil, runtime.NewExecutionError(
			runtime.CategoryDependencyFailed,
			"upstream model endpoint returned an invalid response",
			fmt.Errorf("decode model response: %w", err),
		)
	}
	return decoded, nil
}
