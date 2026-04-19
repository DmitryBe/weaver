package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/dmitryBe/weaver/internal/runner"
	"github.com/dmitryBe/weaver/internal/runtime"
)

var errHelp = errors.New("help requested")

type options struct {
	ConfigPath string
	Pipeline   string
	RequestID  string
	Context    map[string]any
	Debug      bool
}

type output struct {
	RequestID  string             `json:"request_id"`
	Pipeline   string             `json:"pipeline"`
	Context    map[string]any     `json:"context"`
	Candidates []map[string]any   `json:"candidates"`
	DebugInfo  *runtime.DebugInfo `json:"debug_info,omitempty"`
}

func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdout io.Writer, stderr io.Writer) int {
	opts, err := parseArgs(args)
	if err != nil {
		if errors.Is(err, errHelp) {
			_, _ = fmt.Fprint(stdout, usage())
			return 0
		}
		_, _ = fmt.Fprintf(stderr, "error: %v\n\n%s", err, usage())
		return 2
	}

	result, err := runWithOptions(ctx, opts)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}

	if err := json.NewEncoder(stdout).Encode(result); err != nil {
		_, _ = fmt.Fprintf(stderr, "error: encode result: %v\n", err)
		return 1
	}
	return 0
}

func runWithOptions(ctx context.Context, opts options) (output, error) {
	current, err := runner.New(ctx, opts.ConfigPath)
	if err != nil {
		return output{}, err
	}
	defer current.Close()

	result, err := current.Execute(ctx, runner.ExecuteRequest{
		Pipeline:       opts.Pipeline,
		RequestID:      opts.RequestID,
		Context:        opts.Context,
		NormalizeInput: true,
		Debug:          opts.Debug,
	})
	if err != nil {
		return output{}, err
	}
	return output{
		RequestID:  result.RequestID,
		Pipeline:   result.Pipeline,
		Context:    copyMap(map[string]any(result.State.Context)),
		Candidates: toResponse(result.State.Candidates),
		DebugInfo:  result.DebugInfo,
	}, nil
}

func parseArgs(args []string) (options, error) {
	opts := options{
		Context: make(map[string]any),
	}

	for i := 0; i < len(args); i++ {
		switch arg := args[i]; arg {
		case "-h", "--help":
			return options{}, errHelp
		case "--config":
			value, next, err := requireValue(args, i, "--config")
			if err != nil {
				return options{}, err
			}
			opts.ConfigPath = value
			i = next
		case "--pipeline":
			value, next, err := requireValue(args, i, "--pipeline")
			if err != nil {
				return options{}, err
			}
			opts.Pipeline = value
			i = next
		case "--request-id":
			value, next, err := requireValue(args, i, "--request-id")
			if err != nil {
				return options{}, err
			}
			opts.RequestID = value
			i = next
		case "--debug":
			opts.Debug = true
		case "--context":
			next, err := parseContextArgs(args, i+1, opts.Context)
			if err != nil {
				return options{}, err
			}
			i = next - 1
		default:
			if strings.HasPrefix(arg, "--") {
				return options{}, fmt.Errorf("unknown flag %q", arg)
			}
			if opts.Pipeline != "" {
				return options{}, fmt.Errorf("unexpected argument %q", arg)
			}
			opts.Pipeline = arg
		}
	}

	if strings.TrimSpace(opts.Pipeline) == "" {
		return options{}, fmt.Errorf("pipeline name is required")
	}
	return opts, nil
}

func parseContextArgs(args []string, start int, target map[string]any) (int, error) {
	count := 0
	i := start
	for i < len(args) {
		current := args[i]
		if strings.HasPrefix(current, "--") || !strings.Contains(current, "=") {
			break
		}

		key, value, err := parseAssignment(current)
		if err != nil {
			return 0, err
		}
		target[key] = value
		count++
		i++
	}
	if count == 0 {
		return 0, fmt.Errorf("--context requires one or more key=value pairs")
	}
	return i, nil
}

func requireValue(args []string, index int, flag string) (string, int, error) {
	if index+1 >= len(args) {
		return "", 0, fmt.Errorf("%s requires a value", flag)
	}
	return args[index+1], index + 1, nil
}

func parseAssignment(token string) (string, any, error) {
	parts := strings.SplitN(token, "=", 2)
	key := strings.TrimSpace(parts[0])
	if len(parts) != 2 || key == "" {
		return "", nil, fmt.Errorf("invalid key=value pair %q", token)
	}
	return key, parseValue(parts[1]), nil
}

func parseValue(raw string) any {
	if raw == "" {
		return ""
	}

	if !looksLikeJSONLiteral(raw) {
		return raw
	}

	var decoded any
	if err := json.Unmarshal([]byte(raw), &decoded); err == nil {
		return decoded
	}
	return raw
}

func looksLikeJSONLiteral(raw string) bool {
	switch raw {
	case "true", "false", "null":
		return true
	}
	return strings.HasPrefix(raw, "{") || strings.HasPrefix(raw, "[") || strings.HasPrefix(raw, `"`)
}

func toResponse(in []runtime.Candidate) []map[string]any {
	out := make([]map[string]any, len(in))
	for i, candidate := range in {
		if candidate == nil {
			out[i] = nil
			continue
		}
		out[i] = copyMap(map[string]any(candidate))
	}
	return out
}

func copyMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func usage() string {
	return `Usage:
  prun <pipeline-name> --context key=value [key=value ...]
  prun --pipeline <pipeline-name> --context key=value [key=value ...]

Options:
  --config <path>      Load config from a specific file instead of CONFIG_FILE
  --request-id <id>    Use a fixed request id in the output
  --debug              Include per-step pipeline snapshots in debug_info
  --context ...        One or more context fields passed as key=value pairs
  -h, --help           Show this help
`
}
