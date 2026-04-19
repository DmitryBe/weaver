package testutil

import (
	"context"
	"sync"
	"time"

	"github.com/dmitryBe/weaver/internal/runtime"
)

type FeatureStoreCall struct {
	Features []string
	Keys     []runtime.Key
}

type CountingFeatureStore struct {
	base  runtime.FeatureStore
	Delay time.Duration

	mu          sync.Mutex
	calls       int
	logs        []FeatureStoreCall
	inFlight    int
	maxInFlight int
}

func NewCountingFeatureStore(base runtime.FeatureStore) *CountingFeatureStore {
	return &CountingFeatureStore{base: base}
}

func (s *CountingFeatureStore) Fetch(ctx context.Context, features []string, keys ...runtime.Key) ([]map[string]any, error) {
	s.mu.Lock()
	s.calls++
	s.inFlight++
	if s.inFlight > s.maxInFlight {
		s.maxInFlight = s.inFlight
	}
	copiedKeys := make([]runtime.Key, len(keys))
	for i, key := range keys {
		keyCopy := make(runtime.Key, len(key))
		for k, v := range key {
			keyCopy[k] = v
		}
		copiedKeys[i] = keyCopy
	}
	s.logs = append(s.logs, FeatureStoreCall{
		Features: append([]string(nil), features...),
		Keys:     copiedKeys,
	})
	s.mu.Unlock()

	if s.Delay > 0 {
		time.Sleep(s.Delay)
	}

	rows, err := s.base.Fetch(ctx, features, keys...)

	s.mu.Lock()
	s.inFlight--
	s.mu.Unlock()
	return rows, err
}

func (s *CountingFeatureStore) Calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *CountingFeatureStore) MaxInFlight() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxInFlight
}

func (s *CountingFeatureStore) Logs() []FeatureStoreCall {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]FeatureStoreCall, len(s.logs))
	for i, call := range s.logs {
		keys := make([]runtime.Key, len(call.Keys))
		for j, key := range call.Keys {
			keyCopy := make(runtime.Key, len(key))
			for k, v := range key {
				keyCopy[k] = v
			}
			keys[j] = keyCopy
		}
		out[i] = FeatureStoreCall{
			Features: append([]string(nil), call.Features...),
			Keys:     keys,
		}
	}
	return out
}
