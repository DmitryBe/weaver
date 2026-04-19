package featurestore

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/dmitryBe/weaver/internal/runtime"
)

type FileStore struct {
	Dir string
}

func NewFileStore(dir string) *FileStore {
	return &FileStore{Dir: dir}
}

func (s *FileStore) Fetch(ctx context.Context, features []string, keys ...runtime.Key) ([]map[string]any, error) {
	_ = ctx
	if len(features) == 0 || len(keys) == 0 {
		return []map[string]any{}, nil
	}

	entity, err := requireSingleEntity(features)
	if err != nil {
		return nil, err
	}
	entityData, err := s.loadEntity(entity)
	if err != nil {
		return nil, err
	}

	rows := make([]map[string]any, len(keys))
	for i, key := range keys {
		lookupKey := normalizeLookupKey(key)
		row := make(map[string]any)
		record, ok := entityData[lookupKey]
		if ok {
			for _, feature := range features {
				if value, exists := record[feature]; exists {
					row[feature] = value
				}
			}
		}
		rows[i] = row
	}
	return rows, nil
}

func (s *FileStore) loadEntity(entity string) (map[string]map[string]any, error) {
	path := filepath.Join(s.Dir, entity+".json")
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read feature store entity %q: %w", entity, err)
	}

	data := make(map[string]map[string]any)
	if err := json.Unmarshal(contents, &data); err != nil {
		return nil, fmt.Errorf("decode feature store entity %q: %w", entity, err)
	}
	return data, nil
}

func requireSingleEntity(features []string) (string, error) {
	if len(features) == 0 {
		return "", nil
	}

	entity, err := entityFromFeature(features[0])
	if err != nil {
		return "", err
	}
	for _, feature := range features[1:] {
		current, err := entityFromFeature(feature)
		if err != nil {
			return "", err
		}
		if current != entity {
			return "", fmt.Errorf("feature store fetch requires features from one entity, got %q and %q", entity, current)
		}
	}
	return entity, nil
}

func entityFromFeature(feature string) (string, error) {
	entity, _, ok := strings.Cut(feature, "/")
	if !ok || entity == "" {
		return "", fmt.Errorf("feature %q must be in entity/name form", feature)
	}
	return entity, nil
}

func normalizeLookupKey(key runtime.Key) string {
	fields := make([]string, 0, len(key))
	for field := range key {
		fields = append(fields, field)
	}
	sort.Strings(fields)

	parts := make([]string, len(fields))
	for i, field := range fields {
		parts[i] = key[field]
	}
	return strings.Join(parts, ":")
}
