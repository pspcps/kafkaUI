// Package validation provides pluggable per-topic message validators.
//
// Validators are compiled once and cached in memory keyed by
// cluster:topic:version. The "version" is bumped by the schema repo on every
// upsert, so a schema change automatically invalidates the cached compilation
// without an explicit eviction call.
package validation

import (
	"errors"
	"fmt"
	"sync"
)

// Validator validates a raw message payload against a schema.
type Validator interface {
	// Validate returns nil if the payload conforms to the schema.
	// Returned errors should be safe to surface to API callers.
	Validate(payload []byte) error
	// Format reports the schema format this validator implements.
	Format() string
}

// Compile parses a schema source into a Validator.
// Supported formats:
//   "none", "manual", ""    → noopValidator (always valid)
//   "json_schema"           → JSON Schema (draft 2020-12)
//   "avro"                  → Avro JSON schema
//   "protobuf"              → not yet implemented
func Compile(format, schemaText string) (Validator, error) {
	switch format {
	case "", "none", "manual":
		return noopValidator{format: format}, nil
	case "json_schema":
		return compileJSONSchema(schemaText)
	case "avro":
		return compileAvro(schemaText)
	case "protobuf":
		return nil, errors.New("protobuf validation is not yet implemented")
	default:
		return nil, fmt.Errorf("unknown schema format: %q", format)
	}
}

type noopValidator struct{ format string }

func (n noopValidator) Validate(_ []byte) error { return nil }
func (n noopValidator) Format() string {
	if n.format == "" {
		return "none"
	}
	return n.format
}

// Cache memoises compiled validators keyed by cluster:topic:version.
//
// Concurrency: lock-free reads via sync.Map; on miss, sync.Once ensures the
// schema is parsed only once even under concurrent first-use.
type Cache struct {
	entries sync.Map // key: string → *cacheEntry
}

type cacheEntry struct {
	once      sync.Once
	validator Validator
	err       error
}

// NewCache returns an empty cache.
func NewCache() *Cache { return &Cache{} }

// Get returns the cached validator for the given cluster/topic/version,
// compiling on first use. The "version" parameter is what makes the cache
// self-invalidating: bumping the schema version produces a different key,
// so old entries simply become unreachable (and eligible for GC after the
// reference is dropped).
func (c *Cache) Get(clusterID, topic string, version int, format, schemaText string) (Validator, error) {
	key := fmt.Sprintf("%s\x00%s\x00%d", clusterID, topic, version)
	v, _ := c.entries.LoadOrStore(key, &cacheEntry{})
	entry := v.(*cacheEntry)
	entry.once.Do(func() {
		entry.validator, entry.err = Compile(format, schemaText)
	})
	return entry.validator, entry.err
}

// Forget evicts every cached entry for a given cluster:topic prefix.
// Called on schema delete (rare); not needed on update because version bumps.
func (c *Cache) Forget(clusterID, topic string) {
	prefix := fmt.Sprintf("%s\x00%s\x00", clusterID, topic)
	c.entries.Range(func(k, _ any) bool {
		if s, ok := k.(string); ok && len(s) >= len(prefix) && s[:len(prefix)] == prefix {
			c.entries.Delete(k)
		}
		return true
	})
}
