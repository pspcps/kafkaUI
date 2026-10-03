package validation

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/hamba/avro/v2"
)

// avroValidator validates messages against an Avro JSON schema.
//
// We accept payloads in *Avro JSON encoding* (i.e. the message body is itself
// JSON conforming to the Avro type system). Producers using the Confluent
// binary wire format (5-byte magic + binary Avro) should not have their
// payloads validated by this UI — those are pre-validated against a schema
// registry. We surface an explicit error if we detect a binary payload.
type avroValidator struct {
	schema avro.Schema
}

func compileAvro(schemaText string) (Validator, error) {
	if strings.TrimSpace(schemaText) == "" {
		return nil, errors.New("avro: schema text is empty")
	}
	s, err := avro.Parse(schemaText)
	if err != nil {
		return nil, fmt.Errorf("avro: parse: %w", err)
	}
	return &avroValidator{schema: s}, nil
}

func (v *avroValidator) Format() string { return "avro" }

func (v *avroValidator) Validate(payload []byte) error {
	if looksLikeBinaryAvro(payload) {
		return errors.New("avro: payload looks like Confluent binary wire format; UI validation expects Avro JSON encoding")
	}
	var generic any
	if err := json.Unmarshal(payload, &generic); err != nil {
		return fmt.Errorf("avro: payload is not valid JSON: %w", err)
	}
	// The hamba avro library validates by attempting to encode the value
	// against the schema. If the structure mismatches the schema, encoding
	// fails with a descriptive error.
	if _, err := avro.Marshal(v.schema, generic); err != nil {
		return fmt.Errorf("avro: %w", err)
	}
	return nil
}

func looksLikeBinaryAvro(p []byte) bool {
	// Confluent wire format: magic byte 0x00 followed by 4-byte schema ID.
	return len(p) > 5 && p[0] == 0x00
}
