package validation

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v5"
)

type jsonSchemaValidator struct {
	schema *jsonschema.Schema
}

func compileJSONSchema(schemaText string) (Validator, error) {
	if strings.TrimSpace(schemaText) == "" {
		return nil, errors.New("json_schema: schema text is empty")
	}
	c := jsonschema.NewCompiler()
	if err := c.AddResource("schema.json", bytes.NewReader([]byte(schemaText))); err != nil {
		return nil, fmt.Errorf("json_schema: parse: %w", err)
	}
	s, err := c.Compile("schema.json")
	if err != nil {
		return nil, fmt.Errorf("json_schema: compile: %w", err)
	}
	return &jsonSchemaValidator{schema: s}, nil
}

func (v *jsonSchemaValidator) Format() string { return "json_schema" }

func (v *jsonSchemaValidator) Validate(payload []byte) error {
	var data any
	if err := json.Unmarshal(payload, &data); err != nil {
		return fmt.Errorf("payload is not valid JSON: %w", err)
	}
	if err := v.schema.Validate(data); err != nil {
		var ve *jsonschema.ValidationError
		if errors.As(err, &ve) {
			return formatValidationError(ve)
		}
		return err
	}
	return nil
}

// formatValidationError walks the jsonschema ValidationError tree and produces
// a single human-readable string with the failing field paths and reasons.
func formatValidationError(ve *jsonschema.ValidationError) error {
	var b strings.Builder
	b.WriteString("schema validation failed:")
	walkVE(&b, ve, "")
	return errors.New(b.String())
}

func walkVE(b *strings.Builder, ve *jsonschema.ValidationError, indent string) {
	if ve == nil {
		return
	}
	loc := ve.InstanceLocation
	if loc == "" {
		loc = "(root)"
	}
	fmt.Fprintf(b, "\n%s- %s: %s", indent, loc, ve.Message)
	for _, child := range ve.Causes {
		walkVE(b, child, indent+"  ")
	}
}
