// Package schema implements the backward-compatibility-checked schema
// validation described in docs/design_schema_evolution.md. A minimal
// field-typed schema representation, not full JSON Schema/Avro/Protobuf
// — see the design doc for why that scope choice was made deliberately.
package schema

import (
	"encoding/json"
	"fmt"
	"math"
)

type FieldType string

const (
	TypeString  FieldType = "string"
	TypeInteger FieldType = "integer"
	TypeNumber  FieldType = "number"
	TypeBoolean FieldType = "boolean"
)

type Field struct {
	Type     FieldType
	Required bool
}

// Schema is one partition's currently-registered document shape.
type Schema struct {
	Fields map[string]Field
}

// Validate checks docJSON against schema: every required field present
// with a compatible type. Unknown fields not listed in the schema are
// allowed (open/additive validation, not strict) — see design doc.
func Validate(s *Schema, docJSON []byte) error {
	var doc map[string]interface{}
	if err := json.Unmarshal(docJSON, &doc); err != nil {
		return fmt.Errorf("schema: invalid JSON: %w", err)
	}

	for name, field := range s.Fields {
		val, present := doc[name]
		if !present {
			if field.Required {
				return fmt.Errorf("schema: missing required field %q", name)
			}
			continue
		}
		if err := checkType(name, field.Type, val); err != nil {
			return err
		}
	}
	return nil
}

func checkType(name string, want FieldType, val interface{}) error {
	switch want {
	case TypeString:
		if _, ok := val.(string); !ok {
			return fmt.Errorf("schema: field %q must be a string", name)
		}
	case TypeInteger:
		num, ok := val.(float64)
		if !ok || num != math.Trunc(num) {
			return fmt.Errorf("schema: field %q must be an integer", name)
		}
	case TypeNumber:
		if _, ok := val.(float64); !ok {
			return fmt.Errorf("schema: field %q must be a number", name)
		}
	case TypeBoolean:
		if _, ok := val.(bool); !ok {
			return fmt.Errorf("schema: field %q must be a boolean", name)
		}
	default:
		return fmt.Errorf("schema: unknown field type %q for field %q", want, name)
	}
	return nil
}

// typeWidenedOrSame reports whether newType can validate everything
// oldType validates — same type always qualifies, integer -> number is
// the one allowed widening (every integer is a valid number).
func typeWidenedOrSame(oldType, newType FieldType) bool {
	if oldType == newType {
		return true
	}
	return oldType == TypeInteger && newType == TypeNumber
}

// IsCompatible reports whether newSchema is backward compatible with
// oldSchema per the three rules in the design doc: no required-field
// removal, no type narrowing, no new required field without a default
// (this representation has no defaults, so any new required field breaks
// compatibility).
func IsCompatible(oldSchema, newSchema *Schema) (bool, string) {
	for name, oldField := range oldSchema.Fields {
		newField, ok := newSchema.Fields[name]
		if !ok {
			if oldField.Required {
				return false, fmt.Sprintf("required field %q removed", name)
			}
			continue
		}
		if !typeWidenedOrSame(oldField.Type, newField.Type) {
			return false, fmt.Sprintf("field %q narrowed from %s to %s", name, oldField.Type, newField.Type)
		}
	}
	for name, newField := range newSchema.Fields {
		if !newField.Required {
			continue
		}
		if _, ok := oldSchema.Fields[name]; !ok {
			return false, fmt.Sprintf("new required field %q has no default for existing documents", name)
		}
	}
	return true, ""
}
