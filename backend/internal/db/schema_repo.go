package db

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Schema format values.
const (
	SchemaFormatManual     = "manual"      // legacy: visual field list, no validation
	SchemaFormatNone       = "none"        // schemaless / flexible
	SchemaFormatJSONSchema = "json_schema" // JSON Schema 2020-12
	SchemaFormatAvro       = "avro"        // Avro JSON schema
	SchemaFormatProtobuf   = "protobuf"    // reserved for phase 2
)

// SchemaField is one field in a legacy "manual" topic schema.
type SchemaField struct {
	ID           string `json:"id"`
	SchemaID     string `json:"schemaId"`
	Name         string `json:"name"`
	FieldType    string `json:"fieldType"`
	FieldSource  string `json:"fieldSource"`
	IsRequired   bool   `json:"isRequired"`
	IsPrimaryKey bool   `json:"isPrimaryKey"`
	DefaultValue string `json:"defaultValue,omitempty"`
	Description  string `json:"description,omitempty"`
	Position     int    `json:"position"`
}

// TopicSchema holds the schema for one (cluster, topic) pair.
type TopicSchema struct {
	ID            string        `json:"id"`
	ClusterID     string        `json:"clusterId"`
	TopicName     string        `json:"topicName"`
	SchemaType    string        `json:"schemaType"`            // legacy: "manual" | "avro"
	SchemaFormat  string        `json:"schemaFormat"`          // new: see SchemaFormat* constants
	SchemaText    string        `json:"schemaText,omitempty"`  // raw schema source for json_schema/avro/protobuf
	SchemaVersion int           `json:"schemaVersion"`         // bumps on every save → invalidates validator cache
	AvroRaw       string        `json:"avroRaw,omitempty"`     // legacy column, kept in sync with schema_text for avro
	Fields        []SchemaField `json:"fields"`
	CreatedAt     time.Time     `json:"createdAt"`
	UpdatedAt     time.Time     `json:"updatedAt"`
}

// SchemaRepo handles schema persistence.
type SchemaRepo struct{ db *sql.DB }

func NewSchemaRepo(db *sql.DB) *SchemaRepo { return &SchemaRepo{db: db} }

// EnsureSchema applies idempotent ALTER TABLE migrations so new columns work
// even on databases that pre-date 007_schema_validation.sql.
func (r *SchemaRepo) EnsureSchema() error {
	const ddl = `
	ALTER TABLE IF EXISTS topic_schemas
		ADD COLUMN IF NOT EXISTS schema_format  TEXT NOT NULL DEFAULT 'manual',
		ADD COLUMN IF NOT EXISTS schema_text    TEXT,
		ADD COLUMN IF NOT EXISTS schema_version INT  NOT NULL DEFAULT 1;
	`
	if _, err := r.db.Exec(ddl); err != nil {
		return fmt.Errorf("ensure schema validation columns: %w", err)
	}
	return nil
}

func (r *SchemaRepo) Get(clusterID, topicName string) (*TopicSchema, error) {
	const q = `
		SELECT id, cluster_id, topic_name,
		       schema_type, schema_format,
		       COALESCE(schema_text,''), COALESCE(avro_raw,''), schema_version,
		       created_at, updated_at
		  FROM topic_schemas
		 WHERE cluster_id=$1 AND topic_name=$2`
	row := r.db.QueryRow(q, clusterID, topicName)

	var s TopicSchema
	if err := row.Scan(&s.ID, &s.ClusterID, &s.TopicName,
		&s.SchemaType, &s.SchemaFormat,
		&s.SchemaText, &s.AvroRaw, &s.SchemaVersion,
		&s.CreatedAt, &s.UpdatedAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	fields, err := r.getFields(s.ID)
	if err != nil {
		return nil, err
	}
	s.Fields = fields
	return &s, nil
}

// UpsertInput is the write-side payload — covers both legacy (manual fields) and
// new (json_schema / avro / protobuf / none) modes.
type UpsertInput struct {
	SchemaType   string        // legacy: "manual" | "avro"
	SchemaFormat string        // new: "manual" | "none" | "json_schema" | "avro" | "protobuf"
	SchemaText   string        // raw schema source (for json_schema/avro/protobuf)
	AvroRaw      string        // legacy field, used only for backwards-compat callers
	Fields       []SchemaField // legacy field list (manual mode)
}

func (r *SchemaRepo) Upsert(clusterID, topicName string, in UpsertInput) (*TopicSchema, error) {
	// Derive legacy schema_type from new format if caller didn't set it.
	if in.SchemaType == "" {
		switch in.SchemaFormat {
		case SchemaFormatAvro:
			in.SchemaType = "avro"
		default:
			in.SchemaType = "manual"
		}
	}
	if in.SchemaFormat == "" {
		in.SchemaFormat = SchemaFormatManual
	}
	// For Avro, keep the legacy avro_raw column populated too.
	if in.SchemaFormat == SchemaFormatAvro && in.AvroRaw == "" {
		in.AvroRaw = in.SchemaText
	}

	tx, err := r.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() //nolint:errcheck

	var schemaID string
	const upsertQ = `
		INSERT INTO topic_schemas
		    (cluster_id, topic_name, schema_type, schema_format, schema_text, avro_raw, schema_version)
		VALUES ($1,$2,$3,$4,NULLIF($5,''),NULLIF($6,''), 1)
		ON CONFLICT (cluster_id, topic_name) DO UPDATE
		   SET schema_type    = EXCLUDED.schema_type,
		       schema_format  = EXCLUDED.schema_format,
		       schema_text    = EXCLUDED.schema_text,
		       avro_raw       = EXCLUDED.avro_raw,
		       schema_version = topic_schemas.schema_version + 1,
		       updated_at     = NOW()
		RETURNING id`
	if err := tx.QueryRow(upsertQ,
		clusterID, topicName,
		in.SchemaType, in.SchemaFormat, in.SchemaText, in.AvroRaw,
	).Scan(&schemaID); err != nil {
		return nil, fmt.Errorf("upsert schema: %w", err)
	}

	if _, err := tx.Exec(`DELETE FROM schema_fields WHERE schema_id=$1`, schemaID); err != nil {
		return nil, err
	}
	for i, f := range in.Fields {
		if f.ID == "" {
			f.ID = uuid.New().String()
		}
		if f.FieldSource == "" {
			f.FieldSource = "value"
		}
		if _, err := tx.Exec(`
			INSERT INTO schema_fields
			  (id, schema_id, name, field_type, field_source, is_required, is_primary_key, default_value, description, position)
			VALUES ($1,$2,$3,$4,$5,$6,$7,NULLIF($8,''),NULLIF($9,''),$10)`,
			f.ID, schemaID, f.Name, f.FieldType, f.FieldSource, f.IsRequired, f.IsPrimaryKey,
			f.DefaultValue, f.Description, i,
		); err != nil {
			return nil, fmt.Errorf("insert field %s: %w", f.Name, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return r.Get(clusterID, topicName)
}

func (r *SchemaRepo) getFields(schemaID string) ([]SchemaField, error) {
	rows, err := r.db.Query(`
		SELECT id, schema_id, name, field_type, COALESCE(field_source,'value'),
		       is_required, is_primary_key,
		       COALESCE(default_value,''), COALESCE(description,''), position
		FROM schema_fields WHERE schema_id=$1 ORDER BY position`, schemaID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var fields []SchemaField
	for rows.Next() {
		var f SchemaField
		if err := rows.Scan(&f.ID, &f.SchemaID, &f.Name, &f.FieldType, &f.FieldSource,
			&f.IsRequired, &f.IsPrimaryKey, &f.DefaultValue, &f.Description, &f.Position); err != nil {
			return nil, err
		}
		fields = append(fields, f)
	}
	if fields == nil {
		fields = []SchemaField{}
	}
	return fields, rows.Err()
}
