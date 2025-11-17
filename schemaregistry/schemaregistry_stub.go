//go:build noschemaregistry || minimal

/**
 * Copyright 2022 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package schemaregistry

// Client stub interface that is used when Schema Registry support is not compiled in
type Client interface{}

// NewClient returns an error indicating Schema Registry support is not compiled in
func NewClient(conf *Config) (Client, error) {
	panic("Schema Registry support not compiled in. Rebuild without -tags noschemaregistry or -tags minimal")
}

// Stub types for compatibility when Schema Registry is not compiled in

// Rule represents a data contract rule (stub)
type Rule struct{}

// RulePhase represents the rule phase (stub)
type RulePhase = int

// RuleMode represents the rule mode (stub)
type RuleMode = int

// RuleSet represents a data contract rule set (stub)
type RuleSet struct{}

// Metadata represents user-defined metadata (stub)
type Metadata struct{}

// Reference represents a schema reference (stub)
type Reference struct{}

// SchemaInfo represents basic schema information (stub)
type SchemaInfo struct{}

// SchemaMetadata represents schema metadata (stub)
type SchemaMetadata struct{}

// SubjectAndVersion represents a pair of subject and version (stub)
type SubjectAndVersion struct{}

// Compatibility represents compatibility level (stub)
type Compatibility struct{}

// ServerConfig represents server configuration (stub)
type ServerConfig struct{}

// AssociationCreateRequest represents association create request (stub)
type AssociationCreateRequest struct{}

// AssociationResponse represents association response (stub)
type AssociationResponse struct{}

// Association represents an association (stub)
type Association struct{}
