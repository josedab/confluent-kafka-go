/**
 * Copyright 2025 Confluent Inc.
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

//go:build noschemaregistry || minimal
// +build noschemaregistry minimal

package schemaregistry

import "errors"

const schemaRegistryEnabled = false

// Stub implementation of NewClient when Schema Registry is disabled via build tags
// This allows compilation but provides clear runtime errors if used.
//
// To enable Schema Registry, rebuild without -tags noschemaregistry or -tags minimal:
//   go build ./...
func NewClient(config *Config) (Client, error) {
	return nil, errors.New("Schema Registry support not compiled in. " +
		"Rebuild without -tags noschemaregistry or -tags minimal to enable Schema Registry. " +
		"See https://github.com/confluentinc/confluent-kafka-go/blob/master/README.md#build-options")
}

// NewSerdeClient stub for minimal builds
func NewSerdeClient(config *Config) (*SerdeClient, error) {
	return nil, errors.New("Schema Registry support not compiled in. " +
		"Rebuild without -tags noschemaregistry or -tags minimal to enable Schema Registry. " +
		"See https://github.com/confluentinc/confluent-kafka-go/blob/master/README.md#build-options")
}
