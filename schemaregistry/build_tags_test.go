//go:build !noschemaregistry && !minimal

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

import (
	"testing"
)

// TestSchemaRegistryAvailable verifies that Schema Registry is available in full build
func TestSchemaRegistryAvailable(t *testing.T) {
	// This test only runs when Schema Registry is compiled in
	// If it runs, it means the build tag configuration is working correctly

	// Try to create a mock client to verify the implementation is available
	conf := NewConfig("mock://testurl")
	client, err := NewClient(conf)

	if err != nil {
		t.Fatalf("Expected to create Schema Registry client in full build, got error: %v", err)
	}

	if client == nil {
		t.Fatal("Expected non-nil client in full build")
	}

	t.Log("Schema Registry client created successfully - feature is available")
}
