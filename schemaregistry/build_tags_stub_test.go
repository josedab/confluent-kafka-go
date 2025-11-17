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

import (
	"testing"
)

// TestSchemaRegistryPanicsWhenDisabled verifies that the stub panics when Schema Registry is not compiled in
func TestSchemaRegistryPanicsWhenDisabled(t *testing.T) {
	// This test only runs when Schema Registry is NOT compiled in (noschemaregistry or minimal tags)
	// It verifies that calling NewClient panics with an appropriate message

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("Expected NewClient to panic when Schema Registry is disabled, but it didn't")
		} else {
			// Verify panic message is helpful
			msg, ok := r.(string)
			if !ok {
				t.Fatalf("Expected panic message to be a string, got %T", r)
			}
			if msg != "Schema Registry support not compiled in. Rebuild without -tags noschemaregistry or -tags minimal" {
				t.Fatalf("Unexpected panic message: %s", msg)
			}
			t.Log("Correctly panicked with message:", msg)
		}
	}()

	conf := &Config{}
	NewClient(conf)
}
