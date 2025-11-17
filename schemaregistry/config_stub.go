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

// Config stub for when Schema Registry is not compiled in
type Config struct{}

// NewConfig creates a new Config stub
func NewConfig(url string) *Config {
	return &Config{}
}

// NewConfigWithAuthentication creates a new Config stub with authentication
func NewConfigWithAuthentication(url string, username string, password string) *Config {
	return &Config{}
}

// NewConfigWithBasicAuthentication creates a new Config stub with basic authentication
func NewConfigWithBasicAuthentication(url string, username string, password string) *Config {
	return &Config{}
}

// NewConfigWithBearerAuthentication creates a new Config stub with bearer authentication
func NewConfigWithBearerAuthentication(url, token, targetSr, identityPoolID string) *Config {
	return &Config{}
}
