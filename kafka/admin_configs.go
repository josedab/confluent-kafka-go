/**
 * Copyright 2018 Confluent Inc.
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

package kafka

import (
	"context"
	"fmt"
	"unsafe"
)

/*
#include "select_rdkafka.h"
#include <stdlib.h>
*/
import "C"

// ConfigSource represents an Apache Kafka config source
type ConfigSource int

const (
	// ConfigSourceUnknown is the default value
	ConfigSourceUnknown ConfigSource = C.RD_KAFKA_CONFIG_SOURCE_UNKNOWN_CONFIG
	// ConfigSourceDynamicTopic is dynamic topic config that is configured for a specific topic
	ConfigSourceDynamicTopic ConfigSource = C.RD_KAFKA_CONFIG_SOURCE_DYNAMIC_TOPIC_CONFIG
	// ConfigSourceDynamicBroker is dynamic broker config that is configured for a specific broker
	ConfigSourceDynamicBroker ConfigSource = C.RD_KAFKA_CONFIG_SOURCE_DYNAMIC_BROKER_CONFIG
	// ConfigSourceDynamicDefaultBroker is dynamic broker config that is configured as default for all brokers in the cluster
	ConfigSourceDynamicDefaultBroker ConfigSource = C.RD_KAFKA_CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER_CONFIG
	// ConfigSourceStaticBroker is static broker config provided as broker properties at startup (e.g. from server.properties file)
	ConfigSourceStaticBroker ConfigSource = C.RD_KAFKA_CONFIG_SOURCE_STATIC_BROKER_CONFIG
	// ConfigSourceDefault is built-in default configuration for configs that have a default value
	ConfigSourceDefault ConfigSource = C.RD_KAFKA_CONFIG_SOURCE_DEFAULT_CONFIG
	// ConfigSourceGroup is group config that is configured for a specific group
	ConfigSourceGroup ConfigSource = C.RD_KAFKA_CONFIG_SOURCE_GROUP_CONFIG
)

// String returns the human-readable representation of a ConfigSource type
func (t ConfigSource) String() string {
	return C.GoString(C.rd_kafka_ConfigSource_name(C.rd_kafka_ConfigSource_t(t)))
}

// ConfigResource holds parameters for altering an Apache Kafka configuration resource
type ConfigResource struct {
	// Type of resource to set.
	Type ResourceType
	// Name of resource to set.
	Name string
	// Config entries to set.
	// Configuration updates are atomic, any configuration property not provided
	// here will be reverted (by the broker) to its default value.
	// Use DescribeConfigs to retrieve the list of current configuration entry values.
	Config []ConfigEntry
}

// String returns a human-readable representation of a ConfigResource
func (c ConfigResource) String() string {
	return fmt.Sprintf("Resource(%s, %s)", c.Type, c.Name)
}

// AlterOperation specifies the operation to perform on the ConfigEntry.
// Currently only AlterOperationSet.
type AlterOperation int

const (
	// AlterOperationSet sets/overwrites the configuration setting.
	AlterOperationSet = iota
)

// String returns the human-readable representation of an AlterOperation
func (o AlterOperation) String() string {
	switch o {
	case AlterOperationSet:
		return "Set"
	default:
		return fmt.Sprintf("Unknown%d?", int(o))
	}
}

// AlterConfigOpType specifies the operation to perform
// on the ConfigEntry for IncrementalAlterConfig
type AlterConfigOpType int

const (
	// AlterConfigOpTypeSet sets/overwrites the configuration
	// setting.
	AlterConfigOpTypeSet AlterConfigOpType = C.RD_KAFKA_ALTER_CONFIG_OP_TYPE_SET
	// AlterConfigOpTypeDelete sets the configuration setting
	// to default or NULL.
	AlterConfigOpTypeDelete AlterConfigOpType = C.RD_KAFKA_ALTER_CONFIG_OP_TYPE_DELETE
	// AlterConfigOpTypeAppend appends the value to existing
	// configuration settings.
	AlterConfigOpTypeAppend AlterConfigOpType = C.RD_KAFKA_ALTER_CONFIG_OP_TYPE_APPEND
	// AlterConfigOpTypeSubtract subtracts the value from
	// existing configuration settings.
	AlterConfigOpTypeSubtract AlterConfigOpType = C.RD_KAFKA_ALTER_CONFIG_OP_TYPE_SUBTRACT
)

// String returns the human-readable representation of an AlterOperation
func (o AlterConfigOpType) String() string {
	switch o {
	case AlterConfigOpTypeSet:
		return "Set"
	case AlterConfigOpTypeDelete:
		return "Delete"
	case AlterConfigOpTypeAppend:
		return "Append"
	case AlterConfigOpTypeSubtract:
		return "Subtract"
	default:
		return fmt.Sprintf("Unknown %d", int(o))
	}
}

// ConfigEntry holds parameters for altering a resource's configuration.
type ConfigEntry struct {
	// Name of configuration entry, e.g., topic configuration property name.
	Name string
	// Value of configuration entry.
	Value string
	// Deprecated: Operation to perform on the entry.
	Operation AlterOperation
	// Operation to perform on the entry incrementally.
	IncrementalOperation AlterConfigOpType
}

// StringMapToConfigEntries creates a new map of ConfigEntry objects from the
// provided string map. The AlterOperation is set on each created entry.
func StringMapToConfigEntries(stringMap map[string]string, operation AlterOperation) []ConfigEntry {
	var ceList []ConfigEntry

	for k, v := range stringMap {
		ceList = append(ceList, ConfigEntry{Name: k, Value: v, Operation: operation})
	}

	return ceList
}

// StringMapToIncrementalConfigEntries creates a new map of ConfigEntry objects from the
// provided string map an operation map. The AlterConfigOpType is set on each created entry.
func StringMapToIncrementalConfigEntries(stringMap map[string]string,
	operationMap map[string]AlterConfigOpType) []ConfigEntry {
	var ceList []ConfigEntry

	for k, v := range stringMap {
		ceList = append(ceList, ConfigEntry{Name: k, Value: v, IncrementalOperation: operationMap[k]})
	}

	return ceList
}

// String returns a human-readable representation of a ConfigEntry.
func (c ConfigEntry) String() string {
	return fmt.Sprintf("%v %s=\"%s\"", c.Operation, c.Name, c.Value)
}

// ConfigEntryResult contains the result of a single configuration entry from a
// DescribeConfigs request.
type ConfigEntryResult struct {
	// Name of configuration entry, e.g., topic configuration property name.
	Name string
	// Value of configuration entry.
	Value string
	// Source indicates the configuration source.
	Source ConfigSource
	// IsReadOnly indicates whether the configuration entry can be altered.
	IsReadOnly bool
	// IsDefault indicates whether the value is at its default.
	IsDefault bool
	// IsSensitive indicates whether the configuration entry contains sensitive information, in which case the value will be unset.
	IsSensitive bool
	// IsSynonym indicates whether the configuration entry is a synonym for another configuration property.
	IsSynonym bool
	// Synonyms contains a map of configuration entries that are synonyms to this configuration entry.
	Synonyms map[string]ConfigEntryResult
}

// String returns a human-readable representation of a ConfigEntryResult.
func (c ConfigEntryResult) String() string {
	return fmt.Sprintf("%s=\"%s\"", c.Name, c.Value)
}

// setFromC sets up a ConfigEntryResult from a C ConfigEntry
func configEntryResultFromC(cEntry *C.rd_kafka_ConfigEntry_t) (entry ConfigEntryResult) {
	entry.Name = C.GoString(C.rd_kafka_ConfigEntry_name(cEntry))
	cValue := C.rd_kafka_ConfigEntry_value(cEntry)
	if cValue != nil {
		entry.Value = C.GoString(cValue)
	}
	entry.Source = ConfigSource(C.rd_kafka_ConfigEntry_source(cEntry))
	entry.IsReadOnly = cint2bool(C.rd_kafka_ConfigEntry_is_read_only(cEntry))
	entry.IsDefault = cint2bool(C.rd_kafka_ConfigEntry_is_default(cEntry))
	entry.IsSensitive = cint2bool(C.rd_kafka_ConfigEntry_is_sensitive(cEntry))
	entry.IsSynonym = cint2bool(C.rd_kafka_ConfigEntry_is_synonym(cEntry))

	var cSynCnt C.size_t
	cSyns := C.rd_kafka_ConfigEntry_synonyms(cEntry, &cSynCnt)
	if cSynCnt > 0 {
		entry.Synonyms = make(map[string]ConfigEntryResult)
	}

	for si := 0; si < int(cSynCnt); si++ {
		cSyn := C.ConfigEntry_by_idx(cSyns, cSynCnt, C.size_t(si))
		Syn := configEntryResultFromC(cSyn)
		entry.Synonyms[Syn.Name] = Syn
	}

	return entry
}

// ConfigResourceResult provides the result for a resource from a AlterConfigs or
// DescribeConfigs request.
type ConfigResourceResult struct {
	// Type of returned result resource.
	Type ResourceType
	// Name of returned result resource.
	Name string
	// Error, if any, of returned result resource.
	Error Error
	// Config entries, if any, of returned result resource.
	Config map[string]ConfigEntryResult
}

// String returns a human-readable representation of a ConfigResourceResult.
func (c ConfigResourceResult) String() string {
	if c.Error.Code() != 0 {
		return fmt.Sprintf("ResourceResult(%s, %s, \"%v\")", c.Type, c.Name, c.Error)

	}
	return fmt.Sprintf("ResourceResult(%s, %s, %d config(s))", c.Type, c.Name, len(c.Config))
}

// cConfigResourceToResult converts a C ConfigResource result array to Go ConfigResourceResult
func (a *AdminClient) cConfigResourceToResult(cRes **C.rd_kafka_ConfigResource_t, cCnt C.size_t) (result []ConfigResourceResult, err error) {
	result = make([]ConfigResourceResult, int(cCnt))

	for i := 0; i < int(cCnt); i++ {
		cRes := C.ConfigResource_by_idx(cRes, cCnt, C.size_t(i))
		result[i].Type = ResourceType(C.rd_kafka_ConfigResource_type(cRes))
		result[i].Name = C.GoString(C.rd_kafka_ConfigResource_name(cRes))
		result[i].Error = newErrorFromCString(
			C.rd_kafka_ConfigResource_error(cRes),
			C.rd_kafka_ConfigResource_error_string(cRes))
		var cConfigCnt C.size_t
		cConfigs := C.rd_kafka_ConfigResource_configs(cRes, &cConfigCnt)
		if cConfigCnt > 0 {
			result[i].Config = make(map[string]ConfigEntryResult)
		}
		for ci := 0; ci < int(cConfigCnt); ci++ {
			cEntry := C.ConfigEntry_by_idx(cConfigs, cConfigCnt, C.size_t(ci))
			entry := configEntryResultFromC(cEntry)
			result[i].Config[entry.Name] = entry
		}
	}

	return result, nil
}

// AlterConfigs alters/updates cluster resource configuration.
//
// Updates are not transactional so they may succeed for a subset
// of the provided resources while others fail.
// The configuration for a particular resource is updated atomically,
// replacing values using the provided ConfigEntrys and reverting
// unspecified ConfigEntrys to their default values.
//
// Requires broker version >=0.11.0.0
//
// AlterConfigs will replace all existing configuration for
// the provided resources with the new configuration given,
// reverting all other configuration to their default values.
//
// Multiple resources and resource types may be set, but at most one
// resource of type ResourceBroker is allowed per call since these
// resource requests must be sent to the broker specified in the resource.
// Deprecated: AlterConfigs is deprecated in favour of IncrementalAlterConfigs
func (a *AdminClient) AlterConfigs(ctx context.Context, resources []ConfigResource, options ...AlterConfigsAdminOption) (result []ConfigResourceResult, err error) {
	err = a.verifyClient()
	if err != nil {
		return nil, err
	}

	cRes := make([]*C.rd_kafka_ConfigResource_t, len(resources))

	cErrstrSize := C.size_t(512)
	cErrstr := (*C.char)(C.malloc(cErrstrSize))
	defer C.free(unsafe.Pointer(cErrstr))

	// Convert Go ConfigResources to C ConfigResources
	for i, res := range resources {
		cRes[i] = C.rd_kafka_ConfigResource_new(
			C.rd_kafka_ResourceType_t(res.Type), C.CString(res.Name))
		if cRes[i] == nil {
			return nil, newErrorFromString(ErrInvalidArg,
				fmt.Sprintf("Invalid arguments for resource %v", res))
		}

		defer C.rd_kafka_ConfigResource_destroy(cRes[i])

		for _, entry := range res.Config {
			var cErr C.rd_kafka_resp_err_t
			switch entry.Operation {
			case AlterOperationSet:
				cErr = C.rd_kafka_ConfigResource_set_config(
					cRes[i], C.CString(entry.Name), C.CString(entry.Value))
			default:
				panic(fmt.Sprintf("Invalid ConfigEntry.Operation: %v", entry.Operation))
			}

			if cErr != 0 {
				return nil,
					newCErrorFromString(cErr,
						fmt.Sprintf("Failed to add configuration %s: %s",
							entry, C.GoString(C.rd_kafka_err2str(cErr))))
			}
		}
	}

	// Convert Go AdminOptions (if any) to C AdminOptions
	genericOptions := make([]AdminOption, len(options))
	for i := range options {
		genericOptions[i] = options[i]
	}
	cOptions, err := adminOptionsSetup(a.handle, C.RD_KAFKA_ADMIN_OP_ALTERCONFIGS, genericOptions)
	if err != nil {
		return nil, err
	}
	defer C.rd_kafka_AdminOptions_destroy(cOptions)

	// Create temporary queue for async operation
	cQueue := C.rd_kafka_queue_new(a.handle.rk)
	defer C.rd_kafka_queue_destroy(cQueue)

	// Asynchronous call
	C.rd_kafka_AlterConfigs(
		a.handle.rk,
		(**C.rd_kafka_ConfigResource_t)(&cRes[0]),
		C.size_t(len(cRes)),
		cOptions,
		cQueue)

	// Wait for result, error or context timeout
	rkev, err := a.waitResult(ctx, cQueue, C.RD_KAFKA_EVENT_ALTERCONFIGS_RESULT)
	if err != nil {
		return nil, err
	}
	defer C.rd_kafka_event_destroy(rkev)

	cResult := C.rd_kafka_event_AlterConfigs_result(rkev)

	// Convert results from C to Go
	var cCnt C.size_t
	cResults := C.rd_kafka_AlterConfigs_result_resources(cResult, &cCnt)

	return a.cConfigResourceToResult(cResults, cCnt)
}

// IncrementalAlterConfigs alters/updates cluster resource configuration.
//
// Updates are not transactional so they may succeed for a subset
// of the provided resources while others fail.
// The configuration for a particular resource is updated atomically,
// incrementally updating the provided ConfigEntrys while reverting
// unspecified ConfigEntrys to their default values.
//
// Requires broker version >=2.3.0
//
// IncrementalAlterConfigs will incrementally update the configuration
// for the provided resources with the new configuration given.
//
// Multiple resources and resource types may be set, but at most one
// resource of type ResourceBroker is allowed per call since these
// resource requests must be sent to the broker specified in the resource.
func (a *AdminClient) IncrementalAlterConfigs(ctx context.Context, resources []ConfigResource, options ...AlterConfigsAdminOption) (result []ConfigResourceResult, err error) {
	err = a.verifyClient()
	if err != nil {
		return nil, err
	}

	cRes := make([]*C.rd_kafka_ConfigResource_t, len(resources))

	cErrstrSize := C.size_t(512)
	cErrstr := (*C.char)(C.malloc(cErrstrSize))
	defer C.free(unsafe.Pointer(cErrstr))

	// Convert Go ConfigResources to C ConfigResources
	for i, res := range resources {
		cName := C.CString(res.Name)
		defer C.free(unsafe.Pointer(cName))
		cRes[i] = C.rd_kafka_ConfigResource_new(
			C.rd_kafka_ResourceType_t(res.Type), cName)
		if cRes[i] == nil {
			return nil, newErrorFromString(ErrInvalidArg,
				fmt.Sprintf("Invalid arguments for resource %v", res))
		}

		defer C.rd_kafka_ConfigResource_destroy(cRes[i])

		for _, entry := range res.Config {
			cName := C.CString(entry.Name)
			defer C.free(unsafe.Pointer(cName))
			cValue := C.CString(entry.Value)
			defer C.free(unsafe.Pointer(cValue))
			cError := C.rd_kafka_ConfigResource_add_incremental_config(
				cRes[i], cName,
				C.rd_kafka_AlterConfigOpType_t(entry.IncrementalOperation),
				cValue)

			if cError != nil {
				err := newErrorFromCErrorDestroy(cError)
				return nil, err
			}
		}
	}

	// Convert Go AdminOptions (if any) to C AdminOptions
	genericOptions := make([]AdminOption, len(options))
	for i := range options {
		genericOptions[i] = options[i]
	}
	cOptions, err := adminOptionsSetup(a.handle, C.RD_KAFKA_ADMIN_OP_INCREMENTALALTERCONFIGS, genericOptions)
	if err != nil {
		return nil, err
	}
	defer C.rd_kafka_AdminOptions_destroy(cOptions)

	// Create temporary queue for async operation
	cQueue := C.rd_kafka_queue_new(a.handle.rk)
	defer C.rd_kafka_queue_destroy(cQueue)

	// Asynchronous call
	C.rd_kafka_IncrementalAlterConfigs(
		a.handle.rk,
		(**C.rd_kafka_ConfigResource_t)(&cRes[0]),
		C.size_t(len(cRes)),
		cOptions,
		cQueue)

	// Wait for result, error or context timeout
	rkev, err := a.waitResult(ctx, cQueue, C.RD_KAFKA_EVENT_INCREMENTALALTERCONFIGS_RESULT)
	if err != nil {
		return nil, err
	}
	defer C.rd_kafka_event_destroy(rkev)

	cResult := C.rd_kafka_event_IncrementalAlterConfigs_result(rkev)

	// Convert results from C to Go
	var cCnt C.size_t
	cResults := C.rd_kafka_IncrementalAlterConfigs_result_resources(cResult, &cCnt)

	return a.cConfigResourceToResult(cResults, cCnt)
}

// DescribeConfigs retrieves configuration for cluster resources.
//
// The returned configuration includes default values, use
// ConfigEntryResult.IsDefault or ConfigEntryResult.Source to distinguish
// default values from manually configured settings.
//
// The value of config entries where .IsSensitive is true
// will always be nil to avoid disclosing sensitive
// information, such as security settings.
//
// Configuration entries where .IsReadOnly is true can't be modified
// (with AlterConfigs).
//
// Synonym configuration entries are returned if the broker supports
// it (broker version >= 1.1.0). See .Synonyms.
//
// Requires broker version >=0.11.0.0
//
// Multiple resources and resource types may be requested, but at most
// one resource of type ResourceBroker is allowed per call
// since these resource requests must be sent to the broker specified
// in the resource.
func (a *AdminClient) DescribeConfigs(ctx context.Context, resources []ConfigResource, options ...DescribeConfigsAdminOption) (result []ConfigResourceResult, err error) {
	err = a.verifyClient()
	if err != nil {
		return nil, err
	}

	cRes := make([]*C.rd_kafka_ConfigResource_t, len(resources))

	cErrstrSize := C.size_t(512)
	cErrstr := (*C.char)(C.malloc(cErrstrSize))
	defer C.free(unsafe.Pointer(cErrstr))

	// Convert Go ConfigResources to C ConfigResources
	for i, res := range resources {
		cRes[i] = C.rd_kafka_ConfigResource_new(
			C.rd_kafka_ResourceType_t(res.Type), C.CString(res.Name))
		if cRes[i] == nil {
			return nil, newErrorFromString(ErrInvalidArg,
				fmt.Sprintf("Invalid arguments for resource %v", res))
		}

		defer C.rd_kafka_ConfigResource_destroy(cRes[i])
	}

	// Convert Go AdminOptions (if any) to C AdminOptions
	genericOptions := make([]AdminOption, len(options))
	for i := range options {
		genericOptions[i] = options[i]
	}
	cOptions, err := adminOptionsSetup(a.handle, C.RD_KAFKA_ADMIN_OP_DESCRIBECONFIGS, genericOptions)
	if err != nil {
		return nil, err
	}
	defer C.rd_kafka_AdminOptions_destroy(cOptions)

	// Create temporary queue for async operation
	cQueue := C.rd_kafka_queue_new(a.handle.rk)
	defer C.rd_kafka_queue_destroy(cQueue)

	// Asynchronous call
	C.rd_kafka_DescribeConfigs(
		a.handle.rk,
		(**C.rd_kafka_ConfigResource_t)(&cRes[0]),
		C.size_t(len(cRes)),
		cOptions,
		cQueue)

	// Wait for result, error or context timeout
	rkev, err := a.waitResult(ctx, cQueue, C.RD_KAFKA_EVENT_DESCRIBECONFIGS_RESULT)
	if err != nil {
		return nil, err
	}
	defer C.rd_kafka_event_destroy(rkev)

	cResult := C.rd_kafka_event_DescribeConfigs_result(rkev)

	// Convert results from C to Go
	var cCnt C.size_t
	cResults := C.rd_kafka_DescribeConfigs_result_resources(cResult, &cCnt)

	return a.cConfigResourceToResult(cResults, cCnt)
}
