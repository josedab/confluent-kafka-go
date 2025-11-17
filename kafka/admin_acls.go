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
	"strings"
	"unsafe"
)

/*
#include "select_rdkafka.h"
#include <stdlib.h>
*/
import "C"

// ResourceType represents an Apache Kafka resource type
type ResourceType int

const (
	// ResourceUnknown - Unknown
	ResourceUnknown ResourceType = C.RD_KAFKA_RESOURCE_UNKNOWN
	// ResourceAny - match any resource type (DescribeConfigs)
	ResourceAny ResourceType = C.RD_KAFKA_RESOURCE_ANY
	// ResourceTopic - Topic
	ResourceTopic ResourceType = C.RD_KAFKA_RESOURCE_TOPIC
	// ResourceGroup - Group
	ResourceGroup ResourceType = C.RD_KAFKA_RESOURCE_GROUP
	// ResourceBroker - Broker
	ResourceBroker ResourceType = C.RD_KAFKA_RESOURCE_BROKER
)

// String returns the human-readable representation of a ResourceType
func (t ResourceType) String() string {
	return C.GoString(C.rd_kafka_ResourceType_name(C.rd_kafka_ResourceType_t(t)))
}

// ResourceTypeFromString translates a resource type name/string to
// a ResourceType value.
func ResourceTypeFromString(typeString string) (ResourceType, error) {
	switch strings.ToUpper(typeString) {
	case "ANY":
		return ResourceAny, nil
	case "TOPIC":
		return ResourceTopic, nil
	case "GROUP":
		return ResourceGroup, nil
	case "BROKER":
		return ResourceBroker, nil
	default:
		return ResourceUnknown, NewError(ErrInvalidArg, "Unknown resource type", false)
	}
}

// ResourcePatternType enumerates the different types of Kafka resource patterns.
type ResourcePatternType int

const (
	// ResourcePatternTypeUnknown is a resource pattern type not known or not set.
	ResourcePatternTypeUnknown ResourcePatternType = C.RD_KAFKA_RESOURCE_PATTERN_UNKNOWN
	// ResourcePatternTypeAny matches any resource, used for lookups.
	ResourcePatternTypeAny ResourcePatternType = C.RD_KAFKA_RESOURCE_PATTERN_ANY
	// ResourcePatternTypeMatch will perform pattern matching
	ResourcePatternTypeMatch ResourcePatternType = C.RD_KAFKA_RESOURCE_PATTERN_MATCH
	// ResourcePatternTypeLiteral matches a literal resource name
	ResourcePatternTypeLiteral ResourcePatternType = C.RD_KAFKA_RESOURCE_PATTERN_LITERAL
	// ResourcePatternTypePrefixed matches a prefixed resource name
	ResourcePatternTypePrefixed ResourcePatternType = C.RD_KAFKA_RESOURCE_PATTERN_PREFIXED
)

// String returns the human-readable representation of a ResourcePatternType
func (t ResourcePatternType) String() string {
	return C.GoString(C.rd_kafka_ResourcePatternType_name(C.rd_kafka_ResourcePatternType_t(t)))
}

// ResourcePatternTypeFromString translates a resource pattern type name to
// a ResourcePatternType value.
func ResourcePatternTypeFromString(patternTypeString string) (ResourcePatternType, error) {
	switch strings.ToUpper(patternTypeString) {
	case "ANY":
		return ResourcePatternTypeAny, nil
	case "MATCH":
		return ResourcePatternTypeMatch, nil
	case "LITERAL":
		return ResourcePatternTypeLiteral, nil
	case "PREFIXED":
		return ResourcePatternTypePrefixed, nil
	default:
		return ResourcePatternTypeUnknown, NewError(ErrInvalidArg, "Unknown resource pattern type", false)
	}
}

// ACLOperation enumerates the different types of ACL operation.
type ACLOperation int

const (
	// ACLOperationUnknown represents an unknown or unset operation
	ACLOperationUnknown ACLOperation = C.RD_KAFKA_ACL_OPERATION_UNKNOWN
	// ACLOperationAny in a filter, matches any ACLOperation
	ACLOperationAny ACLOperation = C.RD_KAFKA_ACL_OPERATION_ANY
	// ACLOperationAll represents all the operations
	ACLOperationAll ACLOperation = C.RD_KAFKA_ACL_OPERATION_ALL
	// ACLOperationRead a read operation
	ACLOperationRead ACLOperation = C.RD_KAFKA_ACL_OPERATION_READ
	// ACLOperationWrite represents a write operation
	ACLOperationWrite ACLOperation = C.RD_KAFKA_ACL_OPERATION_WRITE
	// ACLOperationCreate represents a create operation
	ACLOperationCreate ACLOperation = C.RD_KAFKA_ACL_OPERATION_CREATE
	// ACLOperationDelete represents a delete operation
	ACLOperationDelete ACLOperation = C.RD_KAFKA_ACL_OPERATION_DELETE
	// ACLOperationAlter represents an alter operation
	ACLOperationAlter ACLOperation = C.RD_KAFKA_ACL_OPERATION_ALTER
	// ACLOperationDescribe represents a describe operation
	ACLOperationDescribe ACLOperation = C.RD_KAFKA_ACL_OPERATION_DESCRIBE
	// ACLOperationClusterAction represents a cluster action operation
	ACLOperationClusterAction ACLOperation = C.RD_KAFKA_ACL_OPERATION_CLUSTER_ACTION
	// ACLOperationDescribeConfigs represents a describe configs operation
	ACLOperationDescribeConfigs ACLOperation = C.RD_KAFKA_ACL_OPERATION_DESCRIBE_CONFIGS
	// ACLOperationAlterConfigs represents an alter configs operation
	ACLOperationAlterConfigs ACLOperation = C.RD_KAFKA_ACL_OPERATION_ALTER_CONFIGS
	// ACLOperationIdempotentWrite represents an idempotent write operation
	ACLOperationIdempotentWrite ACLOperation = C.RD_KAFKA_ACL_OPERATION_IDEMPOTENT_WRITE
)

// String returns the human-readable representation of an ACLOperation
func (o ACLOperation) String() string {
	return C.GoString(C.rd_kafka_AclOperation_name(C.rd_kafka_AclOperation_t(o)))
}

// ACLOperationFromString translates a ACL operation name to
// a ACLOperation value.
func ACLOperationFromString(aclOperationString string) (ACLOperation, error) {
	switch strings.ToUpper(aclOperationString) {
	case "ANY":
		return ACLOperationAny, nil
	case "ALL":
		return ACLOperationAll, nil
	case "READ":
		return ACLOperationRead, nil
	case "WRITE":
		return ACLOperationWrite, nil
	case "CREATE":
		return ACLOperationCreate, nil
	case "DELETE":
		return ACLOperationDelete, nil
	case "ALTER":
		return ACLOperationAlter, nil
	case "DESCRIBE":
		return ACLOperationDescribe, nil
	case "CLUSTER_ACTION":
		return ACLOperationClusterAction, nil
	case "DESCRIBE_CONFIGS":
		return ACLOperationDescribeConfigs, nil
	case "ALTER_CONFIGS":
		return ACLOperationAlterConfigs, nil
	case "IDEMPOTENT_WRITE":
		return ACLOperationIdempotentWrite, nil
	default:
		return ACLOperationUnknown, NewError(ErrInvalidArg, "Unknown ACL operation", false)
	}
}

// ACLPermissionType enumerates the different types of ACL permission types.
type ACLPermissionType int

const (
	// ACLPermissionTypeUnknown represents an unknown ACLPermissionType
	ACLPermissionTypeUnknown ACLPermissionType = C.RD_KAFKA_ACL_PERMISSION_TYPE_UNKNOWN
	// ACLPermissionTypeAny in a filter, matches any ACLPermissionType
	ACLPermissionTypeAny ACLPermissionType = C.RD_KAFKA_ACL_PERMISSION_TYPE_ANY
	// ACLPermissionTypeDeny disallows access
	ACLPermissionTypeDeny ACLPermissionType = C.RD_KAFKA_ACL_PERMISSION_TYPE_DENY
	// ACLPermissionTypeAllow grants access
	ACLPermissionTypeAllow ACLPermissionType = C.RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW
)

// String returns the human-readable representation of an ACLPermissionType
func (o ACLPermissionType) String() string {
	return C.GoString(C.rd_kafka_AclPermissionType_name(C.rd_kafka_AclPermissionType_t(o)))
}

// ACLPermissionTypeFromString translates a ACL permission type name to
// a ACLPermissionType value.
func ACLPermissionTypeFromString(aclPermissionTypeString string) (ACLPermissionType, error) {
	switch strings.ToUpper(aclPermissionTypeString) {
	case "ANY":
		return ACLPermissionTypeAny, nil
	case "DENY":
		return ACLPermissionTypeDeny, nil
	case "ALLOW":
		return ACLPermissionTypeAllow, nil
	default:
		return ACLPermissionTypeUnknown, NewError(ErrInvalidArg, "Unknown ACL permission type", false)
	}
}

// ACLBinding specifies the operation and permission type for a specific principal
// over one or more resources of the same type. Used by `AdminClient.CreateACLs`,
// returned by `AdminClient.DescribeACLs` and `AdminClient.DeleteACLs`.
type ACLBinding struct {
	Type ResourceType // The resource type.
	// The resource name, which depends on the resource type.
	// For ResourceBroker the resource name is the broker id.
	Name                string
	ResourcePatternType ResourcePatternType // The resource pattern, relative to the name.
	Principal           string              // The principal this ACLBinding refers to.
	Host                string              // The host that the call is allowed to come from.
	Operation           ACLOperation        // The operation/s specified by this binding.
	PermissionType      ACLPermissionType   // The permission type for the specified operation.
}

// ACLBindingFilter specifies a filter used to return a list of ACL bindings matching some or all of its attributes.
// Used by `AdminClient.DescribeACLs` and `AdminClient.DeleteACLs`.
type ACLBindingFilter = ACLBinding

// ACLBindings is a slice of ACLBinding that also implements
// the sort interface
type ACLBindings []ACLBinding

// ACLBindingFilters is a slice of ACLBindingFilter that also implements
// the sort interface
type ACLBindingFilters []ACLBindingFilter

func (a ACLBindings) Len() int {
	return len(a)
}

func (a ACLBindings) Less(i, j int) bool {
	if a[i].Type != a[j].Type {
		return a[i].Type < a[j].Type
	}
	if a[i].Name != a[j].Name {
		return a[i].Name < a[j].Name
	}
	if a[i].ResourcePatternType != a[j].ResourcePatternType {
		return a[i].ResourcePatternType < a[j].ResourcePatternType
	}
	if a[i].Principal != a[j].Principal {
		return a[i].Principal < a[j].Principal
	}
	if a[i].Host != a[j].Host {
		return a[i].Host < a[j].Host
	}
	if a[i].Operation != a[j].Operation {
		return a[i].Operation < a[j].Operation
	}
	if a[i].PermissionType != a[j].PermissionType {
		return a[i].PermissionType < a[j].PermissionType
	}
	return true
}

func (a ACLBindings) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

// CreateACLResult provides create ACL error information.
type CreateACLResult struct {
	// Error, if any, of result. Check with `Error.Code() != ErrNoError`.
	Error Error
}

// DescribeACLsResult provides describe ACLs result or error information.
type DescribeACLsResult struct {
	// Slice of ACL bindings matching the provided filter
	ACLBindings ACLBindings
	// Error, if any, of result. Check with `Error.Code() != ErrNoError`.
	Error Error
}

// DeleteACLsResult provides delete ACLs result or error information.
type DeleteACLsResult = DescribeACLsResult

// aclBindingToC converts a Go ACLBinding struct to a C rd_kafka_AclBinding_t
func (a *AdminClient) aclBindingToC(aclBinding *ACLBinding, cErrstr *C.char, cErrstrSize C.size_t) (result *C.rd_kafka_AclBinding_t, err error) {
	var cName, cPrincipal, cHost *C.char
	cName, cPrincipal, cHost = nil, nil, nil
	if len(aclBinding.Name) > 0 {
		cName = C.CString(aclBinding.Name)
		defer C.free(unsafe.Pointer(cName))
	}
	if len(aclBinding.Principal) > 0 {
		cPrincipal = C.CString(aclBinding.Principal)
		defer C.free(unsafe.Pointer(cPrincipal))
	}
	if len(aclBinding.Host) > 0 {
		cHost = C.CString(aclBinding.Host)
		defer C.free(unsafe.Pointer(cHost))
	}

	result = C.rd_kafka_AclBinding_new(
		C.rd_kafka_ResourceType_t(aclBinding.Type),
		cName,
		C.rd_kafka_ResourcePatternType_t(aclBinding.ResourcePatternType),
		cPrincipal,
		cHost,
		C.rd_kafka_AclOperation_t(aclBinding.Operation),
		C.rd_kafka_AclPermissionType_t(aclBinding.PermissionType),
		cErrstr,
		cErrstrSize,
	)
	if result == nil {
		err = newErrorFromString(ErrInvalidArg,
			fmt.Sprintf("Invalid arguments for ACL binding %v: %v", aclBinding, C.GoString(cErrstr)))
	}
	return
}

// aclBindingFilterToC converts a Go ACLBindingFilter struct to a C rd_kafka_AclBindingFilter_t
func (a *AdminClient) aclBindingFilterToC(aclBindingFilter *ACLBindingFilter, cErrstr *C.char, cErrstrSize C.size_t) (result *C.rd_kafka_AclBindingFilter_t, err error) {
	var cName, cPrincipal, cHost *C.char
	cName, cPrincipal, cHost = nil, nil, nil
	if len(aclBindingFilter.Name) > 0 {
		cName = C.CString(aclBindingFilter.Name)
		defer C.free(unsafe.Pointer(cName))
	}
	if len(aclBindingFilter.Principal) > 0 {
		cPrincipal = C.CString(aclBindingFilter.Principal)
		defer C.free(unsafe.Pointer(cPrincipal))
	}
	if len(aclBindingFilter.Host) > 0 {
		cHost = C.CString(aclBindingFilter.Host)
		defer C.free(unsafe.Pointer(cHost))
	}

	result = C.rd_kafka_AclBindingFilter_new(
		C.rd_kafka_ResourceType_t(aclBindingFilter.Type),
		cName,
		C.rd_kafka_ResourcePatternType_t(aclBindingFilter.ResourcePatternType),
		cPrincipal,
		cHost,
		C.rd_kafka_AclOperation_t(aclBindingFilter.Operation),
		C.rd_kafka_AclPermissionType_t(aclBindingFilter.PermissionType),
		cErrstr,
		cErrstrSize,
	)
	if result == nil {
		err = newErrorFromString(ErrInvalidArg,
			fmt.Sprintf("Invalid arguments for ACL binding filter %v: %v", aclBindingFilter, C.GoString(cErrstr)))
	}
	return
}

// cToACLBinding converts a C rd_kafka_AclBinding_t to Go ACLBinding
func (a *AdminClient) cToACLBinding(cACLBinding *C.rd_kafka_AclBinding_t) ACLBinding {
	return ACLBinding{
		ResourceType(C.rd_kafka_AclBinding_restype(cACLBinding)),
		C.GoString(C.rd_kafka_AclBinding_name(cACLBinding)),
		ResourcePatternType(C.rd_kafka_AclBinding_resource_pattern_type(cACLBinding)),
		C.GoString(C.rd_kafka_AclBinding_principal(cACLBinding)),
		C.GoString(C.rd_kafka_AclBinding_host(cACLBinding)),
		ACLOperation(C.rd_kafka_AclBinding_operation(cACLBinding)),
		ACLPermissionType(C.rd_kafka_AclBinding_permission_type(cACLBinding)),
	}
}

// cToACLBindings converts a C rd_kafka_AclBinding_t list to Go ACLBindings
func (a *AdminClient) cToACLBindings(cACLBindings **C.rd_kafka_AclBinding_t, aclCnt C.size_t) (result ACLBindings) {
	result = make(ACLBindings, aclCnt)
	for i := uint(0); i < uint(aclCnt); i++ {
		cACLBinding := C.AclBinding_by_idx(cACLBindings, aclCnt, C.size_t(i))
		if cACLBinding == nil {
			panic("AclBinding_by_idx must not return nil")
		}
		result[i] = a.cToACLBinding(cACLBinding)
	}
	return
}

// cToCreateACLResults converts a C acl_result_t array to Go CreateACLResult list.
func (a *AdminClient) cToCreateACLResults(cCreateAclsRes **C.rd_kafka_acl_result_t, aclCnt C.size_t) (result []CreateACLResult, err error) {
	result = make([]CreateACLResult, uint(aclCnt))

	for i := uint(0); i < uint(aclCnt); i++ {
		cCreateACLRes := C.acl_result_by_idx(cCreateAclsRes, aclCnt, C.size_t(i))
		if cCreateACLRes != nil {
			cCreateACLError := C.rd_kafka_acl_result_error(cCreateACLRes)
			result[i].Error = newErrorFromCError(cCreateACLError)
		}
	}

	return result, nil
}

// cToDescribeACLsResult converts a C rd_kafka_event_t to a Go DescribeAclsResult struct.
func (a *AdminClient) cToDescribeACLsResult(rkev *C.rd_kafka_event_t) (result *DescribeACLsResult) {
	result = &DescribeACLsResult{}
	err := C.rd_kafka_event_error(rkev)
	errCode := ErrorCode(err)
	errStr := C.rd_kafka_event_error_string(rkev)

	var cResultACLsCount C.size_t
	cResult := C.rd_kafka_event_DescribeAcls_result(rkev)
	cResultACLs := C.rd_kafka_DescribeAcls_result_acls(cResult, &cResultACLsCount)
	if errCode != ErrNoError {
		result.Error = newErrorFromCString(err, errStr)
	}
	result.ACLBindings = a.cToACLBindings(cResultACLs, cResultACLsCount)
	return
}

// cToDeleteACLsResults converts a C rd_kafka_DeleteAcls_result_response_t array to Go DeleteAclsResult slice.
func (a *AdminClient) cToDeleteACLsResults(cDeleteACLsResResponse **C.rd_kafka_DeleteAcls_result_response_t, resResponseCnt C.size_t) (result []DeleteACLsResult) {
	result = make([]DeleteACLsResult, uint(resResponseCnt))

	for i := uint(0); i < uint(resResponseCnt); i++ {
		cDeleteACLsResResponse := C.DeleteAcls_result_response_by_idx(cDeleteACLsResResponse, resResponseCnt, C.size_t(i))
		if cDeleteACLsResResponse == nil {
			panic("DeleteAcls_result_response_by_idx must not return nil")
		}

		cDeleteACLsError := C.rd_kafka_DeleteAcls_result_response_error(cDeleteACLsResResponse)
		result[i].Error = newErrorFromCError(cDeleteACLsError)

		var cMatchingACLsCount C.size_t
		cMatchingACLs := C.rd_kafka_DeleteAcls_result_response_matching_acls(
			cDeleteACLsResResponse, &cMatchingACLsCount)

		result[i].ACLBindings = a.cToACLBindings(cMatchingACLs, cMatchingACLsCount)
	}
	return
}

// CreateACLs creates one or more ACL bindings.
//
// Parameters:
//   - `ctx` - context with the maximum amount of time to block, or nil for indefinite.
//   - `aclBindings` - A slice of ACL binding specifications to create.
//   - `options` - Create ACLs options
//
// Returns a slice of CreateACLResult with a ErrNoError ErrorCode when the operation was successful
// plus an error that is not nil for client level errors
func (a *AdminClient) CreateACLs(ctx context.Context, aclBindings ACLBindings, options ...CreateACLsAdminOption) (result []CreateACLResult, err error) {
	err = a.verifyClient()
	if err != nil {
		return nil, err
	}

	if aclBindings == nil {
		return nil, newErrorFromString(ErrInvalidArg,
			"Expected non-nil slice of ACLBinding structs")
	}
	if len(aclBindings) == 0 {
		return nil, newErrorFromString(ErrInvalidArg,
			"Expected non-empty slice of ACLBinding structs")
	}

	cErrstrSize := C.size_t(512)
	cErrstr := (*C.char)(C.malloc(cErrstrSize))
	defer C.free(unsafe.Pointer(cErrstr))

	cACLBindings := make([]*C.rd_kafka_AclBinding_t, len(aclBindings))

	for i, aclBinding := range aclBindings {
		cACLBindings[i], err = a.aclBindingToC(&aclBinding, cErrstr, cErrstrSize)
		if err != nil {
			return
		}
		defer C.rd_kafka_AclBinding_destroy(cACLBindings[i])
	}

	// Convert Go AdminOptions (if any) to C AdminOptions
	genericOptions := make([]AdminOption, len(options))
	for i := range options {
		genericOptions[i] = options[i]
	}
	cOptions, err := adminOptionsSetup(a.handle, C.RD_KAFKA_ADMIN_OP_CREATEACLS, genericOptions)
	if err != nil {
		return nil, err
	}

	// Create temporary queue for async operation
	cQueue := C.rd_kafka_queue_new(a.handle.rk)
	defer C.rd_kafka_queue_destroy(cQueue)

	// Asynchronous call
	C.rd_kafka_CreateAcls(
		a.handle.rk,
		(**C.rd_kafka_AclBinding_t)(&cACLBindings[0]),
		C.size_t(len(cACLBindings)),
		cOptions,
		cQueue)

	// Wait for result, error or context timeout
	rkev, err := a.waitResult(ctx, cQueue, C.RD_KAFKA_EVENT_CREATEACLS_RESULT)
	if err != nil {
		return nil, err
	}
	defer C.rd_kafka_event_destroy(rkev)

	var cResultCnt C.size_t
	cResult := C.rd_kafka_event_CreateAcls_result(rkev)
	aclResults := C.rd_kafka_CreateAcls_result_acls(cResult, &cResultCnt)
	result, err = a.cToCreateACLResults(aclResults, cResultCnt)
	return
}

// DescribeACLs matches ACL bindings by filter.
//
// Parameters:
//   - `ctx` - context with the maximum amount of time to block, or nil for indefinite.
//   - `aclBindingFilter` - A filter with attributes that must match.
//     string attributes match exact values or any string if set to empty string.
//     Enum attributes match exact values or any value if ending with `Any`.
//     If `ResourcePatternType` is set to `ResourcePatternTypeMatch` returns ACL bindings with:
//   - `ResourcePatternTypeLiteral` pattern type with resource name equal to the given resource name
//   - `ResourcePatternTypeLiteral` pattern type with wildcard resource name that matches the given resource name
//   - `ResourcePatternTypePrefixed` pattern type with resource name that is a prefix of the given resource name
//   - `options` - Describe ACLs options
//
// Returns a slice of ACLBindings when the operation was successful
// plus an error that is not `nil` for client level errors
func (a *AdminClient) DescribeACLs(ctx context.Context, aclBindingFilter ACLBindingFilter, options ...DescribeACLsAdminOption) (result *DescribeACLsResult, err error) {
	err = a.verifyClient()
	if err != nil {
		return nil, err
	}

	cErrstrSize := C.size_t(512)
	cErrstr := (*C.char)(C.malloc(cErrstrSize))
	defer C.free(unsafe.Pointer(cErrstr))

	cACLBindingFilter, err := a.aclBindingFilterToC(&aclBindingFilter, cErrstr, cErrstrSize)
	if err != nil {
		return
	}

	// Convert Go AdminOptions (if any) to C AdminOptions
	genericOptions := make([]AdminOption, len(options))
	for i := range options {
		genericOptions[i] = options[i]
	}
	cOptions, err := adminOptionsSetup(a.handle, C.RD_KAFKA_ADMIN_OP_DESCRIBEACLS, genericOptions)
	if err != nil {
		return nil, err
	}
	// Create temporary queue for async operation
	cQueue := C.rd_kafka_queue_new(a.handle.rk)
	defer C.rd_kafka_queue_destroy(cQueue)

	// Asynchronous call
	C.rd_kafka_DescribeAcls(
		a.handle.rk,
		cACLBindingFilter,
		cOptions,
		cQueue)

	// Wait for result, error or context timeout
	rkev, err := a.waitResult(ctx, cQueue, C.RD_KAFKA_EVENT_DESCRIBEACLS_RESULT)
	if err != nil {
		return nil, err
	}
	defer C.rd_kafka_event_destroy(rkev)
	result = a.cToDescribeACLsResult(rkev)
	return
}

// DeleteACLs deletes ACL bindings matching one or more ACL binding filters.
//
// Parameters:
//   - `ctx` - context with the maximum amount of time to block, or nil for indefinite.
//   - `aclBindingFilters` - a slice of ACL binding filters to match ACLs to delete.
//     string attributes match exact values or any string if set to empty string.
//     Enum attributes match exact values or any value if ending with `Any`.
//     If `ResourcePatternType` is set to `ResourcePatternTypeMatch` deletes ACL bindings with:
//   - `ResourcePatternTypeLiteral` pattern type with resource name equal to the given resource name
//   - `ResourcePatternTypeLiteral` pattern type with wildcard resource name that matches the given resource name
//   - `ResourcePatternTypePrefixed` pattern type with resource name that is a prefix of the given resource name
//   - `options` - Delete ACLs options
//
// Returns a slice of ACLBinding for each filter when the operation was successful
// plus an error that is not `nil` for client level errors
func (a *AdminClient) DeleteACLs(ctx context.Context, aclBindingFilters ACLBindingFilters, options ...DeleteACLsAdminOption) (result []DeleteACLsResult, err error) {
	err = a.verifyClient()
	if err != nil {
		return nil, err
	}

	if aclBindingFilters == nil {
		return nil, newErrorFromString(ErrInvalidArg,
			"Expected non-nil slice of ACLBindingFilter structs")
	}
	if len(aclBindingFilters) == 0 {
		return nil, newErrorFromString(ErrInvalidArg,
			"Expected non-empty slice of ACLBindingFilter structs")
	}

	cErrstrSize := C.size_t(512)
	cErrstr := (*C.char)(C.malloc(cErrstrSize))
	defer C.free(unsafe.Pointer(cErrstr))

	cACLBindingFilters := make([]*C.rd_kafka_AclBindingFilter_t, len(aclBindingFilters))

	for i, aclBindingFilter := range aclBindingFilters {
		cACLBindingFilters[i], err = a.aclBindingFilterToC(&aclBindingFilter, cErrstr, cErrstrSize)
		if err != nil {
			return
		}
		defer C.rd_kafka_AclBinding_destroy(cACLBindingFilters[i])
	}

	// Convert Go AdminOptions (if any) to C AdminOptions
	genericOptions := make([]AdminOption, len(options))
	for i := range options {
		genericOptions[i] = options[i]
	}
	cOptions, err := adminOptionsSetup(a.handle, C.RD_KAFKA_ADMIN_OP_DELETEACLS, genericOptions)
	if err != nil {
		return nil, err
	}
	// Create temporary queue for async operation
	cQueue := C.rd_kafka_queue_new(a.handle.rk)
	defer C.rd_kafka_queue_destroy(cQueue)

	// Asynchronous call
	C.rd_kafka_DeleteAcls(
		a.handle.rk,
		(**C.rd_kafka_AclBindingFilter_t)(&cACLBindingFilters[0]),
		C.size_t(len(aclBindingFilters)),
		cOptions,
		cQueue)

	// Wait for result, error or context timeout
	rkev, err := a.waitResult(ctx, cQueue, C.RD_KAFKA_EVENT_DELETEACLS_RESULT)
	if err != nil {
		return nil, err
	}
	defer C.rd_kafka_event_destroy(rkev)

	var cResultResponsesCount C.size_t
	cResult := C.rd_kafka_event_DeleteAcls_result(rkev)
	cResultResponses := C.rd_kafka_DeleteAcls_result_responses(cResult, &cResultResponsesCount)
	result = a.cToDeleteACLsResults(cResultResponses, cResultResponsesCount)
	return
}
