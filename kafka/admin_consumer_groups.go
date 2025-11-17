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

// ConsumerGroupResult provides per-group operation result (error) information.
type ConsumerGroupResult struct {
	// Group name
	Group string
	// Error, if any, of result. Check with `Error.Code() != ErrNoError`.
	Error Error
}

// String returns a human-readable representation of a ConsumerGroupResult.
func (g ConsumerGroupResult) String() string {
	if g.Error.code == ErrNoError {
		return g.Group
	}
	return fmt.Sprintf("%s (%s)", g.Group, g.Error.str)
}

// ConsumerGroupState represents a consumer group state
type ConsumerGroupState int

const (
	// ConsumerGroupStateUnknown - Unknown ConsumerGroupState
	ConsumerGroupStateUnknown ConsumerGroupState = C.RD_KAFKA_CONSUMER_GROUP_STATE_UNKNOWN
	// ConsumerGroupStatePreparingRebalance - preparing rebalance
	ConsumerGroupStatePreparingRebalance ConsumerGroupState = C.RD_KAFKA_CONSUMER_GROUP_STATE_PREPARING_REBALANCE
	// ConsumerGroupStateCompletingRebalance - completing rebalance
	ConsumerGroupStateCompletingRebalance ConsumerGroupState = C.RD_KAFKA_CONSUMER_GROUP_STATE_COMPLETING_REBALANCE
	// ConsumerGroupStateStable - stable
	ConsumerGroupStateStable ConsumerGroupState = C.RD_KAFKA_CONSUMER_GROUP_STATE_STABLE
	// ConsumerGroupStateDead - dead group
	ConsumerGroupStateDead ConsumerGroupState = C.RD_KAFKA_CONSUMER_GROUP_STATE_DEAD
	// ConsumerGroupStateEmpty - empty group
	ConsumerGroupStateEmpty ConsumerGroupState = C.RD_KAFKA_CONSUMER_GROUP_STATE_EMPTY
)

// String returns the human-readable representation of a consumer_group_state
func (t ConsumerGroupState) String() string {
	return C.GoString(C.rd_kafka_consumer_group_state_name(
		C.rd_kafka_consumer_group_state_t(t)))
}

// ConsumerGroupStateFromString translates a consumer group state name/string to
// a ConsumerGroupState value.
func ConsumerGroupStateFromString(stateString string) (ConsumerGroupState, error) {
	cStr := C.CString(stateString)
	defer C.free(unsafe.Pointer(cStr))
	state := ConsumerGroupState(C.rd_kafka_consumer_group_state_code(cStr))
	return state, nil
}

// ConsumerGroupType represents a consumer group type
type ConsumerGroupType int

const (
	// ConsumerGroupTypeUnknown - Unknown ConsumerGroupType
	ConsumerGroupTypeUnknown ConsumerGroupType = C.RD_KAFKA_CONSUMER_GROUP_TYPE_UNKNOWN
	// ConsumerGroupTypeConsumer - Consumer ConsumerGroupType
	ConsumerGroupTypeConsumer ConsumerGroupType = C.RD_KAFKA_CONSUMER_GROUP_TYPE_CONSUMER
	// ConsumerGroupTypeClassic - Classic ConsumerGroupType
	ConsumerGroupTypeClassic ConsumerGroupType = C.RD_KAFKA_CONSUMER_GROUP_TYPE_CLASSIC
)

// String returns the human-readable representation of a ConsumerGroupType
func (t ConsumerGroupType) String() string {
	return C.GoString(C.rd_kafka_consumer_group_type_name(
		C.rd_kafka_consumer_group_type_t(t)))
}

// ConsumerGroupTypeFromString translates a consumer group type name/string to
// a ConsumerGroupType value.
func ConsumerGroupTypeFromString(typeString string) ConsumerGroupType {
	cStr := C.CString(typeString)
	defer C.free(unsafe.Pointer(cStr))
	groupType := ConsumerGroupType(C.rd_kafka_consumer_group_type_code(cStr))
	return groupType
}

// ConsumerGroupListing represents the result of ListConsumerGroups for a single
// group.
type ConsumerGroupListing struct {
	// Group id.
	GroupID string
	// Is a simple consumer group.
	IsSimpleConsumerGroup bool
	// Group state.
	State ConsumerGroupState
	// Group type.
	Type ConsumerGroupType
}

// ListConsumerGroupsResult represents ListConsumerGroups results and errors.
type ListConsumerGroupsResult struct {
	// List of valid ConsumerGroupListings.
	Valid []ConsumerGroupListing
	// List of errors.
	Errors []error
}

// MemberAssignment represents the assignment of a consumer group member.
type MemberAssignment struct {
	// Partitions assigned to current member.
	TopicPartitions []TopicPartition
}

// MemberDescription represents the description of a consumer group member.
type MemberDescription struct {
	// Client id.
	ClientID string
	// Group instance id.
	GroupInstanceID string
	// Consumer id.
	ConsumerID string
	// Group member host.
	Host string
	// Member assignment.
	Assignment MemberAssignment
	// Member Target Assignment. Set to `nil` for `Classic` GroupType.
	TargetAssignment *MemberAssignment
}

// ConsumerGroupDescription represents the result of DescribeConsumerGroups for
// a single group.
type ConsumerGroupDescription struct {
	// Group id.
	GroupID string
	// Error, if any, of result. Check with `Error.Code() != ErrNoError`.
	Error Error
	// Is a simple consumer group.
	IsSimpleConsumerGroup bool
	// Partition assignor identifier.
	PartitionAssignor string
	// Consumer group state.
	State ConsumerGroupState
	// Consumer group type.
	Type ConsumerGroupType
	// Consumer group coordinator (has ID == -1 if not known).
	Coordinator Node
	// Members list.
	Members []MemberDescription
	// Operations allowed for the group (nil if not available or not requested)
	AuthorizedOperations []ACLOperation
}

// DescribeConsumerGroupsResult represents the result of a
// DescribeConsumerGroups call.
type DescribeConsumerGroupsResult struct {
	// Slice of ConsumerGroupDescription.
	ConsumerGroupDescriptions []ConsumerGroupDescription
}

// DeleteConsumerGroupsResult represents the result of a DeleteConsumerGroups
// call.
type DeleteConsumerGroupsResult struct {
	// Slice of ConsumerGroupResult.
	ConsumerGroupResults []ConsumerGroupResult
}

// ListConsumerGroupOffsetsResult represents the result of a
// ListConsumerGroupOffsets operation.
type ListConsumerGroupOffsetsResult struct {
	// A slice of ConsumerGroupTopicPartitions, each element represents a group's
	// TopicPartitions and Offsets.
	ConsumerGroupsTopicPartitions []ConsumerGroupTopicPartitions
}

// AlterConsumerGroupOffsetsResult represents the result of a
// AlterConsumerGroupOffsets operation.
type AlterConsumerGroupOffsetsResult struct {
	// A slice of ConsumerGroupTopicPartitions, each element represents a group's
	// TopicPartitions and Offsets.
	ConsumerGroupsTopicPartitions []ConsumerGroupTopicPartitions
}

// cToConsumerGroupResults converts a C group_result_t array to Go ConsumerGroupResult list.
func (a *AdminClient) cToConsumerGroupResults(
	cGroupRes **C.rd_kafka_group_result_t, cCnt C.size_t) (result []ConsumerGroupResult, err error) {
	result = make([]ConsumerGroupResult, int(cCnt))

	for idx := 0; idx < int(cCnt); idx++ {
		cGroup := C.group_result_by_idx(cGroupRes, cCnt, C.size_t(idx))
		result[idx].Group = C.GoString(C.rd_kafka_group_result_name(cGroup))
		result[idx].Error = newErrorFromCError(C.rd_kafka_group_result_error(cGroup))
	}

	return result, nil
}

// cToConsumerGroupDescriptions converts a C rd_kafka_ConsumerGroupDescription_t
// array to a Go ConsumerGroupDescription slice.
func (a *AdminClient) cToConsumerGroupDescriptions(
	cGroups **C.rd_kafka_ConsumerGroupDescription_t,
	cGroupCount C.size_t) (result []ConsumerGroupDescription) {
	result = make([]ConsumerGroupDescription, cGroupCount)
	for idx := 0; idx < int(cGroupCount); idx++ {
		cGroup := C.ConsumerGroupDescription_by_idx(
			cGroups, cGroupCount, C.size_t(idx))

		groupID := C.GoString(
			C.rd_kafka_ConsumerGroupDescription_group_id(cGroup))
		err := newErrorFromCError(
			C.rd_kafka_ConsumerGroupDescription_error(cGroup))
		isSimple := cint2bool(
			C.rd_kafka_ConsumerGroupDescription_is_simple_consumer_group(cGroup))
		paritionAssignor := C.GoString(
			C.rd_kafka_ConsumerGroupDescription_partition_assignor(cGroup))
		state := ConsumerGroupState(
			C.rd_kafka_ConsumerGroupDescription_state(cGroup))
		groupType := ConsumerGroupType(
			C.rd_kafka_ConsumerGroupDescription_type(cGroup))

		cNode := C.rd_kafka_ConsumerGroupDescription_coordinator(cGroup)
		coordinator := a.cToNode(cNode)

		membersCount := int(
			C.rd_kafka_ConsumerGroupDescription_member_count(cGroup))
		members := make([]MemberDescription, membersCount)

		for midx := 0; midx < membersCount; midx++ {
			cMember :=
				C.rd_kafka_ConsumerGroupDescription_member(cGroup, C.size_t(midx))
			cMemberAssignment :=
				C.rd_kafka_MemberDescription_assignment(cMember)
			cToppars :=
				C.rd_kafka_MemberAssignment_partitions(cMemberAssignment)
			memberAssignment := MemberAssignment{}
			if cToppars != nil {
				memberAssignment.TopicPartitions = newTopicPartitionsFromCparts(cToppars)
			}
			cMemberTargetAssignment :=
				C.rd_kafka_MemberDescription_target_assignment(cMember)
			memberTargetAssignment := &MemberAssignment{}
			if cMemberTargetAssignment != nil {
				cTargetToppars := C.rd_kafka_MemberAssignment_partitions(cMemberTargetAssignment)
				if cTargetToppars != nil {
					memberTargetAssignment.TopicPartitions = newTopicPartitionsFromCparts(cTargetToppars)
				}
			} else {
				memberTargetAssignment = nil
			}

			members[midx] = MemberDescription{
				ClientID: C.GoString(
					C.rd_kafka_MemberDescription_client_id(cMember)),
				GroupInstanceID: C.GoString(
					C.rd_kafka_MemberDescription_group_instance_id(cMember)),
				ConsumerID: C.GoString(
					C.rd_kafka_MemberDescription_consumer_id(cMember)),
				Host: C.GoString(
					C.rd_kafka_MemberDescription_host(cMember)),
				Assignment:       memberAssignment,
				TargetAssignment: memberTargetAssignment,
			}
		}

		cAuthorizedOperationsCnt := C.size_t(0)
		cAuthorizedOperations := C.rd_kafka_ConsumerGroupDescription_authorized_operations(
			cGroup, &cAuthorizedOperationsCnt)
		authorizedOperations := a.cToAuthorizedOperations(cAuthorizedOperations,
			cAuthorizedOperationsCnt)

		result[idx] = ConsumerGroupDescription{
			GroupID:               groupID,
			Error:                 err,
			IsSimpleConsumerGroup: isSimple,
			PartitionAssignor:     paritionAssignor,
			Type:                  groupType,
			State:                 state,
			Coordinator:           coordinator,
			Members:               members,
			AuthorizedOperations:  authorizedOperations,
		}
	}
	return result
}

// ConsumerGroupDescription converts a C rd_kafka_ConsumerGroupListing_t array
// to a Go ConsumerGroupListing slice.
func (a *AdminClient) cToConsumerGroupListings(
	cGroups **C.rd_kafka_ConsumerGroupListing_t,
	cGroupCount C.size_t) (result []ConsumerGroupListing) {
	result = make([]ConsumerGroupListing, cGroupCount)

	for idx := 0; idx < int(cGroupCount); idx++ {
		cGroup :=
			C.ConsumerGroupListing_by_idx(cGroups, cGroupCount, C.size_t(idx))
		state := ConsumerGroupState(
			C.rd_kafka_ConsumerGroupListing_state(cGroup))
		groupType := ConsumerGroupType(C.rd_kafka_ConsumerGroupListing_type(cGroup))
		result[idx] = ConsumerGroupListing{
			GroupID: C.GoString(
				C.rd_kafka_ConsumerGroupListing_group_id(cGroup)),
			IsSimpleConsumerGroup: cint2bool(
				C.rd_kafka_ConsumerGroupListing_is_simple_consumer_group(cGroup)),
			State: state,
			Type:  groupType,
		}
	}
	return result
}

// ListConsumerGroups lists the consumer groups available in the cluster.
//
// Parameters:
//   - `ctx` - context with the maximum amount of time to block, or nil for
//     indefinite.
//   - `options` - ListConsumerGroupsAdminOption options.
//
// Returns a ListConsumerGroupsResult, which contains a slice corresponding to
// each group in the cluster and a slice of errors encountered while listing.
// Additionally, an error that is not nil for client-level errors is returned.
// Both the returned error, and the errors slice should be checked.
func (a *AdminClient) ListConsumerGroups(
	ctx context.Context,
	options ...ListConsumerGroupsAdminOption) (result ListConsumerGroupsResult, err error) {

	result = ListConsumerGroupsResult{}
	err = a.verifyClient()
	if err != nil {
		return result, err
	}

	// Convert Go AdminOptions (if any) to C AdminOptions.
	genericOptions := make([]AdminOption, len(options))
	for i := range options {
		genericOptions[i] = options[i]
	}
	cOptions, err := adminOptionsSetup(a.handle,
		C.RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPS, genericOptions)
	if err != nil {
		return result, err
	}
	defer C.rd_kafka_AdminOptions_destroy(cOptions)

	// Create temporary queue for async operation.
	cQueue := C.rd_kafka_queue_new(a.handle.rk)
	defer C.rd_kafka_queue_destroy(cQueue)

	// Call rd_kafka_ListConsumerGroups (asynchronous).
	C.rd_kafka_ListConsumerGroups(
		a.handle.rk,
		cOptions,
		cQueue)

	// Wait for result, error or context timeout.
	rkev, err := a.waitResult(
		ctx, cQueue, C.RD_KAFKA_EVENT_LISTCONSUMERGROUPS_RESULT)
	if err != nil {
		return result, err
	}
	defer C.rd_kafka_event_destroy(rkev)

	cRes := C.rd_kafka_event_ListConsumerGroups_result(rkev)

	// Convert result and broker errors from C to Go.
	var cGroupCount C.size_t
	cGroups := C.rd_kafka_ListConsumerGroups_result_valid(cRes, &cGroupCount)
	result.Valid = a.cToConsumerGroupListings(cGroups, cGroupCount)

	var cErrsCount C.size_t
	cErrs := C.rd_kafka_ListConsumerGroups_result_errors(cRes, &cErrsCount)
	if cErrsCount == 0 {
		return result, nil
	}

	result.Errors = a.cToErrorList(cErrs, cErrsCount)
	return result, nil
}

// DescribeConsumerGroups describes groups from cluster as specified by the
// groups list.
//
// Parameters:
//   - `ctx` - context with the maximum amount of time to block, or nil for
//     indefinite.
//   - `groups` - Slice of groups to describe. This should not be nil/empty.
//   - `options` - DescribeConsumerGroupsAdminOption options.
//
// Returns DescribeConsumerGroupsResult, which contains a slice of
// ConsumerGroupDescriptions corresponding to the input groups, plus an error
// that is not `nil` for client level errors. Individual
// ConsumerGroupDescriptions inside the slice should also be checked for
// errors.
func (a *AdminClient) DescribeConsumerGroups(
	ctx context.Context, groups []string,
	options ...DescribeConsumerGroupsAdminOption) (result DescribeConsumerGroupsResult, err error) {

	describeResult := DescribeConsumerGroupsResult{}
	err = a.verifyClient()
	if err != nil {
		return result, err
	}

	// Convert group names into char** required by the implementation.
	cGroupNameList := make([]*C.char, len(groups))
	cGroupNameCount := C.size_t(len(groups))

	for idx, group := range groups {
		cGroupNameList[idx] = C.CString(group)
		defer C.free(unsafe.Pointer(cGroupNameList[idx]))
	}

	var cGroupNameListPtr **C.char
	if cGroupNameCount > 0 {
		cGroupNameListPtr = ((**C.char)(&cGroupNameList[0]))
	}

	// Convert Go AdminOptions (if any) to C AdminOptions.
	genericOptions := make([]AdminOption, len(options))
	for i := range options {
		genericOptions[i] = options[i]
	}
	cOptions, err := adminOptionsSetup(
		a.handle, C.RD_KAFKA_ADMIN_OP_DESCRIBECONSUMERGROUPS, genericOptions)
	if err != nil {
		return describeResult, err
	}
	defer C.rd_kafka_AdminOptions_destroy(cOptions)

	// Create temporary queue for async operation.
	cQueue := C.rd_kafka_queue_new(a.handle.rk)
	defer C.rd_kafka_queue_destroy(cQueue)

	// Call rd_kafka_DescribeConsumerGroups (asynchronous).
	C.rd_kafka_DescribeConsumerGroups(
		a.handle.rk,
		cGroupNameListPtr,
		cGroupNameCount,
		cOptions,
		cQueue)

	// Wait for result, error or context timeout.
	rkev, err := a.waitResult(
		ctx, cQueue, C.RD_KAFKA_EVENT_DESCRIBECONSUMERGROUPS_RESULT)
	if err != nil {
		return describeResult, err
	}
	defer C.rd_kafka_event_destroy(rkev)

	cRes := C.rd_kafka_event_DescribeConsumerGroups_result(rkev)

	// Convert result from C to Go.
	var cGroupCount C.size_t
	cGroups := C.rd_kafka_DescribeConsumerGroups_result_groups(cRes, &cGroupCount)
	describeResult.ConsumerGroupDescriptions = a.cToConsumerGroupDescriptions(cGroups, cGroupCount)

	return describeResult, nil
}

// DeleteConsumerGroups deletes a batch of consumer groups.
//
// Parameters:
//   - `ctx` - context with the maximum amount of time to block, or nil for
//     indefinite.
//   - `groups` - Slice of consumer group ids (strings) to delete.
//   - `options` - DeleteConsumerGroupsAdminOption options.
//
// Returns DeleteConsumerGroupsResult, which contains a slice of
// ConsumerGroupResults corresponding to the input groups, each containing
// the Group id and any error for that particular group.
func (a *AdminClient) DeleteConsumerGroups(
	ctx context.Context,
	groups []string, options ...DeleteConsumerGroupsAdminOption) (result DeleteConsumerGroupsResult, err error) {
	cGroups := make([]*C.rd_kafka_DeleteGroup_t, len(groups))
	deleteResult := DeleteConsumerGroupsResult{}
	err = a.verifyClient()
	if err != nil {
		return deleteResult, err
	}

	// Convert Go DeleteGroups to C DeleteGroups
	for i, group := range groups {
		cGroupID := C.CString(group)
		defer C.free(unsafe.Pointer(cGroupID))

		cGroups[i] = C.rd_kafka_DeleteGroup_new(cGroupID)
		if cGroups[i] == nil {
			return deleteResult, newErrorFromString(ErrInvalidArg,
				fmt.Sprintf("Invalid arguments for group %s", group))
		}

		defer C.rd_kafka_DeleteGroup_destroy(cGroups[i])
	}

	// Convert Go AdminOptions (if any) to C AdminOptions
	genericOptions := make([]AdminOption, len(options))
	for i := range options {
		genericOptions[i] = options[i]
	}
	cOptions, err := adminOptionsSetup(
		a.handle, C.RD_KAFKA_ADMIN_OP_DELETEGROUPS, genericOptions)
	if err != nil {
		return deleteResult, err
	}
	defer C.rd_kafka_AdminOptions_destroy(cOptions)

	// Create temporary queue for async operation
	cQueue := C.rd_kafka_queue_new(a.handle.rk)
	defer C.rd_kafka_queue_destroy(cQueue)

	// Asynchronous call
	C.rd_kafka_DeleteGroups(
		a.handle.rk,
		(**C.rd_kafka_DeleteGroup_t)(&cGroups[0]),
		C.size_t(len(cGroups)),
		cOptions,
		cQueue)

	// Wait for result, error or context timeout
	rkev, err := a.waitResult(ctx, cQueue, C.RD_KAFKA_EVENT_DELETEGROUPS_RESULT)
	if err != nil {
		return deleteResult, err
	}
	defer C.rd_kafka_event_destroy(rkev)

	cRes := C.rd_kafka_event_DeleteGroups_result(rkev)

	// Convert result from C to Go
	var cCnt C.size_t
	cGroupRes := C.rd_kafka_DeleteGroups_result_groups(cRes, &cCnt)

	deleteResult.ConsumerGroupResults, err = a.cToConsumerGroupResults(cGroupRes, cCnt)
	return deleteResult, err
}

// ListConsumerGroupOffsets fetches the offsets for topic partition(s) for
// consumer group(s).
//
// Parameters:
//   - `ctx` - context with the maximum amount of time to block, or nil for indefinite.
//   - `groupsPartitions` - a slice of ConsumerGroupTopicPartitions, each element of which
//     has the id of a consumer group, and a slice of the TopicPartitions we
//     need to fetch the offsets for. The slice of TopicPartitions can be nil, to fetch
//     all topic partitions for that group.
//     Currently, the size of `groupsPartitions` has to be exactly one.
//   - `options` - ListConsumerGroupOffsetsAdminOption options.
//
// Returns a ListConsumerGroupOffsetsResult, containing a slice of
// ConsumerGroupTopicPartitions corresponding to the input slice, plus an error that is
// not `nil` for client level errors. Individual TopicPartitions inside each of
// the ConsumerGroupTopicPartitions should also be checked for errors.
func (a *AdminClient) ListConsumerGroupOffsets(
	ctx context.Context, groupsPartitions []ConsumerGroupTopicPartitions,
	options ...ListConsumerGroupOffsetsAdminOption) (lcgor ListConsumerGroupOffsetsResult, err error) {
	err = a.verifyClient()
	if err != nil {
		return lcgor, err
	}

	lcgor.ConsumerGroupsTopicPartitions = nil

	// For now, we only support one group at a time given as a single element of
	// groupsPartitions.
	// Code has been written so that only this if-guard needs to be removed when
	// we add support for multiple ConsumerGroupTopicPartitions.
	if len(groupsPartitions) != 1 {
		return lcgor, fmt.Errorf(
			"expected length of groupsPartitions is 1, got %d", len(groupsPartitions))
	}

	cGroupsPartitions := make([]*C.rd_kafka_ListConsumerGroupOffsets_t,
		len(groupsPartitions))

	// Convert Go ConsumerGroupTopicPartitions to C ListConsumerGroupOffsets.
	for i, groupPartitions := range groupsPartitions {
		// We need to destroy this list because rd_kafka_ListConsumerGroupOffsets_new
		// creates a copy of it.
		var cPartitions *C.rd_kafka_topic_partition_list_t = nil

		if groupPartitions.Partitions != nil {
			cPartitions = newCPartsFromTopicPartitions(groupPartitions.Partitions)
			defer C.rd_kafka_topic_partition_list_destroy(cPartitions)
		}

		cGroupID := C.CString(groupPartitions.Group)
		defer C.free(unsafe.Pointer(cGroupID))

		cGroupsPartitions[i] =
			C.rd_kafka_ListConsumerGroupOffsets_new(cGroupID, cPartitions)
		defer C.rd_kafka_ListConsumerGroupOffsets_destroy(cGroupsPartitions[i])
	}

	// Convert Go AdminOptions (if any) to C AdminOptions.
	genericOptions := make([]AdminOption, len(options))
	for i := range options {
		genericOptions[i] = options[i]
	}
	cOptions, err := adminOptionsSetup(
		a.handle, C.RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPOFFSETS, genericOptions)
	if err != nil {
		return lcgor, err
	}
	defer C.rd_kafka_AdminOptions_destroy(cOptions)

	// Create temporary queue for async operation.
	cQueue := C.rd_kafka_queue_new(a.handle.rk)
	defer C.rd_kafka_queue_destroy(cQueue)

	// Call rd_kafka_ListConsumerGroupOffsets (asynchronous).
	C.rd_kafka_ListConsumerGroupOffsets(
		a.handle.rk,
		(**C.rd_kafka_ListConsumerGroupOffsets_t)(&cGroupsPartitions[0]),
		C.size_t(len(cGroupsPartitions)),
		cOptions,
		cQueue)

	// Wait for result, error or context timeout.
	rkev, err := a.waitResult(
		ctx, cQueue, C.RD_KAFKA_EVENT_LISTCONSUMERGROUPOFFSETS_RESULT)
	if err != nil {
		return lcgor, err
	}
	defer C.rd_kafka_event_destroy(rkev)

	cRes := C.rd_kafka_event_ListConsumerGroupOffsets_result(rkev)

	// Convert result from C to Go.
	var cGroupCount C.size_t
	cGroups := C.rd_kafka_ListConsumerGroupOffsets_result_groups(cRes, &cGroupCount)
	lcgor.ConsumerGroupsTopicPartitions = a.cToConsumerGroupTopicPartitions(cGroups, cGroupCount)

	return lcgor, nil
}

// AlterConsumerGroupOffsets alters the offsets for topic partition(s) for
// consumer group(s).
//
// Parameters:
//   - `ctx` - context with the maximum amount of time to block, or nil for
//     indefinite.
//   - `groupsPartitions` - a slice of ConsumerGroupTopicPartitions, each element of
//     which has the id of a consumer group, and a slice of the TopicPartitions
//     we need to alter the offsets for. Currently, the size of
//     `groupsPartitions` has to be exactly one.
//   - `options` - AlterConsumerGroupOffsetsAdminOption options.
//
// Returns a AlterConsumerGroupOffsetsResult, containing a slice of
// ConsumerGroupTopicPartitions corresponding to the input slice, plus an error
// that is not `nil` for client level errors. Individual TopicPartitions inside
// each of the ConsumerGroupTopicPartitions should also be checked for errors.
// This will succeed at the partition level only if the group is not actively
// subscribed to the corresponding topic(s).
func (a *AdminClient) AlterConsumerGroupOffsets(
	ctx context.Context, groupsPartitions []ConsumerGroupTopicPartitions,
	options ...AlterConsumerGroupOffsetsAdminOption) (acgor AlterConsumerGroupOffsetsResult, err error) {
	err = a.verifyClient()
	if err != nil {
		return acgor, err
	}

	acgor.ConsumerGroupsTopicPartitions = nil

	// For now, we only support one group at a time given as a single element of groupsPartitions.
	// Code has been written so that only this if-guard needs to be removed when we add support for
	// multiple ConsumerGroupTopicPartitions.
	if len(groupsPartitions) != 1 {
		return acgor, fmt.Errorf(
			"expected length of groupsPartitions is 1, got %d",
			len(groupsPartitions))
	}

	cGroupsPartitions := make(
		[]*C.rd_kafka_AlterConsumerGroupOffsets_t, len(groupsPartitions))

	// Convert Go ConsumerGroupTopicPartitions to C AlterConsumerGroupOffsets.
	for idx, groupPartitions := range groupsPartitions {
		// We need to destroy this list because rd_kafka_AlterConsumerGroupOffsets_new
		// creates a copy of it.
		cPartitions := newCPartsFromTopicPartitions(groupPartitions.Partitions)

		cGroupID := C.CString(groupPartitions.Group)
		defer C.free(unsafe.Pointer(cGroupID))

		cGroupsPartitions[idx] =
			C.rd_kafka_AlterConsumerGroupOffsets_new(cGroupID, cPartitions)
		defer C.rd_kafka_AlterConsumerGroupOffsets_destroy(cGroupsPartitions[idx])
	}

	// Convert Go AdminOptions (if any) to C AdminOptions.
	genericOptions := make([]AdminOption, len(options))
	for i := range options {
		genericOptions[i] = options[i]
	}
	cOptions, err := adminOptionsSetup(
		a.handle, C.RD_KAFKA_ADMIN_OP_ALTERCONSUMERGROUPOFFSETS, genericOptions)
	if err != nil {
		return acgor, err
	}
	defer C.rd_kafka_AdminOptions_destroy(cOptions)

	// Create temporary queue for async operation.
	cQueue := C.rd_kafka_queue_new(a.handle.rk)
	defer C.rd_kafka_queue_destroy(cQueue)

	// Call rd_kafka_AlterConsumerGroupOffsets (asynchronous).
	C.rd_kafka_AlterConsumerGroupOffsets(
		a.handle.rk,
		(**C.rd_kafka_AlterConsumerGroupOffsets_t)(&cGroupsPartitions[0]),
		C.size_t(len(cGroupsPartitions)),
		cOptions,
		cQueue)

	// Wait for result, error or context timeout.
	rkev, err := a.waitResult(
		ctx, cQueue, C.RD_KAFKA_EVENT_ALTERCONSUMERGROUPOFFSETS_RESULT)
	if err != nil {
		return acgor, err
	}
	defer C.rd_kafka_event_destroy(rkev)

	cRes := C.rd_kafka_event_AlterConsumerGroupOffsets_result(rkev)

	// Convert result from C to Go.
	var cGroupCount C.size_t
	cGroups := C.rd_kafka_AlterConsumerGroupOffsets_result_groups(cRes, &cGroupCount)
	acgor.ConsumerGroupsTopicPartitions = a.cToConsumerGroupTopicPartitions(cGroups, cGroupCount)

	return acgor, nil
}
