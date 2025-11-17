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
	"unsafe"
)

/*
#include "select_rdkafka.h"
#include <stdlib.h>
*/
import "C"

// DeletedRecords contains information about deleted
// records of a single partition
type DeletedRecords struct {
	// Low-watermark offset after deletion
	LowWatermark Offset
}

// DeleteRecordsResult represents the result of a DeleteRecords call
// for a single partition.
type DeleteRecordsResult struct {
	// One of requested partitions.
	// The Error field is set if any occurred for that partition.
	TopicPartition TopicPartition
	// Deleted records information, or nil if an error occurred.
	DeletedRecords *DeletedRecords
}

// DeleteRecordsResults represents the results of a DeleteRecords call.
type DeleteRecordsResults struct {
	// A slice of DeleteRecordsResult, one for each requested topic partition.
	DeleteRecordsResults []DeleteRecordsResult
}

// OffsetSpec specifies desired offsets while using ListOffsets.
type OffsetSpec int64

const (
	// MaxTimestampOffsetSpec is used to describe the offset with the Max Timestamp which may be different then LatestOffsetSpec as Timestamp can be set client side.
	MaxTimestampOffsetSpec OffsetSpec = C.RD_KAFKA_OFFSET_SPEC_MAX_TIMESTAMP
	// EarliestOffsetSpec is used to describe the earliest offset for the TopicPartition.
	EarliestOffsetSpec OffsetSpec = C.RD_KAFKA_OFFSET_SPEC_EARLIEST
	// LatestOffsetSpec is used to describe the latest offset for the TopicPartition.
	LatestOffsetSpec OffsetSpec = C.RD_KAFKA_OFFSET_SPEC_LATEST
)

// NewOffsetSpecForTimestamp creates an OffsetSpec corresponding to the timestamp.
func NewOffsetSpecForTimestamp(timestamp int64) OffsetSpec {
	return OffsetSpec(timestamp)
}

// ListOffsetsResultInfo describes the result of ListOffsets request for a Topic Partition.
type ListOffsetsResultInfo struct {
	Offset      Offset
	Timestamp   int64
	LeaderEpoch *int32
	Error       Error
}

// ListOffsetsResult holds the map of TopicPartition to ListOffsetsResultInfo for a request.
type ListOffsetsResult struct {
	ResultInfos map[TopicPartition]ListOffsetsResultInfo
}

// setupTopicPartitionFromCtopicPartitionResult sets up a Go TopicPartition from a C rd_kafka_topic_partition_t & C.rd_kafka_error_t.
func setupTopicPartitionFromCtopicPartitionResult(partition *TopicPartition, ctopicPartRes *C.rd_kafka_topic_partition_result_t) {

	setupTopicPartitionFromCrktpar(partition, C.rd_kafka_topic_partition_result_partition(ctopicPartRes))
	partition.Error = newErrorFromCError(C.rd_kafka_topic_partition_result_error(ctopicPartRes))
}

// Convert a C rd_kafka_topic_partition_result_t array to a Go TopicPartition list.
func newTopicPartitionsFromCTopicPartitionResult(cResponse **C.rd_kafka_topic_partition_result_t, size C.size_t) (partitions []TopicPartition) {

	partCnt := int(size)

	partitions = make([]TopicPartition, partCnt)

	for i := 0; i < partCnt; i++ {
		setupTopicPartitionFromCtopicPartitionResult(&partitions[i], C.TopicPartitionResult_by_idx(cResponse, C.size_t(partCnt), C.size_t(i)))
	}

	return partitions
}

// cToDeletedRecordResult converts a C topic partitions list to a Go DeleteRecordsResult slice.
func cToDeletedRecordResult(
	cparts *C.rd_kafka_topic_partition_list_t) (results []DeleteRecordsResult) {
	partitions := newTopicPartitionsFromCparts(cparts)
	partitionsLen := len(partitions)
	results = make([]DeleteRecordsResult, partitionsLen)

	for i := 0; i < partitionsLen; i++ {
		results[i].TopicPartition = partitions[i]
		if results[i].TopicPartition.Error == nil {
			results[i].DeletedRecords = &DeletedRecords{
				LowWatermark: results[i].TopicPartition.Offset}
		}
	}

	return results
}

// cToListOffsetsResult converts a C
// rd_kafka_ListOffsets_result_t to a Go ListOffsetsResult
func cToListOffsetsResult(cRes *C.rd_kafka_ListOffsets_result_t) (result ListOffsetsResult) {
	result = ListOffsetsResult{ResultInfos: make(map[TopicPartition]ListOffsetsResultInfo)}
	var cPartitionCount C.size_t
	cResultInfos := C.rd_kafka_ListOffsets_result_infos(cRes, &cPartitionCount)
	for itr := 0; itr < int(cPartitionCount); itr++ {
		cResultInfo := C.ListOffsetsResultInfo_by_idx(cResultInfos, cPartitionCount, C.size_t(itr))
		resultInfo := ListOffsetsResultInfo{}
		cPartition := C.rd_kafka_ListOffsetsResultInfo_topic_partition(cResultInfo)
		Topic := C.GoString(cPartition.topic)
		Partition := TopicPartition{Topic: &Topic, Partition: int32(cPartition.partition)}
		resultInfo.Offset = Offset(cPartition.offset)
		resultInfo.Timestamp = int64(C.rd_kafka_ListOffsetsResultInfo_timestamp(cResultInfo))
		cLeaderEpoch := int32(C.rd_kafka_topic_partition_get_leader_epoch(cPartition))
		if cLeaderEpoch >= 0 {
			resultInfo.LeaderEpoch = &cLeaderEpoch
		}
		resultInfo.Error = newError(cPartition.err)
		result.ResultInfos[Partition] = resultInfo
	}
	return result
}

// ListOffsets lists the offsets for topic partition(s) specified by the
// topicPartitionOffsets parameter according to the parameters passed via
// options.
//
// Parameters:
//   - `ctx` - context with the maximum amount of time to block, or nil for
//     indefinite.
//   - `topicPartitionOffsets` - the Topic Partitions for which we are seeking
//     offsets, and the corresponding offset spec (OffsetSpec) we are looking for.
//   - `options` - ListOffsetsAdminOptions options.
//
// Returns a ListOffsetsResult, a map of TopicPartition to ListOffsetsResultInfo
// each of which has the Timestamp, Offset, LeaderEpoch and Error for that
// particular Topic Partition.
func (a *AdminClient) ListOffsets(
	ctx context.Context, topicPartitionOffsets map[TopicPartition]OffsetSpec,
	options ...ListOffsetsAdminOption) (result ListOffsetsResult, err error) {
	if topicPartitionOffsets == nil {
		return result, newErrorFromString(ErrInvalidArg, "expected topicPartitionOffsets parameter.")
	}

	topicPartitions := C.rd_kafka_topic_partition_list_new(C.int(len(topicPartitionOffsets)))
	defer C.rd_kafka_topic_partition_list_destroy(topicPartitions)

	for tp, offsetValue := range topicPartitionOffsets {
		cStr := C.CString(*tp.Topic)
		defer C.free(unsafe.Pointer(cStr))
		topicPartition := C.rd_kafka_topic_partition_list_add(topicPartitions, cStr, C.int32_t(tp.Partition))
		topicPartition.offset = C.int64_t(offsetValue)
	}

	// Convert Go AdminOptions (if any) to C AdminOptions.
	genericOptions := make([]AdminOption, len(options))
	for i := range options {
		genericOptions[i] = options[i]
	}
	cOptions, err := adminOptionsSetup(
		a.handle, C.RD_KAFKA_ADMIN_OP_LISTOFFSETS, genericOptions)
	if err != nil {
		return result, err
	}
	defer C.rd_kafka_AdminOptions_destroy(cOptions)

	// Create temporary queue for async operation.
	cQueue := C.rd_kafka_queue_new(a.handle.rk)
	defer C.rd_kafka_queue_destroy(cQueue)

	// Call rd_kafka_ListOffsets (asynchronous).
	C.rd_kafka_ListOffsets(
		a.handle.rk,
		topicPartitions,
		cOptions,
		cQueue)

	// Wait for result, error or context timeout.
	rkev, err := a.waitResult(
		ctx, cQueue, C.RD_KAFKA_EVENT_LISTOFFSETS_RESULT)
	if err != nil {
		return result, err
	}
	defer C.rd_kafka_event_destroy(rkev)

	cRes := C.rd_kafka_event_ListOffsets_result(rkev)

	// Convert result from C to Go.
	result = cToListOffsetsResult(cRes)

	return result, nil
}

// DeleteRecords deletes records (messages) in topic partitions older than the offsets provided.
//
// Parameters:
//   - `ctx` - context with the maximum amount of time to block, or nil for
//     indefinite.
//   - `recordsToDelete` - A slice of TopicPartitions with the offset field set.
//     For each partition, delete all messages up to but not including the specified offset.
//     The offset could be set to kafka.OffsetEnd to delete all the messages in the partition.
//   - `options` - DeleteRecordsAdminOptions options.
//
// Returns a DeleteRecordsResults, which contains a slice of
// DeleteRecordsResult, each representing the result for one topic partition.
// Individual TopicPartitions inside the DeleteRecordsResult should be checked for errors.
// If successful, the DeletedRecords within the DeleteRecordsResult will be non-nil,
// and contain the low-watermark offset (smallest available offset of all live replicas).
func (a *AdminClient) DeleteRecords(ctx context.Context,
	recordsToDelete []TopicPartition,
	options ...DeleteRecordsAdminOption) (result DeleteRecordsResults, err error) {
	err = a.verifyClient()
	if err != nil {
		return result, err
	}

	if len(recordsToDelete) == 0 {
		return result, newErrorFromString(ErrInvalidArg, "No records to delete")
	}

	// convert recordsToDelete to rd_kafka_DeleteRecords_t** required by implementation
	cRecordsToDelete := newCPartsFromTopicPartitions(recordsToDelete)
	defer C.rd_kafka_topic_partition_list_destroy(cRecordsToDelete)

	cDelRecords := make([]*C.rd_kafka_DeleteRecords_t, 1)
	defer C.rd_kafka_DeleteRecords_destroy_array(&cDelRecords[0], C.size_t(1))

	cDelRecords[0] = C.rd_kafka_DeleteRecords_new(cRecordsToDelete)

	// Convert Go AdminOptions (if any) to C AdminOptions.
	genericOptions := make([]AdminOption, len(options))
	for i := range options {
		genericOptions[i] = options[i]
	}
	cOptions, err := adminOptionsSetup(
		a.handle, C.RD_KAFKA_ADMIN_OP_DELETERECORDS, genericOptions)
	if err != nil {
		return result, err
	}
	defer C.rd_kafka_AdminOptions_destroy(cOptions)

	// Create temporary queue for async operation.
	cQueue := C.rd_kafka_queue_new(a.handle.rk)
	defer C.rd_kafka_queue_destroy(cQueue)

	// Call rd_kafka_DeleteRecords (asynchronous).
	C.rd_kafka_DeleteRecords(
		a.handle.rk,
		&cDelRecords[0],
		C.size_t(1),
		cOptions,
		cQueue)

	// Wait for result, error or context timeout.
	rkev, err := a.waitResult(
		ctx, cQueue, C.RD_KAFKA_EVENT_DELETERECORDS_RESULT)
	if err != nil {
		return result, err
	}
	defer C.rd_kafka_event_destroy(rkev)

	cRes := C.rd_kafka_event_DeleteRecords_result(rkev)
	cDeleteRecordsResultList := C.rd_kafka_DeleteRecords_result_offsets(cRes)

	// Convert result from C to Go.
	result.DeleteRecordsResults =
		cToDeletedRecordResult(cDeleteRecordsResultList)

	return result, nil
}
