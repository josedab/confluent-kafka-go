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

// TopicResult provides per-topic operation result (error) information.
type TopicResult struct {
	// Topic name
	Topic string
	// Error, if any, of result. Check with `Error.Code() != ErrNoError`.
	Error Error
}

// String returns a human-readable representation of a TopicResult.
func (t TopicResult) String() string {
	if t.Error.code == 0 {
		return t.Topic
	}
	return fmt.Sprintf("%s (%s)", t.Topic, t.Error.str)
}

// TopicSpecification holds parameters for creating a new topic.
// TopicSpecification is analogous to NewTopic in the Java Topic Admin API.
type TopicSpecification struct {
	// Topic name to create.
	Topic string
	// Number of partitions in topic.
	NumPartitions int
	// Default replication factor for the topic's partitions, or zero
	// if an explicit ReplicaAssignment is set.
	ReplicationFactor int
	// (Optional) Explicit replica assignment. The outer array is
	// indexed by the partition number, while the inner per-partition array
	// contains the replica broker ids. The first broker in each
	// broker id list will be the preferred replica.
	ReplicaAssignment [][]int32
	// Topic configuration.
	Config map[string]string
}

// PartitionsSpecification holds parameters for creating additional partitions for a topic.
// PartitionsSpecification is analogous to NewPartitions in the Java Topic Admin API.
type PartitionsSpecification struct {
	// Topic to create more partitions for.
	Topic string
	// New partition count for topic, must be higher than current partition count.
	IncreaseTo int
	// (Optional) Explicit replica assignment. The outer array is
	// indexed by the new partition index (i.e., 0 for the first added
	// partition), while the inner per-partition array
	// contains the replica broker ids. The first broker in each
	// broker id list will be the preferred replica.
	ReplicaAssignment [][]int32
}

// TopicCollection represents a collection of topics.
type TopicCollection struct {
	// Slice of topic names.
	topicNames []string
}

// NewTopicCollectionOfTopicNames creates a new TopicCollection based on a list
// of topic names.
func NewTopicCollectionOfTopicNames(names []string) TopicCollection {
	return TopicCollection{
		topicNames: names,
	}
}

// TopicPartitionInfo represents a specific partition's information inside a
// TopicDescription.
type TopicPartitionInfo struct {
	// Partition id.
	Partition int
	// Leader broker.
	Leader *Node
	// Replicas of the partition.
	Replicas []Node
	// In-Sync-Replicas of the partition.
	Isr []Node
}

// TopicDescription represents the result of DescribeTopics for
// a single topic.
type TopicDescription struct {
	// Topic name.
	Name string
	// Topic Id
	TopicID UUID
	// Error, if any, of the result. Check with `Error.Code() != ErrNoError`.
	Error Error
	// Is the topic internal to Kafka?
	IsInternal bool
	// Partitions' information list.
	Partitions []TopicPartitionInfo
	// Operations allowed for the topic (nil if not available or not requested).
	AuthorizedOperations []ACLOperation
}

// DescribeTopicsResult represents the result of a
// DescribeTopics call.
type DescribeTopicsResult struct {
	// Slice of TopicDescription.
	TopicDescriptions []TopicDescription
}

// cToTopicResults converts a C topic_result_t array to Go TopicResult list.
func (a *AdminClient) cToTopicResults(cTopicRes **C.rd_kafka_topic_result_t, cCnt C.size_t) (result []TopicResult, err error) {
	result = make([]TopicResult, int(cCnt))

	for i := 0; i < int(cCnt); i++ {
		cTopic := C.topic_result_by_idx(cTopicRes, cCnt, C.size_t(i))
		result[i].Topic = C.GoString(C.rd_kafka_topic_result_name(cTopic))
		result[i].Error = newErrorFromCString(
			C.rd_kafka_topic_result_error(cTopic),
			C.rd_kafka_topic_result_error_string(cTopic))
	}

	return result, nil
}

// cToTopicPartitionInfo converts a C TopicPartitionInfo_t into a Go
// TopicPartitionInfo.
func (a *AdminClient) cToTopicPartitionInfo(
	partitionInfo *C.rd_kafka_TopicPartitionInfo_t) TopicPartitionInfo {
	cPartitionID := C.rd_kafka_TopicPartitionInfo_partition(partitionInfo)
	info := TopicPartitionInfo{
		Partition: int(cPartitionID),
	}

	cLeader := C.rd_kafka_TopicPartitionInfo_leader(partitionInfo)
	info.Leader = a.cToNodePtr(cLeader)

	cReplicaCnt := C.size_t(0)
	cReplicas := C.rd_kafka_TopicPartitionInfo_replicas(
		partitionInfo, &cReplicaCnt)
	info.Replicas = a.cToNodes(cReplicas, cReplicaCnt)

	cIsrCnt := C.size_t(0)
	cIsr := C.rd_kafka_TopicPartitionInfo_isr(partitionInfo, &cIsrCnt)
	info.Isr = a.cToNodes(cIsr, cIsrCnt)

	return info
}

// cToTopicDescriptions converts a C TopicDescription_t
// array to a Go TopicDescription list.
func (a *AdminClient) cToTopicDescriptions(
	cTopicDescriptions **C.rd_kafka_TopicDescription_t,
	cTopicDescriptionCount C.size_t) (result []TopicDescription) {
	result = make([]TopicDescription, cTopicDescriptionCount)
	for idx := 0; idx < int(cTopicDescriptionCount); idx++ {
		cTopic := C.TopicDescription_by_idx(
			cTopicDescriptions, cTopicDescriptionCount, C.size_t(idx))

		topicName := C.GoString(
			C.rd_kafka_TopicDescription_name(cTopic))
		TopicID := a.cToUUID(C.rd_kafka_TopicDescription_topic_id(cTopic))
		err := newErrorFromCError(
			C.rd_kafka_TopicDescription_error(cTopic))

		if err.Code() != ErrNoError {
			result[idx] = TopicDescription{
				Name:  topicName,
				Error: err,
			}
			continue
		}

		cPartitionInfoCnt := C.size_t(0)
		cPartitionInfos := C.rd_kafka_TopicDescription_partitions(cTopic, &cPartitionInfoCnt)

		partitions := make([]TopicPartitionInfo, int(cPartitionInfoCnt))

		for pidx := 0; pidx < int(cPartitionInfoCnt); pidx++ {
			cPartitionInfo := C.TopicPartitionInfo_by_idx(cPartitionInfos, cPartitionInfoCnt, C.size_t(pidx))
			partitions[pidx] = a.cToTopicPartitionInfo(cPartitionInfo)
		}

		cAuthorizedOperationsCnt := C.size_t(0)
		cAuthorizedOperations := C.rd_kafka_TopicDescription_authorized_operations(
			cTopic, &cAuthorizedOperationsCnt)
		authorizedOperations := a.cToAuthorizedOperations(cAuthorizedOperations, cAuthorizedOperationsCnt)

		result[idx] = TopicDescription{
			Name:                 topicName,
			TopicID:              TopicID,
			Error:                err,
			Partitions:           partitions,
			AuthorizedOperations: authorizedOperations,
		}
	}
	return result
}

// CreateTopics creates topics in cluster.
//
// The list of TopicSpecification objects define the per-topic partition count, replicas, etc.
//
// Topic creation is non-atomic and may succeed for some topics while fail for others,
// make sure to check the result for topic-specific errors.
//
// Note: TopicSpecification.ReplicationFactor and TopicSpecification.ReplicaAssignment
// are mutually exclusive, either method may be used to set up the desired replica count
// and assignment, but not both.
//
// Requires broker version >= 0.10.1.0
func (a *AdminClient) CreateTopics(ctx context.Context, topics []TopicSpecification, options ...CreateTopicsAdminOption) (result []TopicResult, err error) {
	err = a.verifyClient()
	if err != nil {
		return nil, err
	}

	cTopics := make([]*C.rd_kafka_NewTopic_t, len(topics))

	cErrstrSize := C.size_t(512)
	cErrstr := (*C.char)(C.malloc(cErrstrSize))
	defer C.free(unsafe.Pointer(cErrstr))

	// Convert Go TopicSpecifications to C TopicSpecifications
	for i, topic := range topics {

		var cReplicationFactor C.int
		if topic.ReplicationFactor == 0 {
			cReplicationFactor = -1
		} else {
			cReplicationFactor = C.int(topic.ReplicationFactor)
		}
		if topic.ReplicaAssignment != nil {
			if cReplicationFactor != -1 {
				return nil, newErrorFromString(ErrInvalidArg,
					"TopicSpecification.ReplicationFactor and TopicSpecification.ReplicaAssignment are mutually exclusive")
			}

			if len(topic.ReplicaAssignment) != topic.NumPartitions {
				return nil, newErrorFromString(ErrInvalidArg,
					"TopicSpecification.ReplicaAssignment must contain exactly TopicSpecification.NumPartitions partitions")
			}
		}

		cTopics[i] = C.rd_kafka_NewTopic_new(
			C.CString(topic.Topic),
			C.int(topic.NumPartitions),
			cReplicationFactor,
			cErrstr, cErrstrSize)
		if cTopics[i] == nil {
			return nil, newErrorFromString(ErrInvalidArg,
				fmt.Sprintf("Topic %s: %s", topic.Topic, C.GoString(cErrstr)))
		}

		defer C.rd_kafka_NewTopic_destroy(cTopics[i])

		for p, replicas := range topic.ReplicaAssignment {
			cReplicas := make([]C.int32_t, len(replicas))
			for ri, replica := range replicas {
				cReplicas[ri] = C.int32_t(replica)
			}
			cErr := C.rd_kafka_NewTopic_set_replica_assignment(
				cTopics[i], C.int32_t(p),
				(*C.int32_t)(&cReplicas[0]), C.size_t(len(cReplicas)),
				cErrstr, cErrstrSize)
			if cErr != 0 {
				return nil, newCErrorFromString(cErr,
					fmt.Sprintf("Failed to set replica assignment for topic %s partition %d: %s", topic.Topic, p, C.GoString(cErrstr)))
			}
		}

		for key, value := range topic.Config {
			cErr := C.rd_kafka_NewTopic_set_config(
				cTopics[i],
				C.CString(key), C.CString(value))
			if cErr != 0 {
				return nil, newCErrorFromString(cErr,
					fmt.Sprintf("Failed to set config %s=%s for topic %s", key, value, topic.Topic))
			}
		}
	}

	// Convert Go AdminOptions (if any) to C AdminOptions
	genericOptions := make([]AdminOption, len(options))
	for i := range options {
		genericOptions[i] = options[i]
	}
	cOptions, err := adminOptionsSetup(a.handle, C.RD_KAFKA_ADMIN_OP_CREATETOPICS, genericOptions)
	if err != nil {
		return nil, err
	}
	defer C.rd_kafka_AdminOptions_destroy(cOptions)

	// Create temporary queue for async operation
	cQueue := C.rd_kafka_queue_new(a.handle.rk)
	defer C.rd_kafka_queue_destroy(cQueue)

	// Asynchronous call
	C.rd_kafka_CreateTopics(
		a.handle.rk,
		(**C.rd_kafka_NewTopic_t)(&cTopics[0]),
		C.size_t(len(cTopics)),
		cOptions,
		cQueue)

	// Wait for result, error or context timeout
	rkev, err := a.waitResult(ctx, cQueue, C.RD_KAFKA_EVENT_CREATETOPICS_RESULT)
	if err != nil {
		return nil, err
	}
	defer C.rd_kafka_event_destroy(rkev)

	cRes := C.rd_kafka_event_CreateTopics_result(rkev)

	// Convert result from C to Go
	var cCnt C.size_t
	cTopicRes := C.rd_kafka_CreateTopics_result_topics(cRes, &cCnt)

	return a.cToTopicResults(cTopicRes, cCnt)
}

// DeleteTopics deletes a batch of topics.
//
// This operation is not transactional and may succeed for a subset of topics while
// failing others.
// It may take several seconds after the DeleteTopics result returns success for
// all the brokers to become aware that the topics are gone. During this time,
// topic metadata and configuration may continue to return information about deleted topics.
//
// Requires broker version >= 0.10.1.0
func (a *AdminClient) DeleteTopics(ctx context.Context, topics []string, options ...DeleteTopicsAdminOption) (result []TopicResult, err error) {
	err = a.verifyClient()
	if err != nil {
		return nil, err
	}

	cTopics := make([]*C.rd_kafka_DeleteTopic_t, len(topics))

	cErrstrSize := C.size_t(512)
	cErrstr := (*C.char)(C.malloc(cErrstrSize))
	defer C.free(unsafe.Pointer(cErrstr))

	// Convert Go DeleteTopics to C DeleteTopics
	for i, topic := range topics {
		cTopics[i] = C.rd_kafka_DeleteTopic_new(C.CString(topic))
		if cTopics[i] == nil {
			return nil, newErrorFromString(ErrInvalidArg,
				fmt.Sprintf("Invalid arguments for topic %s", topic))
		}

		defer C.rd_kafka_DeleteTopic_destroy(cTopics[i])
	}

	// Convert Go AdminOptions (if any) to C AdminOptions
	genericOptions := make([]AdminOption, len(options))
	for i := range options {
		genericOptions[i] = options[i]
	}
	cOptions, err := adminOptionsSetup(a.handle, C.RD_KAFKA_ADMIN_OP_DELETETOPICS, genericOptions)
	if err != nil {
		return nil, err
	}
	defer C.rd_kafka_AdminOptions_destroy(cOptions)

	// Create temporary queue for async operation
	cQueue := C.rd_kafka_queue_new(a.handle.rk)
	defer C.rd_kafka_queue_destroy(cQueue)

	// Asynchronous call
	C.rd_kafka_DeleteTopics(
		a.handle.rk,
		(**C.rd_kafka_DeleteTopic_t)(&cTopics[0]),
		C.size_t(len(cTopics)),
		cOptions,
		cQueue)

	// Wait for result, error or context timeout
	rkev, err := a.waitResult(ctx, cQueue, C.RD_KAFKA_EVENT_DELETETOPICS_RESULT)
	if err != nil {
		return nil, err
	}
	defer C.rd_kafka_event_destroy(rkev)

	cRes := C.rd_kafka_event_DeleteTopics_result(rkev)

	// Convert result from C to Go
	var cCnt C.size_t
	cTopicRes := C.rd_kafka_DeleteTopics_result_topics(cRes, &cCnt)

	return a.cToTopicResults(cTopicRes, cCnt)
}

// CreatePartitions creates additional partitions for topics.
func (a *AdminClient) CreatePartitions(ctx context.Context, partitions []PartitionsSpecification, options ...CreatePartitionsAdminOption) (result []TopicResult, err error) {
	err = a.verifyClient()
	if err != nil {
		return nil, err
	}

	cParts := make([]*C.rd_kafka_NewPartitions_t, len(partitions))

	cErrstrSize := C.size_t(512)
	cErrstr := (*C.char)(C.malloc(cErrstrSize))
	defer C.free(unsafe.Pointer(cErrstr))

	// Convert Go PartitionsSpecification to C NewPartitions
	for i, part := range partitions {
		cParts[i] = C.rd_kafka_NewPartitions_new(C.CString(part.Topic), C.size_t(part.IncreaseTo), cErrstr, cErrstrSize)
		if cParts[i] == nil {
			return nil, newErrorFromString(ErrInvalidArg,
				fmt.Sprintf("Topic %s: %s", part.Topic, C.GoString(cErrstr)))
		}

		defer C.rd_kafka_NewPartitions_destroy(cParts[i])

		for pidx, replicas := range part.ReplicaAssignment {
			cReplicas := make([]C.int32_t, len(replicas))
			for ri, replica := range replicas {
				cReplicas[ri] = C.int32_t(replica)
			}
			cErr := C.rd_kafka_NewPartitions_set_replica_assignment(
				cParts[i], C.int32_t(pidx),
				(*C.int32_t)(&cReplicas[0]), C.size_t(len(cReplicas)),
				cErrstr, cErrstrSize)
			if cErr != 0 {
				return nil, newCErrorFromString(cErr,
					fmt.Sprintf("Failed to set replica assignment for topic %s new partition index %d: %s", part.Topic, pidx, C.GoString(cErrstr)))
			}
		}

	}

	// Convert Go AdminOptions (if any) to C AdminOptions
	genericOptions := make([]AdminOption, len(options))
	for i := range options {
		genericOptions[i] = options[i]
	}
	cOptions, err := adminOptionsSetup(a.handle, C.RD_KAFKA_ADMIN_OP_CREATEPARTITIONS, genericOptions)
	if err != nil {
		return nil, err
	}
	defer C.rd_kafka_AdminOptions_destroy(cOptions)

	// Create temporary queue for async operation
	cQueue := C.rd_kafka_queue_new(a.handle.rk)
	defer C.rd_kafka_queue_destroy(cQueue)

	// Asynchronous call
	C.rd_kafka_CreatePartitions(
		a.handle.rk,
		(**C.rd_kafka_NewPartitions_t)(&cParts[0]),
		C.size_t(len(cParts)),
		cOptions,
		cQueue)

	// Wait for result, error or context timeout
	rkev, err := a.waitResult(ctx, cQueue, C.RD_KAFKA_EVENT_CREATEPARTITIONS_RESULT)
	if err != nil {
		return nil, err
	}
	defer C.rd_kafka_event_destroy(rkev)

	cRes := C.rd_kafka_event_CreatePartitions_result(rkev)

	// Convert result from C to Go
	var cCnt C.size_t
	cTopicRes := C.rd_kafka_CreatePartitions_result_topics(cRes, &cCnt)

	return a.cToTopicResults(cTopicRes, cCnt)
}

// DescribeTopics describes topics.
//
// Parameters:
//   - `ctx` - context with the maximum amount of time to block, or nil for
//     indefinite.
//   - `topics` - TopicCollection with the topic names to describe.
//   - `options` - DescribeTopicsAdminOption options.
//
// Returns DescribeTopicsResult, which contains a slice of TopicDescription for
// each topic in the cluster. It contains the topic name, topic ID, partitions
// information, and any error, if occurred.
func (a *AdminClient) DescribeTopics(
	ctx context.Context, topics TopicCollection,
	options ...DescribeTopicsAdminOption) (result DescribeTopicsResult, err error) {

	describeResult := DescribeTopicsResult{}
	err = a.verifyClient()
	if err != nil {
		return result, err
	}

	// Convert topic names into char**.
	cTopicNameList := make([]*C.char, len(topics.topicNames))
	cTopicNameCount := C.size_t(len(topics.topicNames))

	if topics.topicNames == nil {
		return describeResult, newErrorFromString(ErrInvalidArg,
			"TopicCollection of topic names cannot be nil")
	}

	for idx, topic := range topics.topicNames {
		cTopicNameList[idx] = C.CString(topic)
		defer C.free(unsafe.Pointer(cTopicNameList[idx]))
	}

	var cTopicNameListPtr **C.char
	if cTopicNameCount > 0 {
		cTopicNameListPtr = ((**C.char)(&cTopicNameList[0]))
	}

	// Convert char** of topic names into rd_kafka_TopicCollection_t*
	cTopicCollection := C.rd_kafka_TopicCollection_of_topic_names(
		cTopicNameListPtr, cTopicNameCount)
	defer C.rd_kafka_TopicCollection_destroy(cTopicCollection)

	// Convert Go AdminOptions (if any) to C AdminOptions.
	genericOptions := make([]AdminOption, len(options))
	for i := range options {
		genericOptions[i] = options[i]
	}
	cOptions, err := adminOptionsSetup(
		a.handle, C.RD_KAFKA_ADMIN_OP_DESCRIBETOPICS, genericOptions)
	if err != nil {
		return describeResult, err
	}
	defer C.rd_kafka_AdminOptions_destroy(cOptions)

	// Create temporary queue for async operation.
	cQueue := C.rd_kafka_queue_new(a.handle.rk)
	defer C.rd_kafka_queue_destroy(cQueue)

	// Call rd_kafka_DescribeTopics (asynchronous).
	C.rd_kafka_DescribeTopics(
		a.handle.rk,
		cTopicCollection,
		cOptions,
		cQueue)

	// Wait for result, error or context timeout.
	rkev, err := a.waitResult(
		ctx, cQueue, C.RD_KAFKA_EVENT_DESCRIBETOPICS_RESULT)
	if err != nil {
		return describeResult, err
	}
	defer C.rd_kafka_event_destroy(rkev)

	cRes := C.rd_kafka_event_DescribeTopics_result(rkev)

	// Convert result from C to Go.
	var cTopicDescriptionCount C.size_t
	cTopicDescriptions :=
		C.rd_kafka_DescribeTopics_result_topics(cRes, &cTopicDescriptionCount)
	describeResult.TopicDescriptions =
		a.cToTopicDescriptions(cTopicDescriptions, cTopicDescriptionCount)

	return describeResult, nil
}
