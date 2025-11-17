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
	"time"
	"unsafe"
)

/*
#include "select_rdkafka.h"
#include <stdlib.h>

static const rd_kafka_group_result_t *
group_result_by_idx (const rd_kafka_group_result_t **groups, size_t cnt, size_t idx) {
    if (idx >= cnt)
      return NULL;
    return groups[idx];
}

static const rd_kafka_topic_result_t *
topic_result_by_idx (const rd_kafka_topic_result_t **topics, size_t cnt, size_t idx) {
    if (idx >= cnt)
      return NULL;
    return topics[idx];
}

static const rd_kafka_ConfigResource_t *
ConfigResource_by_idx (const rd_kafka_ConfigResource_t **res, size_t cnt, size_t idx) {
    if (idx >= cnt)
      return NULL;
    return res[idx];
}

static const rd_kafka_ConfigEntry_t *
ConfigEntry_by_idx (const rd_kafka_ConfigEntry_t **entries, size_t cnt, size_t idx) {
    if (idx >= cnt)
      return NULL;
    return entries[idx];
}

static const rd_kafka_acl_result_t *
acl_result_by_idx (const rd_kafka_acl_result_t **acl_results, size_t cnt, size_t idx) {
    if (idx >= cnt)
      return NULL;
    return acl_results[idx];
}

static const rd_kafka_DeleteAcls_result_response_t *
DeleteAcls_result_response_by_idx (const rd_kafka_DeleteAcls_result_response_t **delete_acls_result_responses, size_t cnt, size_t idx) {
    if (idx >= cnt)
      return NULL;
    return delete_acls_result_responses[idx];
}

static const rd_kafka_AclBinding_t *
AclBinding_by_idx (const rd_kafka_AclBinding_t **acl_bindings, size_t cnt, size_t idx) {
    if (idx >= cnt)
      return NULL;
    return acl_bindings[idx];
}

static const rd_kafka_ConsumerGroupListing_t *
ConsumerGroupListing_by_idx(const rd_kafka_ConsumerGroupListing_t **result_groups, size_t cnt, size_t idx) {
	if (idx >= cnt)
		return NULL;
	return result_groups[idx];
}

static const rd_kafka_ConsumerGroupDescription_t *
ConsumerGroupDescription_by_idx(const rd_kafka_ConsumerGroupDescription_t **result_groups, size_t cnt, size_t idx) {
	if (idx >= cnt)
		return NULL;
	return result_groups[idx];
}

static const rd_kafka_TopicDescription_t *
TopicDescription_by_idx(const rd_kafka_TopicDescription_t **result_topics, size_t cnt, size_t idx) {
	if (idx >= cnt)
		return NULL;
	return result_topics[idx];
}

static const rd_kafka_TopicPartitionInfo_t *
TopicPartitionInfo_by_idx(const rd_kafka_TopicPartitionInfo_t **partitions, size_t cnt, size_t idx) {
	if (idx >= cnt)
		return NULL;
	return partitions[idx];
}

static const rd_kafka_AclOperation_t AclOperation_by_idx(const rd_kafka_AclOperation_t *acl_operations, size_t cnt, size_t idx) {
	if (idx >= cnt)
		return RD_KAFKA_ACL_OPERATION_UNKNOWN;
	return acl_operations[idx];
}

static const rd_kafka_Node_t *Node_by_idx(const rd_kafka_Node_t **nodes, size_t cnt, size_t idx) {
	if (idx >= cnt)
		return NULL;
	return nodes[idx];
}

static const rd_kafka_UserScramCredentialsDescription_t *
DescribeUserScramCredentials_result_description_by_idx(const rd_kafka_UserScramCredentialsDescription_t **descriptions, size_t cnt, size_t idx) {
	if (idx >= cnt)
		return NULL;
	return descriptions[idx];
}

static const rd_kafka_AlterUserScramCredentials_result_response_t*
AlterUserScramCredentials_result_response_by_idx(const rd_kafka_AlterUserScramCredentials_result_response_t **responses, size_t cnt, size_t idx) {
	if (idx >= cnt)
		return NULL;
	return responses[idx];
}

static const rd_kafka_ListOffsetsResultInfo_t *
ListOffsetsResultInfo_by_idx(const rd_kafka_ListOffsetsResultInfo_t **result_infos, size_t cnt, size_t idx) {
	if (idx >= cnt)
		return NULL;
	return result_infos[idx];
}

static const rd_kafka_error_t *
error_by_idx(const rd_kafka_error_t **errors, size_t cnt, size_t idx) {
	if (idx >= cnt)
		return NULL;
	return errors[idx];
}

static const rd_kafka_topic_partition_result_t *
TopicPartitionResult_by_idx(const rd_kafka_topic_partition_result_t **results, size_t cnt, size_t idx) {
	if (idx >= cnt)
		return NULL;
	return results[idx];
}
*/
import "C"

func durationToMilliseconds(t time.Duration) int {
	if t > 0 {
		return (int)(t.Seconds() * 1000.0)
	}
	return (int)(t)
}

// waitResult waits for a result event on cQueue or the ctx to be cancelled, whichever happens
// first.
// The returned result event is checked for errors its error is returned if set.
func (a *AdminClient) waitResult(ctx context.Context, cQueue *C.rd_kafka_queue_t, cEventType C.rd_kafka_event_type_t) (rkev *C.rd_kafka_event_t, err error) {
	resultChan := make(chan *C.rd_kafka_event_t)
	closeChan := make(chan bool) // never written to, just closed

	go func() {
		for {
			select {
			case _, ok := <-closeChan:
				if !ok {
					// Context cancelled/timed out
					close(resultChan)
					return
				}

			default:
				// Wait for result event for at most 50ms
				// to avoid blocking for too long if
				// context is cancelled.
				rkev := C.rd_kafka_queue_poll(cQueue, 50)
				if rkev != nil {
					resultChan <- rkev
					close(resultChan)
					return
				}
			}
		}
	}()

	select {
	case rkev = <-resultChan:
		// Result type check
		if cEventType != C.rd_kafka_event_type(rkev) {
			err = newErrorFromString(ErrInvalidType,
				fmt.Sprintf("Expected %d result event, not %d", (int)(cEventType), (int)(C.rd_kafka_event_type(rkev))))
			C.rd_kafka_event_destroy(rkev)
			return nil, err
		}

		// Generic error handling
		cErr := C.rd_kafka_event_error(rkev)
		if cErr != 0 {
			err = newErrorFromCString(cErr, C.rd_kafka_event_error_string(rkev))
			C.rd_kafka_event_destroy(rkev)
			return nil, err
		}
		close(closeChan)
		return rkev, nil
	case <-ctx.Done():
		// signal close to go-routine
		close(closeChan)
		// wait for close from go-routine to make sure it is done
		// using cQueue before we return.
		rkev, ok := <-resultChan
		if ok {
			// throw away result since context was cancelled
			C.rd_kafka_event_destroy(rkev)
		}
		return nil, ctx.Err()
	}
}

// cToAuthorizedOperations converts a C AclOperation_t array to a Go
// ACLOperation list.
func (a *AdminClient) cToAuthorizedOperations(
	cAuthorizedOperations *C.rd_kafka_AclOperation_t,
	cAuthorizedOperationCnt C.size_t) []ACLOperation {
	if cAuthorizedOperations == nil {
		return nil
	}

	authorizedOperations := make([]ACLOperation, int(cAuthorizedOperationCnt))
	for i := 0; i < int(cAuthorizedOperationCnt); i++ {
		cAuthorizedOperation := C.AclOperation_by_idx(
			cAuthorizedOperations, cAuthorizedOperationCnt, C.size_t(i))
		authorizedOperations[i] = ACLOperation(cAuthorizedOperation)
	}

	return authorizedOperations
}

// cToUUID converts a C rd_kafka_Uuid_t to a Go UUID.
func (a *AdminClient) cToUUID(cUUID *C.rd_kafka_Uuid_t) UUID {
	uuid := UUID{
		mostSignificantBits:  int64(C.rd_kafka_Uuid_most_significant_bits(cUUID)),
		leastSignificantBits: int64(C.rd_kafka_Uuid_least_significant_bits(cUUID)),
		base64str:            C.GoString(C.rd_kafka_Uuid_base64str(cUUID)),
	}
	return uuid
}

// cToNode converts a C Node_t* to a Go Node.
// If cNode is nil returns a Node with ID: -1.
func (a *AdminClient) cToNode(cNode *C.rd_kafka_Node_t) Node {
	if cNode == nil {
		return Node{
			ID: -1,
		}
	}

	node := Node{
		ID:   int(C.rd_kafka_Node_id(cNode)),
		Host: C.GoString(C.rd_kafka_Node_host(cNode)),
		Port: int(C.rd_kafka_Node_port(cNode)),
	}

	cRack := C.rd_kafka_Node_rack(cNode)
	if cRack != nil {
		rackID := C.GoString(cRack)
		node.Rack = &rackID
	}

	return node
}

// cToNodePtr converts a C Node_t* to a Go *Node.
func (a *AdminClient) cToNodePtr(cNode *C.rd_kafka_Node_t) *Node {
	if cNode == nil {
		return nil
	}

	node := a.cToNode(cNode)
	return &node
}

// cToNode converts a C Node_t array to a Go Node list.
func (a *AdminClient) cToNodes(
	cNodes **C.rd_kafka_Node_t, cNodeCnt C.size_t) []Node {
	nodes := make([]Node, int(cNodeCnt))
	for i := 0; i < int(cNodeCnt); i++ {
		cNode := C.Node_by_idx(cNodes, cNodeCnt, C.size_t(i))
		nodes[i] = a.cToNode(cNode)
	}
	return nodes
}

// cToErrorList converts a C rd_kafka_error_t array to a Go errors slice.
func (a *AdminClient) cToErrorList(
	cErrs **C.rd_kafka_error_t, cErrCount C.size_t) (errs []error) {
	errs = make([]error, cErrCount)

	for idx := 0; idx < int(cErrCount); idx++ {
		cErr := C.error_by_idx(cErrs, cErrCount, C.size_t(idx))
		errs[idx] = newErrorFromCError(cErr)
	}

	return errs
}
