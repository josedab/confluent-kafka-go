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

// DescribeClusterResult represents the result of DescribeCluster.
type DescribeClusterResult struct {
	// Cluster id for the cluster (always available if broker version >= 0.10.1.0, otherwise nil).
	ClusterID *string
	// Current controller broker for the cluster (nil if there is none).
	Controller *Node
	// List of brokers in the cluster.
	Nodes []Node
	// Operations allowed for the cluster (nil if not available or not requested).
	AuthorizedOperations []ACLOperation
}

// ElectionType represents the type of election to be performed
type ElectionType int

const (
	// ElectionTypePreferred - Preferred election type
	ElectionTypePreferred ElectionType = C.RD_KAFKA_ELECTION_TYPE_PREFERRED
	// ElectionTypeUnclean - Unclean election type
	ElectionTypeUnclean ElectionType = C.RD_KAFKA_ELECTION_TYPE_UNCLEAN
)

// ElectionTypeFromString translates an election type name to
// an ElectionType value.
func ElectionTypeFromString(electionTypeString string) (ElectionType, error) {
	switch strings.ToUpper(electionTypeString) {
	case "PREFERRED":
		return ElectionTypePreferred, nil
	case "UNCLEAN":
		return ElectionTypeUnclean, nil
	default:
		return ElectionTypePreferred, NewError(ErrInvalidArg, "Unknown election type", false)
	}
}

// ElectLeadersRequest holds parameters for the type of election to be performed and
// the topic partitions for which election has to be performed
type ElectLeadersRequest struct {
	// Election type to be performed
	electionType ElectionType
	// TopicPartitions for which election has to be performed
	partitions []TopicPartition
}

// NewElectLeadersRequest creates a new ElectLeadersRequest with the given election type
// and topic partitions
func NewElectLeadersRequest(electionType ElectionType, partitions []TopicPartition) ElectLeadersRequest {
	return ElectLeadersRequest{
		electionType: electionType,
		partitions:   partitions,
	}
}

// ElectLeadersResult holds the result of the election performed
type ElectLeadersResult struct {
	// TopicPartitions for which election has been performed and the per-partition error, if any
	// that occurred while running the election for the specific TopicPartition.
	TopicPartitions []TopicPartition
}

// cToDescribeClusterResult converts a C DescribeTopics_result_t to a Go
// DescribeClusterResult.
func (a *AdminClient) cToDescribeClusterResult(
	cResult *C.rd_kafka_DescribeTopics_result_t) (result DescribeClusterResult) {
	var clusterIDPtr *string = nil
	cClusterID := C.rd_kafka_DescribeCluster_result_cluster_id(cResult)
	if cClusterID != nil {
		clusterID := C.GoString(cClusterID)
		clusterIDPtr = &clusterID
	}

	var controller *Node = nil
	cController := C.rd_kafka_DescribeCluster_result_controller(cResult)
	controller = a.cToNodePtr(cController)

	cNodeCnt := C.size_t(0)
	cNodes := C.rd_kafka_DescribeCluster_result_nodes(cResult, &cNodeCnt)
	nodes := a.cToNodes(cNodes, cNodeCnt)

	cAuthorizedOperationsCnt := C.size_t(0)
	cAuthorizedOperations :=
		C.rd_kafka_DescribeCluster_result_authorized_operations(
			cResult, &cAuthorizedOperationsCnt)
	authorizedOperations := a.cToAuthorizedOperations(
		cAuthorizedOperations, cAuthorizedOperationsCnt)

	return DescribeClusterResult{
		ClusterID:            clusterIDPtr,
		Controller:           controller,
		Nodes:                nodes,
		AuthorizedOperations: authorizedOperations,
	}
}

// ClusterID returns the cluster ID as reported in broker metadata.
//
// Note on cancellation: Although the underlying C function respects the
// timeout, it currently cannot be manually cancelled. That means manually
// cancelling the context will block until the C function call returns.
//
// Requires broker version >= 0.10.0.
func (a *AdminClient) ClusterID(ctx context.Context) (clusterID string, err error) {
	err = a.verifyClient()
	if err != nil {
		return "", err
	}

	responseChan := make(chan *C.char, 1)

	go func() {
		responseChan <- C.rd_kafka_clusterid(a.handle.rk, cTimeoutFromContext(ctx))
	}()

	select {
	case <-ctx.Done():
		if cClusterID := <-responseChan; cClusterID != nil {
			C.rd_kafka_mem_free(a.handle.rk, unsafe.Pointer(cClusterID))
		}
		return "", ctx.Err()

	case cClusterID := <-responseChan:
		if cClusterID == nil { // C timeout
			<-ctx.Done()
			return "", ctx.Err()
		}
		defer C.rd_kafka_mem_free(a.handle.rk, unsafe.Pointer(cClusterID))
		return C.GoString(cClusterID), nil
	}
}

// ControllerID returns the broker ID of the current controller as reported in
// broker metadata.
//
// Note on cancellation: Although the underlying C function respects the
// timeout, it currently cannot be manually cancelled. That means manually
// cancelling the context will block until the C function call returns.
//
// Requires broker version >= 0.10.0.
func (a *AdminClient) ControllerID(ctx context.Context) (controllerID int32, err error) {
	err = a.verifyClient()
	if err != nil {
		return -1, err
	}

	responseChan := make(chan int32, 1)

	go func() {
		responseChan <- int32(C.rd_kafka_controllerid(a.handle.rk, cTimeoutFromContext(ctx)))
	}()

	select {
	case <-ctx.Done():
		<-responseChan
		return 0, ctx.Err()

	case controllerID := <-responseChan:
		if controllerID < 0 { // C timeout
			<-ctx.Done()
			return 0, ctx.Err()
		}
		return controllerID, nil
	}
}

// DescribeCluster describes the cluster
//
// Parameters:
//   - `ctx` - context with the maximum amount of time to block, or nil for
//     indefinite.
//   - `options` - DescribeClusterAdminOption options.
//
// Returns ClusterDescription, which contains current cluster ID and controller
// along with a slice of Nodes. It also has a slice of allowed ACLOperations.
func (a *AdminClient) DescribeCluster(
	ctx context.Context,
	options ...DescribeClusterAdminOption) (result DescribeClusterResult, err error) {
	err = a.verifyClient()
	if err != nil {
		return result, err
	}
	clusterDesc := DescribeClusterResult{}

	// Convert Go AdminOptions (if any) to C AdminOptions.
	genericOptions := make([]AdminOption, len(options))
	for i := range options {
		genericOptions[i] = options[i]
	}
	cOptions, err := adminOptionsSetup(
		a.handle, C.RD_KAFKA_ADMIN_OP_DESCRIBECLUSTER, genericOptions)
	if err != nil {
		return clusterDesc, err
	}
	defer C.rd_kafka_AdminOptions_destroy(cOptions)

	// Create temporary queue for async operation.
	cQueue := C.rd_kafka_queue_new(a.handle.rk)
	defer C.rd_kafka_queue_destroy(cQueue)

	// Call rd_kafka_DescribeCluster (asynchronous).
	C.rd_kafka_DescribeCluster(
		a.handle.rk,
		cOptions,
		cQueue)

	// Wait for result, error or context timeout.
	rkev, err := a.waitResult(
		ctx, cQueue, C.RD_KAFKA_EVENT_DESCRIBECLUSTER_RESULT)
	if err != nil {
		return clusterDesc, err
	}
	defer C.rd_kafka_event_destroy(rkev)

	cRes := C.rd_kafka_event_DescribeCluster_result(rkev)

	// Convert result from C to Go.
	clusterDesc = a.cToDescribeClusterResult(cRes)

	return clusterDesc, nil
}

// ElectLeaders performs Preferred or Unclean Elections for the specified topic Partitions or for all of them.
//
// Parameters:
//   - `ctx` - context with the maximum amount of time to block, or nil for
//     indefinite.
//   - `electLeaderRequest` - ElectLeadersRequest containing the election type
//     and the partitions to elect leaders for or nil for election in all the
//     partitions.
//   - `options` - ElectLeadersAdminOption options.
//
// Returns ElectLeadersResult, which contains a slice of TopicPartitions containing the partitions for which the leader election was performed.
// If we are passing partitions as nil, the broker will perform leader elections for all partitions,
// but the results will only contain partitions for which there was an election or resulted in an error.
// Individual TopicPartitions inside the ElectLeadersResult should be checked for errors.
// Additionally, an error that is not nil for client-level errors is returned.
func (a *AdminClient) ElectLeaders(ctx context.Context, electLeaderRequest ElectLeadersRequest, options ...ElectLeadersAdminOption) (result ElectLeadersResult, err error) {

	err = a.verifyClient()
	if err != nil {
		return result, err
	}

	var cTopicPartitions *C.rd_kafka_topic_partition_list_t
	if electLeaderRequest.partitions != nil {
		cTopicPartitions = newCPartsFromTopicPartitions(electLeaderRequest.partitions)
		defer C.rd_kafka_topic_partition_list_destroy(cTopicPartitions)
	}

	cElectLeadersRequest := C.rd_kafka_ElectLeaders_new(C.rd_kafka_ElectionType_t(electLeaderRequest.electionType), cTopicPartitions)
	defer C.rd_kafka_ElectLeaders_destroy(cElectLeadersRequest)

	// Convert Go AdminOptions (if any) to C AdminOptions.
	genericOptions := make([]AdminOption, len(options))
	for i := range options {
		genericOptions[i] = options[i]
	}
	cOptions, err := adminOptionsSetup(
		a.handle, C.RD_KAFKA_ADMIN_OP_ELECTLEADERS, genericOptions)
	if err != nil {
		return result, err
	}
	defer C.rd_kafka_AdminOptions_destroy(cOptions)

	// Create temporary queue for async operation.
	cQueue := C.rd_kafka_queue_new(a.handle.rk)
	defer C.rd_kafka_queue_destroy(cQueue)

	// Call rd_kafka_ElectLeader (asynchronous).
	C.rd_kafka_ElectLeaders(
		a.handle.rk,
		cElectLeadersRequest,
		cOptions,
		cQueue)

	// Wait for result, error or context timeout.
	rkev, err := a.waitResult(
		ctx, cQueue, C.RD_KAFKA_EVENT_ELECTLEADERS_RESULT)
	if err != nil {
		return result, err
	}
	defer C.rd_kafka_event_destroy(rkev)

	cRes := C.rd_kafka_event_ElectLeaders_result(rkev)
	var cResponseSize C.size_t

	cResultPartitions := C.rd_kafka_ElectLeaders_result_partitions(cRes, &cResponseSize)
	result.TopicPartitions = newTopicPartitionsFromCTopicPartitionResult(cResultPartitions, cResponseSize)

	return result, nil
}
