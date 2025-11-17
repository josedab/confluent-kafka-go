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
	"fmt"
	"sync/atomic"
	"unsafe"
)

/*
#include "select_rdkafka.h"
*/
import "C"

// AdminClient is derived from an existing Producer or Consumer
type AdminClient struct {
	handle        *handle
	isDerived     bool      // Derived from existing client handle
	isClosed      uint32    // to check if Admin Client is closed or not.
	adminTermChan chan bool // For log channel termination
}

// IsClosed returns boolean representing if client is closed or not
func (a *AdminClient) IsClosed() bool {
	return atomic.LoadUint32(&a.isClosed) == 1
}

func (a *AdminClient) verifyClient() error {
	if a.IsClosed() {
		return getOperationNotAllowedErrorForClosedClient()
	}
	return nil
}

// GetMetadata queries broker for cluster and topic metadata.
// If topic is non-nil only information about that topic is returned, else if
// allTopics is false only information about locally used topics is returned,
// else information about all topics is returned.
// GetMetadata is equivalent to listTopics, describeTopics and describeCluster in the Java API.
func (a *AdminClient) GetMetadata(topic *string, allTopics bool, timeoutMs int) (*Metadata, error) {
	err := a.verifyClient()
	if err != nil {
		return nil, err
	}
	return getMetadata(a, topic, allTopics, timeoutMs)
}

// String returns a human readable name for an AdminClient instance
func (a *AdminClient) String() string {
	return fmt.Sprintf("admin-%s", a.handle.String())
}

// get_handle implements the Handle interface
func (a *AdminClient) gethandle() *handle {
	return a.handle
}

// SetOAuthBearerToken sets the the data to be transmitted
// to a broker during SASL/OAUTHBEARER authentication. It will return nil
// on success, otherwise an error if:
// 1) the token data is invalid (meaning an expiration time in the past
// or either a token value or an extension key or value that does not meet
// the regular expression requirements as per
// https://tools.ietf.org/html/rfc7628#section-3.1);
// 2) SASL/OAUTHBEARER is not supported by the underlying librdkafka build;
// 3) SASL/OAUTHBEARER is supported but is not configured as the client's
// authentication mechanism.
func (a *AdminClient) SetOAuthBearerToken(oauthBearerToken OAuthBearerToken) error {
	err := a.verifyClient()
	if err != nil {
		return err
	}
	return a.handle.setOAuthBearerToken(oauthBearerToken)
}

// SetOAuthBearerTokenFailure sets the error message describing why token
// retrieval/setting failed; it also schedules a new token refresh event for 10
// seconds later so the attempt may be retried. It will return nil on
// success, otherwise an error if:
// 1) SASL/OAUTHBEARER is not supported by the underlying librdkafka build;
// 2) SASL/OAUTHBEARER is supported but is not configured as the client's
// authentication mechanism.
func (a *AdminClient) SetOAuthBearerTokenFailure(errstr string) error {
	err := a.verifyClient()
	if err != nil {
		return err
	}
	return a.handle.setOAuthBearerTokenFailure(errstr)
}

// SetSaslCredentials sets the SASL credentials used for this client.
// These credentials will overwrite the old ones, and will be used the next
// time the client needs to authenticate.
// This method will not disconnect existing broker connections that have been
// established with the old credentials.
// This method is applicable only to SASL PLAIN and SCRAM mechanisms.
//
// This method applies only to the SASL PLAIN and SCRAM mechanisms.
func (a *AdminClient) SetSaslCredentials(username, password string) error {
	err := a.verifyClient()
	if err != nil {
		return err
	}

	return setSaslCredentials(a.handle.rk, username, password)
}

// Close an AdminClient instance.
func (a *AdminClient) Close() {
	if !atomic.CompareAndSwapUint32(&a.isClosed, 0, 1) {
		return
	}
	if a.isDerived {
		// Derived AdminClient needs no cleanup.
		a.handle = &handle{}
		return
	}

	if a.adminTermChan != nil {
		close(a.adminTermChan)
	}

	// Wait for the log polling goroutine to terminate before cleanup
	a.handle.waitGroup.Wait()

	a.handle.cleanup()

	C.rd_kafka_destroy(a.handle.rk)
}

// Logs returns a channel which receives log messages from the internal librdkafka client.
// If the channel is not read from, logs will be dropped.
// To enable this channel, set "go.logs.channel.enable" to true in the configuration.
func (a *AdminClient) Logs() chan LogEvent {
	return a.handle.logs
}

// NewAdminClient creats a new AdminClient instance with a new underlying client instance
func NewAdminClient(conf *ConfigMap) (*AdminClient, error) {

	err := versionCheck()
	if err != nil {
		return nil, err
	}

	a := &AdminClient{}
	a.handle = &handle{}
	a.isClosed = 0

	// before we do anything with the configuration, create a copy such that
	// the original is not mutated.
	confCopy := conf.clone()

	logsChanEnable, logsChan, err := confCopy.extractLogConfig()
	if err != nil {
		return nil, err
	}

	// Convert ConfigMap to librdkafka conf_t
	cConf, err := confCopy.convert()
	if err != nil {
		return nil, err
	}

	cErrstr := (*C.char)(C.malloc(C.size_t(256)))
	defer C.free(unsafe.Pointer(cErrstr))

	C.rd_kafka_conf_set_events(cConf, C.RD_KAFKA_EVENT_STATS|C.RD_KAFKA_EVENT_ERROR|C.RD_KAFKA_EVENT_OAUTHBEARER_TOKEN_REFRESH)

	// Create librdkafka producer instance. The Producer is somewhat cheaper than
	// the consumer, but any instance type can be used for Admin APIs.
	a.handle.rk = C.rd_kafka_new(C.RD_KAFKA_PRODUCER, cConf, cErrstr, 256)
	if a.handle.rk == nil {
		return nil, newErrorFromCString(C.RD_KAFKA_RESP_ERR__INVALID_ARG, cErrstr)
	}

	a.isDerived = false
	a.handle.setup()

	// Setup log channel if enabled
	if logsChanEnable {
		a.adminTermChan = make(chan bool)
		a.handle.setupLogQueue(logsChan, a.adminTermChan)
	}

	return a, nil
}

// NewAdminClientFromProducer derives a new AdminClient from an existing Producer instance.
// The AdminClient will use the same configuration and connections as the parent instance.
func NewAdminClientFromProducer(p *Producer) (a *AdminClient, err error) {
	if p.handle.rk == nil {
		return nil, newErrorFromString(ErrInvalidArg, "Can't derive AdminClient from closed producer")
	}

	a = &AdminClient{}
	a.handle = &p.handle
	a.isDerived = true
	a.isClosed = 0
	return a, nil
}

// NewAdminClientFromConsumer derives a new AdminClient from an existing Consumer instance.
// The AdminClient will use the same configuration and connections as the parent instance.
func NewAdminClientFromConsumer(c *Consumer) (a *AdminClient, err error) {
	if c.handle.rk == nil {
		return nil, newErrorFromString(ErrInvalidArg, "Can't derive AdminClient from closed consumer")
	}

	a = &AdminClient{}
	a.handle = &c.handle
	a.isDerived = true
	a.isClosed = 0
	return a, nil
}
