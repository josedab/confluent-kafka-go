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
	"strings"
	"unsafe"
)

/*
#include "select_rdkafka.h"
#include <stdlib.h>
*/
import "C"

// ScramMechanism enumerates SASL/SCRAM mechanisms.
// Used by `AdminClient.AlterUserScramCredentials`
// and `AdminClient.DescribeUserScramCredentials`.
type ScramMechanism int

const (
	// ScramMechanismUnknown - Unknown SASL/SCRAM mechanism
	ScramMechanismUnknown ScramMechanism = C.RD_KAFKA_SCRAM_MECHANISM_UNKNOWN
	// ScramMechanismSHA256 - SCRAM-SHA-256 mechanism
	ScramMechanismSHA256 ScramMechanism = C.RD_KAFKA_SCRAM_MECHANISM_SHA_256
	// ScramMechanismSHA512 - SCRAM-SHA-512 mechanism
	ScramMechanismSHA512 ScramMechanism = C.RD_KAFKA_SCRAM_MECHANISM_SHA_512
)

// String returns the human-readable representation of an ScramMechanism
func (o ScramMechanism) String() string {
	switch o {
	case ScramMechanismSHA256:
		return "SCRAM-SHA-256"
	case ScramMechanismSHA512:
		return "SCRAM-SHA-512"
	default:
		return "UNKNOWN"
	}
}

// ScramMechanismFromString translates a Scram Mechanism name to
// a ScramMechanism value.
func ScramMechanismFromString(mechanism string) (ScramMechanism, error) {
	switch strings.ToUpper(mechanism) {
	case "SCRAM-SHA-256":
		return ScramMechanismSHA256, nil
	case "SCRAM-SHA-512":
		return ScramMechanismSHA512, nil
	default:
		return ScramMechanismUnknown,
			NewError(ErrInvalidArg, "Unknown SCRAM mechanism", false)
	}
}

// ScramCredentialInfo contains Mechanism and Iterations for a
// SASL/SCRAM credential associated with a user.
type ScramCredentialInfo struct {
	// Iterations - positive number of iterations used when creating the credential
	Iterations int
	// Mechanism - SASL/SCRAM mechanism
	Mechanism ScramMechanism
}

// UserScramCredentialsDescription represent all SASL/SCRAM credentials
// associated with a user that can be retrieved, or an error indicating
// why credentials could not be retrieved.
type UserScramCredentialsDescription struct {
	// User - the user name.
	User string
	// ScramCredentialInfos - SASL/SCRAM credential representations for the user.
	ScramCredentialInfos []ScramCredentialInfo
	// Error - error corresponding to this user description.
	Error Error
}

// UserScramCredentialDeletion is a request to delete
// a SASL/SCRAM credential for a user.
type UserScramCredentialDeletion struct {
	// User - user name
	User string
	// Mechanism - SASL/SCRAM mechanism.
	Mechanism ScramMechanism
}

// UserScramCredentialUpsertion is a request to update/insert
// a SASL/SCRAM credential for a user.
type UserScramCredentialUpsertion struct {
	// User - user name
	User string
	// ScramCredentialInfo - the mechanism and iterations.
	ScramCredentialInfo ScramCredentialInfo
	// Password - password to HMAC before storage.
	Password []byte
	// Salt - salt to use. Will be generated randomly if nil. (optional)
	Salt []byte
}

// DescribeUserScramCredentialsResult represents the result of a
// DescribeUserScramCredentials call.
type DescribeUserScramCredentialsResult struct {
	// Descriptions - Map from user name
	// to UserScramCredentialsDescription
	Descriptions map[string]UserScramCredentialsDescription
}

// AlterUserScramCredentialsResult represents the result of a
// AlterUserScramCredentials call.
type AlterUserScramCredentialsResult struct {
	// Errors - Map from user name
	// to an Error, with ErrNoError code on success.
	Errors map[string]Error
}

// cToDescribeUserScramCredentialsResult converts a C
// rd_kafka_DescribeUserScramCredentials_result_t to a Go map of users to
// UserScramCredentialsDescription.
func cToDescribeUserScramCredentialsResult(
	cRes *C.rd_kafka_DescribeUserScramCredentials_result_t) map[string]UserScramCredentialsDescription {
	result := make(map[string]UserScramCredentialsDescription)
	var cDescriptionCount C.size_t
	cDescriptions :=
		C.rd_kafka_DescribeUserScramCredentials_result_descriptions(cRes,
			&cDescriptionCount)

	for i := 0; i < int(cDescriptionCount); i++ {
		cDescription :=
			C.DescribeUserScramCredentials_result_description_by_idx(
				cDescriptions, cDescriptionCount, C.size_t(i))
		user := C.GoString(C.rd_kafka_UserScramCredentialsDescription_user(cDescription))
		userDescription := UserScramCredentialsDescription{User: user}

		// Populate the error if required.
		cError := C.rd_kafka_UserScramCredentialsDescription_error(cDescription)
		if C.rd_kafka_error_code(cError) != C.RD_KAFKA_RESP_ERR_NO_ERROR {
			userDescription.Error = newError(C.rd_kafka_error_code(cError))
			result[user] = userDescription
			continue
		}

		cCredentialCount := C.rd_kafka_UserScramCredentialsDescription_scramcredentialinfo_count(cDescription)
		scramCredentialInfos := make([]ScramCredentialInfo, int(cCredentialCount))
		for j := 0; j < int(cCredentialCount); j++ {
			cScramCredentialInfo :=
				C.rd_kafka_UserScramCredentialsDescription_scramcredentialinfo(
					cDescription, C.size_t(j))
			cMechanism := C.rd_kafka_ScramCredentialInfo_mechanism(cScramCredentialInfo)
			cIterations := C.rd_kafka_ScramCredentialInfo_iterations(cScramCredentialInfo)
			scramCredentialInfos[j] = ScramCredentialInfo{
				Mechanism:  ScramMechanism(cMechanism),
				Iterations: int(cIterations),
			}
		}
		userDescription.ScramCredentialInfos = scramCredentialInfos
		result[user] = userDescription
	}
	return result
}

// DescribeUserScramCredentials describe SASL/SCRAM credentials for the
// specified user names.
//
// Parameters:
//   - `ctx` - context with the maximum amount of time to block, or nil for
//     indefinite.
//   - `users` - a slice of string, each one correspond to a user name, no
//     duplicates are allowed
//   - `options` - DescribeUserScramCredentialsAdminOption options.
//
// Returns a map from user name to user SCRAM credentials description.
// Each description can have an individual error.
func (a *AdminClient) DescribeUserScramCredentials(
	ctx context.Context, users []string,
	options ...DescribeUserScramCredentialsAdminOption) (result DescribeUserScramCredentialsResult, err error) {
	result = DescribeUserScramCredentialsResult{
		Descriptions: make(map[string]UserScramCredentialsDescription),
	}
	err = a.verifyClient()
	if err != nil {
		return result, err
	}

	// Convert user names into char** required by the implementation.
	cUserList := make([]*C.char, len(users))
	cUserCount := C.size_t(len(users))

	for idx, user := range users {
		cUserList[idx] = C.CString(user)
		defer C.free(unsafe.Pointer(cUserList[idx]))
	}

	var cUserListPtr **C.char
	if cUserCount > 0 {
		cUserListPtr = ((**C.char)(&cUserList[0]))
	}

	// Convert Go AdminOptions (if any) to C AdminOptions.
	genericOptions := make([]AdminOption, len(options))
	for i := range options {
		genericOptions[i] = options[i]
	}
	cOptions, err := adminOptionsSetup(
		a.handle, C.RD_KAFKA_ADMIN_OP_DESCRIBEUSERSCRAMCREDENTIALS, genericOptions)
	if err != nil {
		return result, err
	}
	defer C.rd_kafka_AdminOptions_destroy(cOptions)

	// Create temporary queue for async operation.
	cQueue := C.rd_kafka_queue_new(a.handle.rk)
	defer C.rd_kafka_queue_destroy(cQueue)

	// Call rd_kafka_DescribeUserScramCredentials (asynchronous).
	C.rd_kafka_DescribeUserScramCredentials(
		a.handle.rk,
		cUserListPtr,
		cUserCount,
		cOptions,
		cQueue)

	// Wait for result, error or context timeout.
	rkev, err := a.waitResult(
		ctx, cQueue, C.RD_KAFKA_EVENT_DESCRIBEUSERSCRAMCREDENTIALS_RESULT)
	if err != nil {
		return result, err
	}
	defer C.rd_kafka_event_destroy(rkev)

	cRes := C.rd_kafka_event_DescribeUserScramCredentials_result(rkev)

	// Convert result from C to Go.
	result.Descriptions = cToDescribeUserScramCredentialsResult(cRes)

	return result, nil
}

// AlterUserScramCredentials alters SASL/SCRAM credentials.
// The pair (user, mechanism) must be unique among upsertions and deletions.
//
// Parameters:
//   - `ctx` - context with the maximum amount of time to block, or nil for
//     indefinite.
//   - `upsertions` - a slice of user credential upsertions
//   - `deletions` - a slice of user credential deletions
//   - `options` - AlterUserScramCredentialsAdminOption options.
//
// Returns a map from user name to the corresponding Error, with error code
// ErrNoError when the request succeeded.
func (a *AdminClient) AlterUserScramCredentials(
	ctx context.Context, upsertions []UserScramCredentialUpsertion, deletions []UserScramCredentialDeletion,
	options ...AlterUserScramCredentialsAdminOption) (result AlterUserScramCredentialsResult, err error) {
	result = AlterUserScramCredentialsResult{
		Errors: make(map[string]Error),
	}
	err = a.verifyClient()
	if err != nil {
		return result, err
	}

	// Convert user names into char** required by the implementation.
	cAlterationList := make([]*C.rd_kafka_UserScramCredentialAlteration_t, len(upsertions)+len(deletions))
	cAlterationCount := C.size_t(len(upsertions) + len(deletions))
	idx := 0

	for _, upsertion := range upsertions {
		user := C.CString(upsertion.User)
		defer C.free(unsafe.Pointer(user))

		var salt *C.uchar = nil
		var saltSize C.size_t = 0
		if upsertion.Salt != nil {
			salt = (*C.uchar)(&upsertion.Salt[0])
			saltSize = C.size_t(len(upsertion.Salt))
		}

		cAlterationList[idx] = C.rd_kafka_UserScramCredentialUpsertion_new(user,
			C.rd_kafka_ScramMechanism_t(upsertion.ScramCredentialInfo.Mechanism),
			C.int(upsertion.ScramCredentialInfo.Iterations),
			(*C.uchar)(&upsertion.Password[0]), C.size_t(len(upsertion.Password)),
			salt, saltSize)
		defer C.rd_kafka_UserScramCredentialAlteration_destroy(cAlterationList[idx])
		idx = idx + 1
	}

	for _, deletion := range deletions {
		user := C.CString(deletion.User)
		defer C.free(unsafe.Pointer(user))
		cAlterationList[idx] = C.rd_kafka_UserScramCredentialDeletion_new(
			user, C.rd_kafka_ScramMechanism_t(deletion.Mechanism))
		defer C.rd_kafka_UserScramCredentialAlteration_destroy(cAlterationList[idx])
		idx = idx + 1
	}

	var cAlterationListPtr **C.rd_kafka_UserScramCredentialAlteration_t
	if cAlterationCount > 0 {
		cAlterationListPtr = ((**C.rd_kafka_UserScramCredentialAlteration_t)(&cAlterationList[0]))
	}

	// Convert Go AdminOptions (if any) to C AdminOptions.
	genericOptions := make([]AdminOption, len(options))
	for i := range options {
		genericOptions[i] = options[i]
	}
	cOptions, err := adminOptionsSetup(
		a.handle, C.RD_KAFKA_ADMIN_OP_ALTERUSERSCRAMCREDENTIALS, genericOptions)
	if err != nil {
		return result, err
	}
	defer C.rd_kafka_AdminOptions_destroy(cOptions)

	// Create temporary queue for async operation.
	cQueue := C.rd_kafka_queue_new(a.handle.rk)
	defer C.rd_kafka_queue_destroy(cQueue)

	// Call rd_kafka_AlterUserScramCredentials (asynchronous).
	C.rd_kafka_AlterUserScramCredentials(
		a.handle.rk,
		cAlterationListPtr,
		cAlterationCount,
		cOptions,
		cQueue)

	// Wait for result, error or context timeout.
	rkev, err := a.waitResult(
		ctx, cQueue, C.RD_KAFKA_EVENT_ALTERUSERSCRAMCREDENTIALS_RESULT)
	if err != nil {
		return result, err
	}
	defer C.rd_kafka_event_destroy(rkev)

	cRes := C.rd_kafka_event_AlterUserScramCredentials_result(rkev)

	// Convert result from C to Go.
	var cResponseSize C.size_t
	cResponses := C.rd_kafka_AlterUserScramCredentials_result_responses(cRes, &cResponseSize)
	for i := 0; i < int(cResponseSize); i++ {
		cResponse := C.AlterUserScramCredentials_result_response_by_idx(
			cResponses, cResponseSize, C.size_t(i))
		user := C.GoString(C.rd_kafka_AlterUserScramCredentials_result_response_user(cResponse))
		err := newErrorFromCError(C.rd_kafka_AlterUserScramCredentials_result_response_error(cResponse))
		result.Errors[user] = err
	}

	return result, nil
}
