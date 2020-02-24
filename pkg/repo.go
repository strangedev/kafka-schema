/* Copyright 2020 Noah Hummel
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */

// Package kafka_schema provides mechanisms for accessing avro schemata stored in Kafka
// Schemata are addressed by uuid. All other kinds of addressing mechanisms
// are built on top of this.
package kafka_schema

import (
	"github.com/google/uuid"
)

// Alias is a plain text name for a schema.
// Aliases are used for plain text addressing of schemata.
type Alias string

// String casts Alias to string.
func (a Alias) String() string {
	return string(a)
}

// Repo provides high-level access to Schemata by their uuid
type Repo interface {
	// Decode decodes a datum with the given avro schema
	Decode(schema uuid.UUID, datum []byte) (interface{}, error)
	// Encode encodes a datum with the given avro schema
	Encode(schema uuid.UUID, datum interface{}) ([]byte, error)
	// WaitSchemaReady returns a channel that can be used to wait for a schema to become available.
	// Since the schemata are stored in Kafka, it might take the underlying implementation
	// a while until it has consumed all schema changes.
	WaitSchemaReady(schema uuid.UUID) chan bool
	// ListSchemata returns a slice of all uuids that are currently available.
	// Note that this may not represent the actual state stored in Kafka, since there
	// is some lag between the time when a schema change arrives in Kafka and the
	// time the Repo implementation has consumed the change.
	ListSchemata() []uuid.UUID
	// GetSpecification returns the avro specification for the given uuid in plain text.
	GetSpecification(schema uuid.UUID) (specification string, ok bool)
	// Count returns the number of schemata currently available
	Count() int
}

// AliasRepo provides high-level access to schemata by their aliases
type AliasRepo interface {
	// WhoIs looks up an alias and returns the associated schema's uuid
	WhoIs(alias Alias) (uuid.UUID, bool)
	// WaitAliasReady returns a channel that can be used to wait for an alias and the associated schema to become ready.
	// This works analogous to func Repo.WaitSchemaReady.
	WaitAliasReady(alias Alias) chan bool
	// ListAliases returns a slice of all aliases that are currently available.
	// Note that this may not represent the actual state stored in Kafka, since there
	// is some lag between the time when an alias change arrives in Kafka and the
	// time the Repo implementation has consumed the change.
	ListAliases() []Alias
}

// VersionedRepo provides high-level access to schemata by a using a versioning scheme.
type VersionedRepo interface {
	// DecodeVersion decodes a datum using the specified schema at the specified version.
	DecodeVersion(schema NameVersion, datum []byte) (interface{}, error)
	// EncodeVersion encodes a datum using the specified schema at the specified version.
	EncodeVersion(schema NameVersion, datum interface{}) ([]byte, error)
	// WaitVersionReady returns a channel that can be used to wait for a schema to become available in the specified version.
	// This works analogous to func Repo.WaitSchemaReady.
	WaitVersionReady(schema NameVersion) chan bool
}
