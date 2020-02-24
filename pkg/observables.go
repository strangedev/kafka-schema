/* Copyright 2020 Noah Hummel
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */

package kafka_schema

import (
	"github.com/google/uuid"
	"github.com/linkedin/goavro"
	"github.com/strangedev/catchall"
)

type aliasMapType map[Alias]uuid.UUID

// SchemaMap is a KeyObservable map of Aliases to UUIDs
type AliasMap struct {
	catchall.ConcurrentObservable
	Map aliasMapType
}

// Upsert inserts or updates an Alias, UUID pair into the map.
// All of the map's observers are notified of this change.
// It returns true, if the map entry did already exist and was overwritten.
func (m AliasMap) Insert(alias Alias, schemaUUID uuid.UUID) bool {
	m.DataLock.Lock()
	_, overwritten := m.Map[alias]
	m.Map[alias] = schemaUUID
	m.DataLock.Unlock()
	m.Notify(alias)
	return overwritten
}

// NewAliasMap constructs an empty AliasMap with no observers.
func NewAliasMap() AliasMap {
	return AliasMap{
		ConcurrentObservable: catchall.NewConcurrentObservable(),
		Map:                  make(aliasMapType),
	}
}

type schemaMapType map[uuid.UUID]*goavro.Codec

// SchemaMap is a KeyObservable map of UUIDs to Avro codecs
type SchemaMap struct {
	catchall.ConcurrentObservable
	Map schemaMapType
}

// Upsert inserts or updates a UUID, Codec pair into the map.
// All of the map's observers are notified of this change.
// It returns true, if the map entry did already exist and was overwritten.
func (m SchemaMap) Upsert(schemaUUID uuid.UUID, codec *goavro.Codec) bool {
	m.DataLock.Lock()
	_, overwritten := m.Map[schemaUUID]
	m.Map[schemaUUID] = codec
	m.DataLock.Unlock()
	m.Notify(schemaUUID)
	return overwritten
}

// NewSchemaMap constructs an empty SchemaMap with no observers.
func NewSchemaMap() SchemaMap {
	return SchemaMap{
		ConcurrentObservable: catchall.NewConcurrentObservable(),
		Map:                  make(schemaMapType),
	}
}
