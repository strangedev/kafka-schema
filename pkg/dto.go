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
)

// SchemaDTO is used by the explorer to encode its response body.
type SchemaDTO struct {
	UUID          uuid.UUID `json:"uuid"`
	Specification string    `json:"spec"`
}

// SchemataDTO is used by the explorer to encode its response body.
type SchemataDTO struct {
	Schemata []SchemaDTO `json:"schemata"`
}

// SchemaListDTO is used by the explorer to encode its response body.
type SchemaListDTO struct {
	Count    int         `json:"count"`
	Schemata []uuid.UUID `json:"schemata"`
}

// AliasDTO is used by the explorer to encode its response body.
type AliasDTO struct {
	Alias Alias `json:"alias"`
	UUID  uuid.UUID         `json:"uuid"`
}

// AliasListDTO is used by the explorer to encode its response body.
type AliasListDTO struct {
	Aliases []Alias `json:"aliases"`
	Count   int                 `json:"count"`
}

// AliasesDTO is used by the explorer to encode its response body.
type AliasesDTO struct {
	Aliases []AliasDTO `json:"aliases"`
}

// UpdateRequest sets the given UUID to equal the given plain-text Avro spec.
type UpdateRequest struct {
	UUID uuid.UUID `json:"UUID"`
	Spec string    `json:"spec"`
}

// AliasRequest sets the given Alias to equal the given UUID.
type AliasRequest struct {
	UUID  uuid.UUID `json:"UUID"`
	Alias string    `json:"alias"`
}
