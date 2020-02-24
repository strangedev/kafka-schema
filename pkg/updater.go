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
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
	core "github.com/strangedev/kafka-golang/pkg"
)

// Commander is a KafkaProducer used for writing schema updates into Kafka.
type Commander struct {
	*core.Producer
}

// Updater encapsulates the methods required to update the schema repository stored in Kafka.
type Updater interface {
	// UpdateSchema sets the given UUID to equal the given plain-text Avro spec.
	UpdateSchema(schemaUUID uuid.UUID, specification string) error
	// UpdateAlias sets the given Alias to equal the given UUID.
	UpdateAlias(alias string, schemaUUID uuid.UUID) error
}

// NewUpdater constructs an Updater that uses the given Kafka broker to write updates.
// This will create a new KafkaProducer.
func NewUpdater(broker string) (Updater, error) {
	p, err := core.NewKafkaProducer(broker)
	if err != nil {
		return nil, err
	}
	return NewUpdaterWithProducer(p), nil
}

// NewUpdater constructs an Updater that uses the given KafkaProducer to produce its events.
// In most cases, it is fine to use NewUpdater instead and let it create a new KafkaProducer.
func NewUpdaterWithProducer(p *core.Producer) Updater {
	return Commander{p}
}


func (cmd Commander) UpdateSchema(schemaUUID uuid.UUID, specification string) error {
	topic := "schema_update"
	request := UpdateRequest{UUID: schemaUUID, Spec: specification}
	return cmd.ProduceJSONSync(topic, kafka.PartitionAny, request)
}

func (cmd Commander) UpdateAlias(alias string, schemaUUID uuid.UUID) error {
	topic := "schema_alias"
	request := AliasRequest{UUID: schemaUUID, Alias: alias}
	return cmd.ProduceJSONSync(topic, kafka.PartitionAny, request)
}
