/* Copyright 2020 Noah Hummel
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */

package main

import (
	"encoding/json"
	"flag"
	"github.com/google/uuid"
	"github.com/strangedev/catchall"
	schema "github.com/strangedev/kafka-schema/pkg"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

var broker string

func init() {
	flag.StringVar(&broker, "broker", "broker0:9092", "URL of a Kafka broker")
}

func writeJSON(writer http.ResponseWriter, data interface{}) {
	ret, err := json.Marshal(data)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		log.Println(err)
		return
	}

	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Access-Control-Allow-Origin", "*")
	writer.Header().Set("Access-Control-Allow-Headers", "*")
	_, err = writer.Write(ret)
}

func main() {
	flag.Parse()
	log.Printf("Broker: %v", broker)
	
	schemaRepo, err := schema.NewLocalRepo(broker)
	catchall.CheckFatal("Unable to initialize schema repository", err)

	stop, err := schemaRepo.Run()
	catchall.CheckFatal("Unable to start schema repository", err)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	go (func() {
		for sig := range signals {
			stop <- true
			log.Panicf("Caught %v", sig)
		}
	})()

	http.HandleFunc("/schema/list", func(writer http.ResponseWriter, request *http.Request) {
		schemata := schemaRepo.ListSchemata()
		schemaList := schema.SchemaListDTO{Schemata: schemata, Count: len(schemata)}

		writeJSON(writer, schemaList)
	})

	http.HandleFunc("/schema/describe", func(writer http.ResponseWriter, request *http.Request) {
		params := request.URL.Query()
		if err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			log.Println(err)
			return
		}

		schemaUUIDs := params["uuid"]
		if len(schemaUUIDs) < 1 {
			http.Error(writer, "Required params <uuid>", http.StatusBadRequest)
			return
		}

		schemata := schema.SchemataDTO{Schemata: make([]schema.SchemaDTO, 0, len(schemaUUIDs))}
		for _, uuidString := range schemaUUIDs {
			schemaUUID, err := uuid.Parse(uuidString)
			if err != nil {
				http.Error(writer, err.Error(), http.StatusBadRequest)
				log.Println(err)
				return
			}

			spec, ok := schemaRepo.GetSpecification(schemaUUID)
			if !ok {
				log.Println(err)
				continue
			}

			schemata.Schemata = append(schemata.Schemata, schema.SchemaDTO{UUID: schemaUUID, Specification: spec})
		}

		writeJSON(writer, schemata)
	})

	http.HandleFunc("/alias/list", func(writer http.ResponseWriter, request *http.Request) {
		aliases := schemaRepo.ListAliases()
		aliasList := schema.AliasListDTO{Aliases: aliases, Count: len(aliases)}

		writeJSON(writer, aliasList)
	})

	http.HandleFunc("/alias/describe", func(writer http.ResponseWriter, request *http.Request) {
		params := request.URL.Query()
		if err != nil {
			log.Println(err)
			return
		}

		aliasesQuery := params["alias"]
		if len(aliasesQuery) < 1 {
			http.Error(writer, "Required params <alias>", http.StatusBadRequest)
			return
		}

		aliases := schema.AliasesDTO{Aliases: make([]schema.AliasDTO, 0)}
		for _, aliasString := range aliasesQuery {
			alias := schema.Alias(aliasString)
			schemaUUID, ok := schemaRepo.WhoIs(alias)
			if !ok {
				log.Println(err)
				continue
			}

			aliases.Aliases = append(aliases.Aliases, schema.AliasDTO{UUID: schemaUUID, Alias: alias})
		}

		writeJSON(writer, aliases)
	})

	err = http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Println(err)
		return
	}
}
