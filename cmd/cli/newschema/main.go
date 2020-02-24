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
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/google/uuid"
	"github.com/linkedin/goavro"
	"github.com/strangedev/catchall"
	schema "github.com/strangedev/kafka-schema/pkg"
	"io/ioutil"
	"log"
	"net/http"
	"os"
)

var name, brokerURL, explorerURL, schemaSpecURL string
var skipExplorerCheck bool

func init() {
	flag.StringVar(&name, "name", "", "A name for the new schema")
	flag.StringVar(&brokerURL, "broker", "broker0:9092", "URL of a Kafka broker")
	flag.StringVar(&explorerURL, "explorer", "schema-explorer:8085", "Use the schema explorer to check if the schema already exists before creating it.")
	flag.BoolVar(&skipExplorerCheck, "skip-check", false, "Do not use the schema explorer to check if if the schema already exists.")
	flag.StringVar(&schemaSpecURL, "from-url", "", "Fetch the specification via HTTP GET rather than reading from Stdin.")
}

func latestSchemaVersion() (uint, bool) {
	route := fmt.Sprintf("http://%v/alias/list", explorerURL)
	resp, err := http.Get(route)
	catchall.CheckFatal("Unable to list current aliases from explorer", err)

	body, err := ioutil.ReadAll(resp.Body)
	var aliases schema.AliasListDTO
	err = json.Unmarshal(body, &aliases)
	catchall.CheckFatal("Unable to list current aliases from explorer (error unmarshalling response)", err)

	for _, alias := range aliases.Aliases {
		schemaVersion, err := schema.VersionFromAlias(alias)
		if err != nil {
			log.Printf("The alias %v does not seems to be in a versioned format. %v", alias, err)
			continue
		}
		if schemaVersion.Name == name {
			return schemaVersion.Version, true
		}
	}
	return 0, false
}

func main() {
	flag.Parse()
	if name == "" {
		log.Fatalf("The schema needs to be named.")
	}

	schemaVersion := schema.NewVersionOrigin(name)
	if !skipExplorerCheck {
		log.Println("Checking if schema already exists...")
		if latestVersion, exists := latestSchemaVersion(); exists {
			log.Fatalf("A schema with that name already exists, its latest version is %v", latestVersion)
		}
		log.Println("Schema does not yet exist, continuing")
	}

	specBytes := bytes.Buffer{}
	if schemaSpecURL != "" {
		resp, err := http.Get(schemaSpecURL)
		catchall.CheckFatal("Unable to fetch specification via HTTP GET", err)
		_, err = specBytes.ReadFrom(resp.Body)
		catchall.CheckFatal("Unable to read response from remote while fetching specification", err)
	} else {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			specBytes.Write(scanner.Bytes())
		}
		if err := scanner.Err(); err != nil {
			log.Fatalf("Unable to read from stdin: %v", err)
		}
	}
	spec := specBytes.String()

	_, err := goavro.NewCodec(spec)
	catchall.CheckFatal("This does not seem like a valid Avro schema", err)

	schemaUUID := uuid.New()
	cmd, err := schema.NewUpdater(brokerURL)
	catchall.CheckFatal("Unable to initialize updater", err)
	err = cmd.UpdateSchema(schemaUUID, spec)
	catchall.CheckFatal("Unable to produce SchemaUpdate event", err)
	log.Printf("Updated schema %v", schemaUUID)
	err = cmd.UpdateAlias(schemaVersion.String(), schemaUUID)
	catchall.CheckFatal("Unable to produce AliasUpdate event", err)
	log.Printf("Updated alias %v", schemaVersion.String())

	log.Println("The schema has been created.")
}
