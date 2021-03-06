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
	"flag"
	"github.com/strangedev/catchall"
	schema "github.com/strangedev/kafka-schema/pkg"
	"log"
	"os"
	"os/signal"
	"syscall"
)

var broker string

func init() {
	flag.StringVar(&broker, "broker", "broker0:9092", "URL of a Kafka broker")
}

func main() {
	flag.Parse()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	log.Printf("Pulling schemata from %v", broker)
	
	schemaRepo, err := schema.NewLocalRepo(broker)
	catchall.CheckFatal("Unable to initialize schema repository", err)

	schemaVersion := schema.NameVersion{Name: "mySchema", Version: 13}
	schemaReady := schemaRepo.WaitVersionReady(schemaVersion)
	stop, err := schemaRepo.Run()
	catchall.CheckFatal("Unable to start schema repository", err)

	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	go (func() {
		<-signals
		stop <- true
	})()

	ok := <-schemaReady
	if ok {
		schemaUUID, _ := schemaRepo.WhoIs(schemaVersion.Alias())
		log.Printf("Schema ready, has UUID %v\n", schemaUUID)
	} else {
		log.Println("Aborted")
	}
}
