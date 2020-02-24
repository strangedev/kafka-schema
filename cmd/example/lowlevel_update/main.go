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
	"fmt"
	"github.com/google/uuid"
	schema "github.com/strangedev/kafka-schema/pkg"
	"os"
)

func main() {
	cmd, err := schema.NewUpdater("broker0:9092")
	if err != nil {
		fmt.Println("Can't create commander")
		fmt.Println(err)
		os.Exit(1)
	}
	if os.Args[1] == "spec" {
		schemaUUID, err := uuid.Parse(os.Args[2])
		if err != nil {
			fmt.Println("Can't parse UUID")
			fmt.Println(err)
			os.Exit(1)
		}
		err = cmd.UpdateSchema(schemaUUID, os.Args[3])
		if err != nil {
			fmt.Println("Can't send command")
			fmt.Println(err)
			os.Exit(1)
		}
	} else if os.Args[1] == "alias" {
		schemaUUID, err := uuid.Parse(os.Args[3])
		if err != nil {
			fmt.Println("Can't parse UUID")
			fmt.Println(err)
			os.Exit(1)
		}
		err = cmd.UpdateAlias(os.Args[2], schemaUUID)
		if err != nil {
			fmt.Println("Can't send command")
			fmt.Println(err)
			os.Exit(1)
		}
	}

}
