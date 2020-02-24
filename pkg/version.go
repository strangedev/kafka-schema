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
	"errors"
	"fmt"
	"strings"
)

// NameVersion represents a plain text addressable thing that is linearly versioned.
type NameVersion struct {
	// Name is the plain text address of the thing.
	Name    string
	// Version is the version number of the thing.
	// The version number starts at 0 and increases linearly by 1.
	Version uint
}

// Version provides a high-level interface for versioning operations on a plain-text addressable versioned thing.
type Version interface {
	// IsOrigin indicates whether this is the first version of the thing.
	IsOrigin() bool
	// GetVersion returns the version number of the thing.
	GetVersion() uint
	// GetName returns the name i.e. the plain text address of the thing.
	GetName() string
	// GetPrevious returns the Version preceding this Version.
	// If this is the origin, it returns the origin unchanged.
	// The ok flag indicates whether the returned Version differs from the passed Version.
	GetPrevious() (v Version, ok bool)
	// GetNext returns the Version succeeding this Version.
	// If this is the most recent Version, it returns the Version unchanged.
	// The ok flag indicates whether the returned Version differs from the passed Version.
	GetNext() (v Version, ok bool)
	// String marshals the Version into a string.
	String() string
	// Alias marshals the Version into an Alias.
	Alias() Alias
}

func NewVersionOrigin(alias string) NameVersion {
	return NameVersion{Name: alias, Version: 0}
}

func (v NameVersion) IsOrigin() bool {
	return v.Version == 0
}

func (v NameVersion) GetVersion() uint {
	return v.Version
}

func (v NameVersion) GetName() string {
	return v.Name
}

func (v NameVersion) GetPrevious() (Version, bool) {
	if v.Version == 0 {
		return v, false
	}
	previousVersion := v.Version - 1
	return NameVersion{Name: v.Name, Version: previousVersion}, true
}

func (v NameVersion) GetNext() (Version, bool) {
	previousVersion := v.Version + 1
	return NameVersion{Name: v.Name, Version: previousVersion}, true
}

func (v NameVersion) String() string {
	return fmt.Sprintf("%s-v%x", v.Name, v.Version)
}

func (v NameVersion) Alias() Alias {
	return Alias(v.String())
}

// VersionFromString unmarshals a Version from string.
// The format is {NAME}-v{VERSION}.
func VersionFromString(s string) (NameVersion, error) {
	version := NameVersion{}
	parts := strings.Split(s, "-v")
	if len(parts) != 2 {
		return version, errors.New("invalid format")
	}
	version.Name = parts[0]
	_, err := fmt.Sscanf(parts[1], "%x", &version.Version)
	return version, err
}

// VersionFromAlias unmarshals a Version from Alias.
func VersionFromAlias(a Alias) (NameVersion, error) {
	return VersionFromString(string(a))
}
