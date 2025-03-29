// SPDX-FileCopyrightText: 2025 Kiril Strezikozin
//
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package ring implements a generic, fixed-size, thread-safe circular
// (ring) buffer.
//
// This implementation operates in a FIFO (first in, first out) manner. Readers
// and writers to a ring buffer can be organized as one-to-many, many-to-one,
// and many-to-many. The data of the buffer is immutable to the user in a sense
// that they cannot modify the elements currently stored in the buffer, only
// retrieve them or insert new ones.
package ring
