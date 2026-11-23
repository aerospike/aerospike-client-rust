// Copyright 2015-2026 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License is
// distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing permissions and limitations under the
// License.

//go:build gc.precise

package udf

// A value made over the guest's own bytes keeps their address as an integer (see guestAddr). TinyGo's
// precise collector does not take that integer for a reference, and could free the bytes while the value
// is still in use. Build with -gc=leaking, as the Makefile does, or -gc=conservative.
var _ = this_sdk_needs_gc_leaking_or_gc_conservative_not_gc_precise
