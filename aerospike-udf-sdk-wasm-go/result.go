// Copyright 2015-2026 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

package udf

import "unsafe"

//==========================================================
// The results the scalar Result* calls do not cover, the clock, and the values made by the host for a
// collection entry. None of it is about the record.
//

//go:wasmimport aerospike result_geojson
func resultGeoJSON(value unsafe.Pointer, valueLen int32)

//go:wasmimport aerospike current_time
func currentTime() int64

//go:wasmimport aerospike val_int
func valInt(value int64) int32

//go:wasmimport aerospike val_str
func valStr(value unsafe.Pointer, valueLen int32) int32

//go:wasmimport aerospike val_blob
func valBlob(value unsafe.Pointer, valueLen int32) int32

//go:wasmimport aerospike val_geojson
func valGeoJSON(value unsafe.Pointer, valueLen int32) int32

//go:wasmimport aerospike map_put
func mapPut(handle int32, key int32, value int32) int32

//go:wasmimport aerospike map_put_i_at
func mapPutIAt(handle int32, key int64, src int32, at int32) int32

// ResultGeoJSON sets a GeoJSON document as the result, kept as that kind.
func ResultGeoJSON(doc string) { resultGeoJSON(strptr(doc), int32(len(doc))) }

// ResultMap sets a built map as the result. When the map kept a refusal while it was built, that error is
// returned and no result is set.
func ResultMap(m *Map) error {
	if m.err != nil {
		return m.err
	}
	return statusError(resultHandle(m.handle), "could not return the map")
}

// ResultList sets a built list as the result. When the list kept a refusal while it was built, that error
// is returned and no result is set.
func ResultList(l *List) error {
	if l.err != nil {
		return l.err
	}
	return statusError(resultHandle(l.handle), "could not return the list")
}

// Result sets a value of any kind as the result: a scalar by value, guest bytes, a built collection by
// handle, a stored value by position; nil or absent is nil.
func Result(v Value) error {
	switch v.Kind {
	case KindInt:
		resultInt(v.Payload)
	case KindDouble:
		resultDouble(v.double())
	case KindBool:
		resultBool(int32(v.Payload))
	case KindNil, KindAbsent:
		resultNil()
	case KindGuestStr:
		resultStr(guestPtr(v), int32(v.Len))
	case KindGuestBlob:
		resultBlob(guestPtr(v), int32(v.Len))
	case KindGuestGeoJSON:
		resultGeoJSON(guestPtr(v), int32(v.Len))
	case KindHandle:
		return statusError(resultHandle(int32(v.Payload)), "could not return the collection")
	default:
		return statusError(resultAt(v.Src(), v.At()), "could not return the value")
	}
	return nil
}

// Now is the server's clock in ms since the Unix epoch, or -1 inside an event handler, where it is refused.
func Now() int64 { return currentTime() }
