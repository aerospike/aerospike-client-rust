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
// The rest of the record: the writes and metadata the Rust SDK has, as low-level calls.
//

//go:wasmimport aerospike set_bool
func setBool(name unsafe.Pointer, nameLen int32, value int32) int32

//go:wasmimport aerospike set_nil
func setNil(name unsafe.Pointer, nameLen int32) int32

//go:wasmimport aerospike set_geojson
func setGeoJSON(name unsafe.Pointer, nameLen int32, value unsafe.Pointer, valueLen int32) int32

//go:wasmimport aerospike set_list_ints
func setListInts(name unsafe.Pointer, nameLen int32, vals unsafe.Pointer, valsLen int32) int32

//go:wasmimport aerospike set_list_doubles
func setListDoubles(name unsafe.Pointer, nameLen int32, vals unsafe.Pointer, valsLen int32) int32

//go:wasmimport aerospike result_geojson
func resultGeoJSON(value unsafe.Pointer, valueLen int32)

//go:wasmimport aerospike last_update
func lastUpdate() int64

//go:wasmimport aerospike record_size
func recordSize() int64

//go:wasmimport aerospike numbins
func numbins() int64

//go:wasmimport aerospike set_name
func setName(out unsafe.Pointer, cap int32) int32

//go:wasmimport aerospike set_ttl
func setTTL(seconds int32) int32

//go:wasmimport aerospike rec_remove
func recRemove() int32

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

// SetBool writes a boolean to a bin.
// Staged until the record is committed. The status is Err for a name that is empty or longer than 15
// bytes, Refused when the record refuses the write.
func SetBool(name string, value bool) int32 {
	b := int32(0)
	if value {
		b = 1
	}
	return setBool(strptr(name), int32(len(name)), b)
}

// SetNull writes a nil bin.
func SetNull(name string) int32 { return setNil(strptr(name), int32(len(name))) }

// SetGeoJSON writes a GeoJSON document, kept as the geospatially indexable kind.
func SetGeoJSON(name, doc string) int32 {
	return setGeoJSON(strptr(name), int32(len(name)), strptr(doc), int32(len(doc)))
}

// SetInts writes a list of integers from the slice in one import, no value built per element.
func SetInts(name string, values []int64) int32 {
	var p unsafe.Pointer
	if len(values) > 0 {
		p = unsafe.Pointer(&values[0])
	}
	return setListInts(strptr(name), int32(len(name)), p, int32(len(values)*8))
}

// SetFloat64s writes a list of doubles from the slice in one import, no value built per element.
func SetFloat64s(name string, values []float64) int32 {
	var p unsafe.Pointer
	if len(values) > 0 {
		p = unsafe.Pointer(&values[0])
	}
	return setListDoubles(strptr(name), int32(len(name)), p, int32(len(values)*8))
}

// ResultGeoJSON sets a GeoJSON document as the result, kept as that kind.
func ResultGeoJSON(doc string) { resultGeoJSON(strptr(doc), int32(len(doc))) }

// ResultMap and ResultList set a built collection as the result.
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

// ResultOf sets a value of any kind as the result: a scalar by value, guest bytes, a built collection by
// handle, a stored value by position; nil or absent is nil.
func ResultOf(v Value) error {
	switch v.Kind {
	case KindInt:
		resultInt(v.Payload)
	case KindDouble:
		resultDouble(v.Double())
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

// TTL is the record's time to live, in seconds.
func TTL() int64 { return ttl() }

// Now is the server's clock in ms since the Unix epoch, or -1 inside an event handler, where it is refused.
func Now() int64 { return currentTime() }

// LastUpdateTime is when the record was last written, in ms since the Aerospike epoch (2010-01-01 UTC),
// which is how the server stores it. It is not on the same scale as Now, which counts from the Unix epoch.
func LastUpdateTime() int64 { return lastUpdate() }

// RecordSize is the record's size on device, in bytes.
func RecordSize() int64 { return recordSize() }

// BinCount is how many bins the record has.
func BinCount() int64 { return numbins() }

// SetName is the record's set name, "" when it is in no set.
func SetName() string {
	var buf [64]byte
	n := setName(unsafe.Pointer(&buf[0]), int32(len(buf)))
	if n <= 0 {
		return ""
	}
	return string(buf[:n])
}

// SetTTL sets the record's time to live, in seconds, for the next commit: 0 for the namespace default,
// -1 for never expire, subject to the namespace's rule on TTL reductions.
func SetTTL(seconds int32) error {
	return statusError(setTTL(seconds), "could not set the TTL")
}
