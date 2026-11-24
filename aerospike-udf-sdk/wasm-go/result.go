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
// The results the scalar Result* calls do not cover, and the clock. None of it is about the record.
//

//go:wasmimport aerospike result_geojson
func resultGeoJSON(value unsafe.Pointer, valueLen int32)

//go:wasmimport aerospike current_time
func currentTime() int64

// ResultGeoJSON sets a GeoJSON document as the call's result, kept as that kind where ResultString
// would return a string. The host copies the text during the call and does not check it.
func ResultGeoJSON(doc string) { resultGeoJSON(strptr(doc), int32(len(doc))) }

// ResultMap sets a built map as the call's result. When the map kept a refusal while it was built, that
// error is returned and no result is set; otherwise the error is nil unless the host no longer has the
// map. A result that nests collections more than 64 deep fails the call.
//
// A function under `//udf:function` returns the *Map instead, and the generated export makes this
// call.
//
//	out := udf.NewMap(2).Put("total", total).Put("count", count)
//	if err := udf.ResultMap(out); err != nil {
//		return udf.FailErr(err)
//	}
//	return 0
func ResultMap(m *Map) error {
	if m.err != nil {
		return m.err
	}
	return statusError(resultHandle(m.handle), "could not return the map")
}

// ResultList sets a built list as the call's result. When the list kept a refusal while it was built,
// that error is returned and no result is set.
func ResultList(l *List) error {
	if l.err != nil {
		return l.err
	}
	return statusError(resultHandle(l.handle), "could not return the list")
}

// Result sets a value of any kind as the call's result: a scalar by value, a string, blob or GeoJSON
// of the guest's, copied by the host, a built collection by its handle, a stored value by position,
// without decoding it. A nil or absent value sets nil.
//
// The error is non-nil only for a built collection or a stored value the host can no longer find. It
// is what a function returning a udf.Value has the generated export call.
//
//	last, ok, err := history.Index(int(history.Len) - 1)
//	if err != nil {
//		return udf.FailErr(err)
//	}
//	if !ok {
//		udf.ResultNil()
//		return 0
//	}
//	if err := udf.Result(last); err != nil {
//		return udf.FailErr(err)
//	}
//	return 0
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

// Now is the server's clock, in milliseconds since the Unix epoch. Inside an event handler it is
// refused and answers -1: a merge that reads the clock cannot come out the same on both clusters.
// It is not on the scale of Record.LastUpdateTime, which counts from 2010.
func Now() int64 { return currentTime() }

//go:wasmimport aerospike cluster_src_id
func clusterSrcID() int64

// ClusterSrcID is this cluster's XDR source id: the src-id of its xdr stanza, 1 to 255, which XDR
// stamps on every write it ships from here. It is 0 when the cluster has none: Community Edition, or no
// src-id configured. A write UDF tags what it writes with it, so a per-origin structure (a counter, a
// set of stamps) keys each cluster's share by the same id XDR reports in EventRecord.SourceID, with no
// argument for a client to get wrong.
//
// Inside an event handler it is refused and answers -1, as Now is: the answer depends on which cluster
// runs the merge, and a merge that reads it cannot converge. A handler has each side's id on its
// records.
//
//	site := udf.ClusterSrcID()
//	if site <= 0 {
//		return 0, errors.New("increment: this cluster has no XDR src-id")
//	}
func ClusterSrcID() int64 { return clusterSrcID() }
