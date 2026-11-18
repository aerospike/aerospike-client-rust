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

package com.aerospike.udf;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The bridge between the server's values and plain Java objects, in both directions: a stored value
 * copied out as a {@code Long}, {@code String}, {@code java.util.List} and so on, and a Java object put
 * into a list or map being built. Copying out costs an object per element, which is the price of handing
 * the data to code that knows nothing of this SDK; the views ({@link ListValue}, {@link MapValue}) read
 * the same data without it.
 */
final class Convert {
    private Convert() {
    }

    /** A value as a Java object; null for nil or absent. */
    static Object toObject(Value v) {
        switch (v.kind()) {
        case Value.INT:
            return v.payload();
        case Value.DOUBLE:
            return v.doubleBits();
        case Value.BOOL:
            return v.payload() != 0;
        case Value.STR:
        case Value.GUEST_STR:
            return v.asString();
        case Value.GEOJSON:
        case Value.GUEST_GEOJSON:
            return new String(v.asBlob(), StandardCharsets.UTF_8);
        case Value.BLOB:
        case Value.HLL:
        case Value.GUEST_BLOB:
            return v.asBlob();
        case Value.LIST:
            return toList(v);
        case Value.MAP:
            return toMap(v);
        case Value.NIL:
        case Value.ABSENT:
            return null;
        default:
            throw new UdfException("a value of kind " + v.kind() + " has no Java form");
        }
    }

    /** A stored list copied out; empty when {@code list} is not one. */
    static List<Object> toList(Value list) {
        List<Object> out = new ArrayList<>(list.kind() == Value.LIST ? list.length() : 0);

        if (list.kind() != Value.LIST) {
            return out;
        }

        // Each element is taken out of the walk's window before it is converted, so a nested collection
        // opens its own walk only after this one has stepped.
        Walk walk = new Walk(list);

        while (walk.more()) {
            out.add(toObject(walk.next(new Value())));
        }

        return out;
    }

    /** A stored map copied out in the server's order; empty when {@code map} is not one. */
    static Map<Object, Object> toMap(Value map) {
        Map<Object, Object> out = new LinkedHashMap<>();

        if (map.kind() != Value.MAP) {
            return out;
        }

        Walk walk = new Walk(map);

        while (walk.more()) {
            Value key = walk.next(new Value());

            if (!walk.more()) {
                throw new UdfException("the map ended on a key");
            }

            Value value = walk.next(new Value());

            out.put(toObject(key), toObject(value));
        }

        return out;
    }

    /** Appends a Java object to a list being built, by what it is. */
    static void add(ListBuilder list, Object o) {
        if (o == null) {
            list.addNil();
        }
        else if (o instanceof Long || o instanceof Integer || o instanceof Short || o instanceof Byte) {
            list.add(((Number) o).longValue());
        }
        else if (o instanceof Double || o instanceof Float) {
            list.add(((Number) o).doubleValue());
        }
        else if (o instanceof Boolean) {
            list.add(((Boolean) o).booleanValue());
        }
        else if (o instanceof String) {
            list.add((String) o);
        }
        else if (o instanceof byte[]) {
            list.add((byte[]) o);
        }
        else if (o instanceof Value) {
            list.add((Value) o);
        }
        else if (o instanceof ListValue) {
            list.add(((ListValue) o).asValue());
        }
        else if (o instanceof MapValue) {
            list.add(((MapValue) o).asValue());
        }
        else if (o instanceof ListBuilder) {
            list.add((ListBuilder) o);
        }
        else if (o instanceof MapBuilder) {
            list.add((MapBuilder) o);
        }
        else if (o instanceof List) {
            list.add((List<?>) o);
        }
        else if (o instanceof Map) {
            list.add((Map<?, ?>) o);
        }
        else {
            throw new UdfException("a " + o.getClass().getName() + " cannot go into a list");
        }
    }

    /** The value a Java object is, for a map entry: scalars and bytes as they are, collections built. */
    static Value valueOf(Object o) {
        if (o == null) {
            return Value.ofNil();
        }

        if (o instanceof Long || o instanceof Integer || o instanceof Short || o instanceof Byte) {
            return Value.ofInt(((Number) o).longValue());
        }

        if (o instanceof Double || o instanceof Float) {
            return Value.ofDouble(((Number) o).doubleValue());
        }

        if (o instanceof Boolean) {
            return Value.ofBool(((Boolean) o).booleanValue());
        }

        if (o instanceof String) {
            return Value.ofString((String) o);
        }

        if (o instanceof byte[]) {
            return Value.ofBlob((byte[]) o);
        }

        if (o instanceof Value) {
            return (Value) o;
        }

        if (o instanceof ListValue) {
            return ((ListValue) o).asValue();
        }

        if (o instanceof MapValue) {
            return ((MapValue) o).asValue();
        }

        if (o instanceof ListBuilder) {
            return ((ListBuilder) o).asValue();
        }

        if (o instanceof MapBuilder) {
            return ((MapBuilder) o).asValue();
        }

        if (o instanceof List) {
            return Udf.list(((List<?>) o).size()).addAll((List<?>) o).asValue();
        }

        if (o instanceof Map) {
            return Udf.map(((Map<?, ?>) o).size()).putAll((Map<?, ?>) o).asValue();
        }

        throw new UdfException("a " + o.getClass().getName() + " cannot go into a map");
    }

    /** Puts one entry of a {@code java.util.Map} into a map being built; the key is a string or an integer. */
    static void put(MapBuilder map, Object key, Object value) {
        if (key instanceof String) {
            map.put((String) key, valueOf(value));
        }
        else if (key instanceof Long || key instanceof Integer || key instanceof Short || key instanceof Byte) {
            map.put(((Number) key).longValue(), valueOf(value));
        }
        else {
            throw new UdfException("a map key is a String or an integer, not "
                    + (key == null ? "null" : "a " + key.getClass().getName()));
        }
    }
}
