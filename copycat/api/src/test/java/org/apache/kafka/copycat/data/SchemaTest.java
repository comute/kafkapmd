/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.copycat.data;

import org.apache.kafka.copycat.errors.DataException;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

public class SchemaTest {
    private static final Schema MAP_INT_STRING_SCHEMA = SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.STRING_SCHEMA).build();
    private static final Schema FLAT_STRUCT_SCHEMA = SchemaBuilder.struct()
            .field("field", Schema.INT32_SCHEMA)
            .build();
    private static final Schema STRUCT_SCHEMA = SchemaBuilder.struct()
            .field("first", Schema.INT32_SCHEMA)
            .field("second", Schema.STRING_SCHEMA)
            .field("array", SchemaBuilder.array(Schema.INT32_SCHEMA).build())
            .field("map", SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.STRING_SCHEMA).build())
            .field("nested", FLAT_STRUCT_SCHEMA)
            .build();
    private static final Schema PARENT_STRUCT_SCHEMA = SchemaBuilder.struct()
            .field("nested", FLAT_STRUCT_SCHEMA)
            .build();

    @Test
    public void testFieldsOnStructSchema() {
        Schema schema = SchemaBuilder.struct()
                .field("foo", Schema.BOOLEAN_SCHEMA)
                .field("bar", Schema.INT32_SCHEMA)
                .build();

        assertEquals(2, schema.fields().size());
        // Validate field lookup by name
        Field foo = schema.field("foo");
        assertEquals(0, foo.index());
        Field bar = schema.field("bar");
        assertEquals(1, bar.index());
        // Any other field name should fail
        assertNull(schema.field("other"));
    }


    @Test(expected = DataException.class)
    public void testFieldsOnlyValidForStructs() {
        Schema.INT8_SCHEMA.fields();
    }

    @Test
    public void testValidateValueMatchingType() {
        Schema.validateValue(Schema.INT8_SCHEMA, (byte) 1);
        Schema.validateValue(Schema.INT16_SCHEMA, (short) 1);
        Schema.validateValue(Schema.INT32_SCHEMA, 1);
        Schema.validateValue(Schema.INT64_SCHEMA, (long) 1);
        Schema.validateValue(Schema.FLOAT32_SCHEMA, 1.f);
        Schema.validateValue(Schema.FLOAT64_SCHEMA, 1.);
        Schema.validateValue(Schema.BOOLEAN_SCHEMA, true);
        Schema.validateValue(Schema.STRING_SCHEMA, "a string");
        Schema.validateValue(Schema.BYTES_SCHEMA, "a byte array".getBytes());
        Schema.validateValue(Schema.BYTES_SCHEMA, ByteBuffer.wrap("a byte array".getBytes()));
        Schema.validateValue(SchemaBuilder.array(Schema.INT32_SCHEMA).build(), Arrays.asList(1, 2, 3));
        Schema.validateValue(
                SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.STRING_SCHEMA).build(),
                Collections.singletonMap(1, "value")
        );
        // Struct tests the basic struct layout + complex field types + nested structs
        Struct structValue = new Struct(STRUCT_SCHEMA)
                .put("first", 1)
                .put("second", "foo")
                .put("array", Arrays.asList(1, 2, 3))
                .put("map", Collections.singletonMap(1, "value"))
                .put("nested", new Struct(FLAT_STRUCT_SCHEMA).put("field", 12));
        Schema.validateValue(STRUCT_SCHEMA, structValue);
    }

    // To avoid requiring excessive numbers of tests, these checks for invalid types use a similar type where possible
    // to only include a single test for each type

    @Test(expected = DataException.class)
    public void testValidateValueMismatchInt8() {
        Schema.validateValue(Schema.INT8_SCHEMA, 1);
    }

    @Test(expected = DataException.class)
    public void testValidateValueMismatchInt16() {
        Schema.validateValue(Schema.INT16_SCHEMA, 1);
    }

    @Test(expected = DataException.class)
    public void testValidateValueMismatchInt32() {
        Schema.validateValue(Schema.INT32_SCHEMA, (long) 1);
    }

    @Test(expected = DataException.class)
    public void testValidateValueMismatchInt64() {
        Schema.validateValue(Schema.INT64_SCHEMA, 1);
    }

    @Test(expected = DataException.class)
    public void testValidateValueMismatchFloat() {
        Schema.validateValue(Schema.FLOAT32_SCHEMA, 1.0);
    }

    @Test(expected = DataException.class)
    public void testValidateValueMismatchDouble() {
        Schema.validateValue(Schema.FLOAT64_SCHEMA, 1.f);
    }

    @Test(expected = DataException.class)
    public void testValidateValueMismatchBoolean() {
        Schema.validateValue(Schema.BOOLEAN_SCHEMA, 1.f);
    }

    @Test(expected = DataException.class)
    public void testValidateValueMismatchString() {
        // CharSequence is a similar type (supertype of String), but we restrict to String.
        CharBuffer cbuf = CharBuffer.wrap("abc");
        Schema.validateValue(Schema.STRING_SCHEMA, cbuf);
    }

    @Test(expected = DataException.class)
    public void testValidateValueMismatchBytes() {
        Schema.validateValue(Schema.BYTES_SCHEMA, new Object[]{1, "foo"});
    }

    @Test(expected = DataException.class)
    public void testValidateValueMismatchArray() {
        Schema.validateValue(SchemaBuilder.array(Schema.INT32_SCHEMA).build(), Arrays.asList("a", "b", "c"));
    }

    @Test(expected = DataException.class)
    public void testValidateValueMismatchArraySomeMatch() {
        // Even if some match the right type, this should fail if any mismatch. In this case, type erasure loses
        // the fact that the list is actually List<Object>, but we couldn't tell if only checking the first element
        Schema.validateValue(SchemaBuilder.array(Schema.INT32_SCHEMA).build(), Arrays.asList(1, 2, "c"));
    }

    @Test(expected = DataException.class)
    public void testValidateValueMismatchMapKey() {
        Schema.validateValue(MAP_INT_STRING_SCHEMA, Collections.singletonMap("wrong key type", "value"));
    }

    @Test(expected = DataException.class)
    public void testValidateValueMismatchMapValue() {
        Schema.validateValue(MAP_INT_STRING_SCHEMA, Collections.singletonMap(1, 2));
    }

    @Test(expected = DataException.class)
    public void testValidateValueMismatchMapSomeKeys() {
        Map<Object, String> data = new HashMap<>();
        data.put(1, "abc");
        data.put("wrong", "it's as easy as one two three");
        Schema.validateValue(MAP_INT_STRING_SCHEMA, data);
    }

    @Test(expected = DataException.class)
    public void testValidateValueMismatchMapSomeValues() {
        Map<Integer, Object> data = new HashMap<>();
        data.put(1, "abc");
        data.put(2, "wrong".getBytes());
        Schema.validateValue(MAP_INT_STRING_SCHEMA, data);
    }

    @Test(expected = DataException.class)
    public void testValidateValueMismatchStructWrongSchema() {
        // Completely mismatching schemas
        Schema.validateValue(
                FLAT_STRUCT_SCHEMA,
                new Struct(SchemaBuilder.struct().field("x", Schema.INT32_SCHEMA).build()).put("x", 1)
        );
    }

    @Test(expected = DataException.class)
    public void testValidateValueMismatchStructWrongNestedSchema() {
        // Top-level schema  matches, but nested does not.
        Schema.validateValue(
                PARENT_STRUCT_SCHEMA,
                new Struct(PARENT_STRUCT_SCHEMA)
                        .put("nested", new Struct(SchemaBuilder.struct().field("x", Schema.INT32_SCHEMA).build()).put("x", 1))
        );
    }


    @Test
    public void testPrimitiveEquality() {
        // Test that primitive types, which only need to consider all the type & metadata fields, handle equality correctly
        Schema s1 = new Schema(Schema.Type.INT8, false, null, "name", "version".getBytes(), "doc", null, null, null);
        Schema s2 = new Schema(Schema.Type.INT8, false, null, "name", "version".getBytes(), "doc", null, null, null);
        Schema differentType = new Schema(Schema.Type.INT16, false, null, "name", "version".getBytes(), "doc", null, null, null);
        Schema differentOptional = new Schema(Schema.Type.INT8, true, null, "name", "version".getBytes(), "doc", null, null, null);
        Schema differentDefault = new Schema(Schema.Type.INT8, false, true, "name", "version".getBytes(), "doc", null, null, null);
        Schema differentName = new Schema(Schema.Type.INT8, false, null, "otherName", "version".getBytes(), "doc", null, null, null);
        Schema differentVersion = new Schema(Schema.Type.INT8, false, null, "name", "otherVersion".getBytes(), "doc", null, null, null);
        Schema differentDoc = new Schema(Schema.Type.INT8, false, null, "name", "version".getBytes(), "other doc", null, null, null);

        assertEquals(s1, s2);
        assertNotEquals(s1, differentType);
        assertNotEquals(s1, differentOptional);
        assertNotEquals(s1, differentDefault);
        assertNotEquals(s1, differentName);
        assertNotEquals(s1, differentVersion);
        assertNotEquals(s1, differentDoc);
    }

    @Test
    public void testArrayEquality() {
        // Validate that the value type for the array is tested for equality. This test makes sure the same schema object is
        // never reused to ensure we're actually checking equality
        Schema s1 = new Schema(Schema.Type.ARRAY, false, null, null, null, null, null, null, SchemaBuilder.int8().build());
        Schema s2 = new Schema(Schema.Type.ARRAY, false, null, null, null, null, null, null, SchemaBuilder.int8().build());
        Schema differentValueSchema = new Schema(Schema.Type.ARRAY, false, null, null, null, null, null, null, SchemaBuilder.int16().build());

        assertEquals(s1, s2);
        assertNotEquals(s1, differentValueSchema);
    }

    @Test
    public void testMapEquality() {
        // Same as testArrayEquality, but for both key and value schemas
        Schema s1 = new Schema(Schema.Type.MAP, false, null, null, null, null, null, SchemaBuilder.int8().build(), SchemaBuilder.int16().build());
        Schema s2 = new Schema(Schema.Type.MAP, false, null, null, null, null, null, SchemaBuilder.int8().build(), SchemaBuilder.int16().build());
        Schema differentKeySchema = new Schema(Schema.Type.MAP, false, null, null, null, null, null, SchemaBuilder.string().build(), SchemaBuilder.int16().build());
        Schema differentValueSchema = new Schema(Schema.Type.MAP, false, null, null, null, null, null, SchemaBuilder.int8().build(), SchemaBuilder.string().build());

        assertEquals(s1, s2);
        assertNotEquals(s1, differentKeySchema);
        assertNotEquals(s1, differentValueSchema);
    }

    @Test
    public void testStructEquality() {
        // Same as testArrayEquality, but checks differences in fields. Only does a simple check, relying on tests of
        // Field's equals() method to validate all variations in the list of fields will be checked
        Schema s1 = new Schema(Schema.Type.STRUCT, false, null, null, null, null,
                Arrays.asList(new Field("field", 0, SchemaBuilder.int8().build()),
                        new Field("field2", 1, SchemaBuilder.int16().build())), null, null);
        Schema s2 = new Schema(Schema.Type.STRUCT, false, null, null, null, null,
                Arrays.asList(new Field("field", 0, SchemaBuilder.int8().build()),
                        new Field("field2", 1, SchemaBuilder.int16().build())), null, null);
        Schema differentField = new Schema(Schema.Type.STRUCT, false, null, null, null, null,
                Arrays.asList(new Field("field", 0, SchemaBuilder.int8().build()),
                        new Field("different field name", 1, SchemaBuilder.int16().build())), null, null);

        assertEquals(s1, s2);
        assertNotEquals(s1, differentField);
    }

}
