/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.trogdor.common;

import org.junit.Test;


import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StringExpanderTest {
    @Test(expected = IllegalArgumentException.class)
    public void testExpandThrowsIllegalArgumentExceptionIfCannotBeExpanded() {
        assertFalse(StringExpander.canExpand("foo"));
        assertFalse(StringExpander.canExpand("bar"));
        assertFalse(StringExpander.canExpand(""));

        StringExpander.expand("foo");
    }

    @Test(expected = RuntimeException.class)
    public void testExpandThrowsRuntimeExceptionIfInvalidRange() {
        assertTrue(StringExpander.canExpand("foo[100-50]"));
        StringExpander.expand("foo[100-50]");
    }

    @Test
    public void testExpand() {
        HashSet<String> expected1 = new HashSet<>(Arrays.asList(
            "foo1",
            "foo2",
            "foo3"
        ));
        List<Integer> expectedRange1 = Arrays.asList(1, 2, 3);
        String toExpand = "foo[1-3]";
        assertTrue(StringExpander.canExpand(toExpand));
        StringExpander.ExpandedResult result = StringExpander.expand(toExpand);
        assertEquals(expected1, result.expandedResult());
        assertEquals(expectedRange1, result.range());
        assertEquals("foo", result.parsedResult());

        HashSet<String> expected2 = new HashSet<>(Arrays.asList(
            "foo bar baz 0"
        ));
        List<Integer> expectedRange2 = Arrays.asList(0);
        toExpand = "foo bar baz [0-0]";
        assertTrue(StringExpander.canExpand(toExpand));
        result = StringExpander.expand(toExpand);
        assertEquals(expected2, result.expandedResult());
        assertEquals(expectedRange2, result.range());
        assertEquals("foo bar baz ", result.parsedResult());

        HashSet<String> expected3 = new HashSet<>(Arrays.asList(
            "[[ wow50 ]]",
            "[[ wow51 ]]",
            "[[ wow52 ]]"
        ));
        List<Integer> expectedRange3 = Arrays.asList(50, 51, 52);
        toExpand = "[[ wow[50-52] ]]";
        assertTrue(StringExpander.canExpand(toExpand));
        result = StringExpander.expand(toExpand);
        assertEquals(expected3, result.expandedResult());
        assertEquals(expectedRange3, result.range());
        assertEquals("[[ wow ]]", result.parsedResult());
    }

    @Test
    public void testExpandIntoMap() {
        String value1 = "foo[1-3][1-3]";
        Map<String, List<Integer>> expected1 = new HashMap<>();
        expected1.put("foo1", Arrays.asList(1, 2, 3));
        expected1.put("foo2", Arrays.asList(1, 2, 3));
        expected1.put("foo3", Arrays.asList(1, 2, 3));
        Map<String, List<Integer>> result1 = StringExpander.expandIntoMap(value1);
        assertEquals(expected1, result1);

        String value2 = "foo[1-3]";
        Map<String, List<Integer>> expected2 = new HashMap<>();
        expected2.put("foo1", Arrays.asList());
        expected2.put("foo2", Arrays.asList());
        expected2.put("foo3", Arrays.asList());
        Map<String, List<Integer>> result2 = StringExpander.expandIntoMap(value2);
        assertEquals(expected2, result2);

        String value3 = "[[ wow[50-52][101-102] ]]";
        Map<String, List<Integer>> expected3 = new HashMap<>();
        expected3.put("[[ wow50 ]]", Arrays.asList(101, 102));
        expected3.put("[[ wow51 ]]", Arrays.asList(101, 102));
        expected3.put("[[ wow52 ]]", Arrays.asList(101, 102));
        Map<String, List<Integer>> result3 = StringExpander.expandIntoMap(value3);
        assertEquals(expected3, result3);

        String value4 = "foo[1-3][1-1]";
        Map<String, List<Integer>> expected4 = new HashMap<>();
        expected4.put("foo1", Arrays.asList(1));
        expected4.put("foo2", Arrays.asList(1));
        expected4.put("foo3", Arrays.asList(1));
        Map<String, List<Integer>> result4 = StringExpander.expandIntoMap(value4);
        assertEquals(expected4, result4);
    }
}
