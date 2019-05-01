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
package org.apache.kafka.common.utils;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * A unit test for ImplicitLinkedHashCollection.
 */
public class ImplicitLinkedHashCollectionTest {
    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    final static class TestElement implements ImplicitLinkedHashCollection.Element {
        private int prev = ImplicitLinkedHashCollection.INVALID_INDEX;
        private int next = ImplicitLinkedHashCollection.INVALID_INDEX;
        private final int val;

        TestElement(int val) {
            this.val = val;
        }

        @Override
        public int prev() {
            return prev;
        }

        @Override
        public void setPrev(int prev) {
            this.prev = prev;
        }

        @Override
        public int next() {
            return next;
        }

        @Override
        public void setNext(int next) {
            this.next = next;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if ((o == null) || (o.getClass() != TestElement.class)) return false;
            TestElement that = (TestElement) o;
            return val == that.val;
        }

        @Override
        public String toString() {
            return "TestElement(" + val + ")";
        }

        @Override
        public int hashCode() {
            return val;
        }
    }

    @Test
    public void testNullForbidden() {
        ImplicitLinkedHashMultiCollection<TestElement> multiColl = new ImplicitLinkedHashMultiCollection<>();
        assertFalse(multiColl.add(null));
    }

    @Test
    public void testInsertDelete() {
        ImplicitLinkedHashCollection<TestElement> coll = new ImplicitLinkedHashCollection<>(100);
        assertTrue(coll.add(new TestElement(1)));
        TestElement second = new TestElement(2);
        assertTrue(coll.add(second));
        assertTrue(coll.add(new TestElement(3)));
        assertFalse(coll.add(new TestElement(3)));
        assertEquals(3, coll.size());
        assertTrue(coll.contains(new TestElement(1)));
        assertFalse(coll.contains(new TestElement(4)));
        TestElement secondAgain = coll.find(new TestElement(2));
        assertTrue(second == secondAgain);
        assertTrue(coll.remove(new TestElement(1)));
        assertFalse(coll.remove(new TestElement(1)));
        assertEquals(2, coll.size());
        coll.clear();
        assertEquals(0, coll.size());
    }

    static void expectTraversal(Iterator<TestElement> iterator, Integer... sequence) {
        int i = 0;
        while (iterator.hasNext()) {
            TestElement element = iterator.next();
            Assert.assertTrue("Iterator yieled " + (i + 1) + " elements, but only " +
                    sequence.length + " were expected.", i < sequence.length);
            Assert.assertEquals("Iterator value number " + (i + 1) + " was incorrect.",
                    sequence[i].intValue(), element.val);
            i = i + 1;
        }
        Assert.assertTrue("Iterator yieled " + (i + 1) + " elements, but " +
                sequence.length + " were expected.", i == sequence.length);
    }

    static void expectTraversal(Iterator<TestElement> iter, Iterator<Integer> expectedIter) {
        int i = 0;
        while (iter.hasNext()) {
            TestElement element = iter.next();
            Assert.assertTrue("Iterator yieled " + (i + 1) + " elements, but only " +
                    i + " were expected.", expectedIter.hasNext());
            Integer expected = expectedIter.next();
            Assert.assertEquals("Iterator value number " + (i + 1) + " was incorrect.",
                    expected.intValue(), element.val);
            i = i + 1;
        }
        Assert.assertFalse("Iterator yieled " + i + " elements, but at least " +
                (i + 1) + " were expected.", expectedIter.hasNext());
    }

    @Test
    public void testTraversal() {
        ImplicitLinkedHashCollection<TestElement> coll = new ImplicitLinkedHashCollection<>();
        expectTraversal(coll.iterator());
        assertTrue(coll.add(new TestElement(2)));
        expectTraversal(coll.iterator(), 2);
        assertTrue(coll.add(new TestElement(1)));
        expectTraversal(coll.iterator(), 2, 1);
        assertTrue(coll.add(new TestElement(100)));
        expectTraversal(coll.iterator(), 2, 1, 100);
        assertTrue(coll.remove(new TestElement(1)));
        expectTraversal(coll.iterator(), 2, 100);
        assertTrue(coll.add(new TestElement(1)));
        expectTraversal(coll.iterator(), 2, 100, 1);
        Iterator<TestElement> iter = coll.iterator();
        iter.next();
        iter.next();
        iter.remove();
        iter.next();
        assertFalse(iter.hasNext());
        expectTraversal(coll.iterator(), 2, 1);
        List<TestElement> list = new ArrayList<>();
        list.add(new TestElement(1));
        list.add(new TestElement(2));
        assertTrue(coll.removeAll(list));
        assertFalse(coll.removeAll(list));
        expectTraversal(coll.iterator());
        assertEquals(0, coll.size());
        assertTrue(coll.isEmpty());
    }

    @Test
    public void testEmptyListIterator() {
        ImplicitLinkedHashCollection<TestElement> coll = new ImplicitLinkedHashCollection<>();
        ListIterator iter = coll.valuesList().listIterator();
        assertFalse(iter.hasNext());
        assertFalse(iter.hasPrevious());
        assertEquals(0, iter.nextIndex());
        assertEquals(-1, iter.previousIndex());
    }

    @Test
    public void testListIterator() {
        ImplicitLinkedHashCollection<TestElement> coll = new ImplicitLinkedHashCollection<>();
        coll.add(new TestElement(1));
        coll.add(new TestElement(2));
        coll.add(new TestElement(3));

        ListIterator<TestElement> iter = coll.valuesList().listIterator();
        try {
            iter.remove();
            fail("Calling remove without calling next() or previous() should raise an exception");
        } catch (IllegalStateException e) {
            // expected
        }

        assertTrue(iter.hasNext());
        assertFalse(iter.hasPrevious());
        assertEquals(0, iter.nextIndex());
        assertEquals(-1, iter.previousIndex());

        assertEquals(1, iter.next().val);
        assertTrue(iter.hasNext());
        assertTrue(iter.hasPrevious());
        assertEquals(1, iter.nextIndex());
        assertEquals(0, iter.previousIndex());

        assertEquals(2, iter.next().val);
        assertTrue(iter.hasNext());
        assertTrue(iter.hasPrevious());
        assertEquals(2, iter.nextIndex());
        assertEquals(1, iter.previousIndex());

        iter.remove();
        assertTrue(iter.hasNext());
        assertTrue(iter.hasPrevious());
        assertEquals(1, iter.nextIndex());
        assertEquals(0, iter.previousIndex());

        assertEquals(3, iter.next().val);
        assertFalse(iter.hasNext());
        assertTrue(iter.hasPrevious());
        assertEquals(2, iter.nextIndex());
        assertEquals(1, iter.previousIndex());

        assertEquals(3, iter.previous().val);
        assertTrue(iter.hasNext());
        assertTrue(iter.hasPrevious());
        assertEquals(1, iter.nextIndex());
        assertEquals(0, iter.previousIndex());

        iter.remove();
        assertFalse(iter.hasNext());
        assertTrue(iter.hasPrevious());
        assertEquals(1, iter.nextIndex());
        assertEquals(0, iter.previousIndex());

        assertEquals(1, iter.previous().val);
        assertTrue(iter.hasNext());
        assertFalse(iter.hasPrevious());
        assertEquals(0, iter.nextIndex());
        assertEquals(-1, iter.previousIndex());
    }

    @Test
    public void testCollisions() {
        ImplicitLinkedHashCollection<TestElement> coll = new ImplicitLinkedHashCollection<>(5);
        assertEquals(11, coll.numSlots());
        assertTrue(coll.add(new TestElement(11)));
        assertTrue(coll.add(new TestElement(0)));
        assertTrue(coll.add(new TestElement(22)));
        assertTrue(coll.add(new TestElement(33)));
        assertEquals(11, coll.numSlots());
        expectTraversal(coll.iterator(), 11, 0, 22, 33);
        assertTrue(coll.remove(new TestElement(22)));
        expectTraversal(coll.iterator(), 11, 0, 33);
        assertEquals(3, coll.size());
        assertFalse(coll.isEmpty());
    }

    @Test
    public void testEnlargement() {
        ImplicitLinkedHashCollection<TestElement> coll = new ImplicitLinkedHashCollection<>(5);
        assertEquals(11, coll.numSlots());
        for (int i = 0; i < 6; i++) {
            assertTrue(coll.add(new TestElement(i)));
        }
        assertEquals(23, coll.numSlots());
        assertEquals(6, coll.size());
        expectTraversal(coll.iterator(), 0, 1, 2, 3, 4, 5);
        for (int i = 0; i < 6; i++) {
            assertTrue("Failed to find element " + i, coll.contains(new TestElement(i)));
        }
        coll.remove(new TestElement(3));
        assertEquals(23, coll.numSlots());
        assertEquals(5, coll.size());
        expectTraversal(coll.iterator(), 0, 1, 2, 4, 5);
    }

    @Test
    public void testManyInsertsAndDeletes() {
        Random random = new Random(123);
        LinkedHashSet<Integer> existing = new LinkedHashSet<>();
        ImplicitLinkedHashCollection<TestElement> coll = new ImplicitLinkedHashCollection<>();
        for (int i = 0; i < 100; i++) {
            addRandomElement(random, existing, coll);
            addRandomElement(random, existing, coll);
            addRandomElement(random, existing, coll);
            removeRandomElement(random, existing, coll);
            expectTraversal(coll.iterator(), existing.iterator());
        }
    }

    @Test
    public void testValuesList() {
        ImplicitLinkedHashCollection<TestElement> coll = new ImplicitLinkedHashCollection<>();
        coll.add(new TestElement(1));
        coll.add(new TestElement(2));
        coll.add(new TestElement(3));

        List<TestElement> list = coll.valuesList();
        Iterator<TestElement> iter = list.iterator();
        assertEquals(1, iter.next().val);
        assertEquals(2, iter.next().val);
        assertEquals(3, iter.next().val);
    }

    @Test
    public void testValuesSet() {
        ImplicitLinkedHashCollection<TestElement> coll = new ImplicitLinkedHashCollection<>();
        coll.add(new TestElement(1));
        coll.add(new TestElement(2));
        coll.add(new TestElement(3));

        Set<TestElement> set = coll.valuesSet();
        assertTrue(set.contains(new TestElement(1)));
        assertTrue(set.contains(new TestElement(2)));
        assertTrue(set.contains(new TestElement(3)));

        set.add(new TestElement(4));
        assertTrue(set.contains(new TestElement(4)));

        set.remove(new TestElement(2));
        assertFalse(set.contains(new TestElement(2)));
    }

    private void addRandomElement(Random random, LinkedHashSet<Integer> existing,
                                  ImplicitLinkedHashCollection<TestElement> set) {
        int next;
        do {
            next = random.nextInt();
        } while (existing.contains(next));
        existing.add(next);
        set.add(new TestElement(next));
    }

    private void removeRandomElement(Random random, Collection<Integer> existing,
                                     ImplicitLinkedHashCollection<TestElement> coll) {
        int removeIdx = random.nextInt(existing.size());
        Iterator<Integer> iter = existing.iterator();
        Integer element = null;
        for (int i = 0; i <= removeIdx; i++) {
            element = iter.next();
        }
        existing.remove(new TestElement(element));
    }
}
