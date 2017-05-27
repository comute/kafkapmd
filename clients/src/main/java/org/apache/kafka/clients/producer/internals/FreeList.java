package org.apache.kafka.clients.producer.internals;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;


public class FreeList {
    static class Node {
        ByteBuffer item;
        Node next = null;

        Node(ByteBuffer item, Node next) {
            this.item = item;
            this.next = next;
        }
    }

    private ArrayDeque<Node> free;
    private Node head = null;

    FreeList(ByteBuffer buffer) {
        this.free = new ArrayDeque<>();
        Node node = new Node(buffer, null);
        head = node;
        node.next = head;
        this.free.add(node);
    }

    void add(ByteBuffer buffer) {
        Node current = head;
        do {
            current = current.next;
        } while (current.item.limit() <= buffer.position() && buffer.limit() <= current.next.item.position() && current != head);

        Node newNode = new Node(buffer, current.next);
        current.next = newNode;
        this.free.add(newNode);

        head = current;
    }

    ByteBuffer find(int size) {
        ByteBuffer newBuffer = null;
        Node candidate = this.free.peek();
        ByteBuffer freeBuffer = candidate.item;
        int capacity = freeBuffer.limit() - freeBuffer.position();
        if (capacity > size) {
            newBuffer = freeBuffer.duplicate();

            // split the buffer
            int newBoundary = freeBuffer.position() + size;
            newBuffer.limit(newBoundary);
            freeBuffer.position(newBoundary);
        } else if (capacity == size) {
            // tidy up free node
//            candidate.prev.next = candidate.next;
//            candidate.next.prev = candidate.prev;

            // remove candidate
            this.free.remove();

            newBuffer = freeBuffer;
        }
        return newBuffer;
    }

    int size() {
        int sum = 0;
        for (Node node: this.free) {
            ByteBuffer freeBuffer = node.item;
            sum += freeBuffer.limit() - freeBuffer.position();
        }
        return sum;
    }

    boolean isEmpty() {
        return this.free.isEmpty();
    }

    int max() {
        int max = 0;
        for (Node node: this.free) {
            ByteBuffer freeBuffer = node.item;
            int capacity = freeBuffer.limit() - freeBuffer.position();
            if (capacity > max) {
                max = capacity;
            }
        }
        return max;
    }
}
