/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package software.amazon.jdbc.plugin;

import java.lang.ref.WeakReference;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

/**
 * A thread-safe doubly-linked list for tracking opened connections.
 * Uses a lock-per-list approach for concurrency control.
 * Supports O(1) removal when a direct node reference is available.
 */
public class TrackedConnectionList {

  private final ReentrantLock lock = new ReentrantLock();
  private Node head;
  private Node tail;
  private int size;

  /**
   * A node in the doubly-linked list holding a weak reference to a connection.
   */
  public static class Node {
    final WeakReference<Connection> connectionRef;
    final TrackedConnectionList ownerList;
    Node prev;
    Node next;
    boolean removed;

    Node(final WeakReference<Connection> connectionRef, final TrackedConnectionList ownerList) {
      this.connectionRef = connectionRef;
      this.ownerList = ownerList;
    }
  }

  /**
   * Adds a connection to the tail of the list.
   *
   * @param connection the connection to track
   * @return the node representing this connection, for O(1) removal later
   */
  public Node add(final Connection connection) {
    final Node node = new Node(new WeakReference<>(connection), this);
    lock.lock();
    try {
      if (tail == null) {
        head = node;
        tail = node;
      } else {
        node.prev = tail;
        tail.next = node;
        tail = node;
      }
      size++;
    } finally {
      lock.unlock();
    }
    return node;
  }

  /**
   * Removes a node from the list in O(1) time.
   * Safe to call multiple times on the same node.
   *
   * @param node the node to remove
   */
  public void remove(final Node node) {
    if (node == null) {
      return;
    }
    lock.lock();
    try {
      if (node.removed) {
        return;
      }
      node.removed = true;

      if (node.prev != null) {
        node.prev.next = node.next;
      } else {
        head = node.next;
      }

      if (node.next != null) {
        node.next.prev = node.prev;
      } else {
        tail = node.prev;
      }

      node.prev = null;
      node.next = null;
      size--;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Removes all nodes from the list and returns the connections.
   * Used during invalidation to drain and abort all tracked connections.
   *
   * @return list of connections that were in the list (may contain nulls from GC'd weak refs)
   */
  public List<Connection> drainAll() {
    final List<Connection> connections = new ArrayList<>();
    lock.lock();
    try {
      Node current = head;
      while (current != null) {
        current.removed = true;
        final Connection conn = current.connectionRef.get();
        if (conn != null) {
          connections.add(conn);
        }
        final Node next = current.next;
        current.prev = null;
        current.next = null;
        current = next;
      }
      head = null;
      tail = null;
      size = 0;
    } finally {
      lock.unlock();
    }
    return connections;
  }

  /**
   * Removes nodes matching the predicate. Used for pruning GC'd or closed connections.
   *
   * @param predicate test applied to each node's weak reference
   */
  public void removeIf(final Predicate<WeakReference<Connection>> predicate) {
    lock.lock();
    try {
      Node current = head;
      while (current != null) {
        final Node next = current.next;
        if (predicate.test(current.connectionRef)) {
          current.removed = true;
          if (current.prev != null) {
            current.prev.next = current.next;
          } else {
            head = current.next;
          }
          if (current.next != null) {
            current.next.prev = current.prev;
          } else {
            tail = current.prev;
          }
          current.prev = null;
          current.next = null;
          size--;
        }
        current = next;
      }
    } finally {
      lock.unlock();
    }
  }

  public boolean isEmpty() {
    lock.lock();
    try {
      return size == 0;
    } finally {
      lock.unlock();
    }
  }

  public int size() {
    lock.lock();
    try {
      return size;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Iterates over all nodes and collects connection string representations.
   * Used for logging only.
   *
   * @param builder the StringBuilder to append to
   */
  public void appendTo(final StringBuilder builder) {
    lock.lock();
    try {
      Node current = head;
      while (current != null) {
        Connection conn = current.connectionRef.get();
        if (conn != null) {
          builder.append("\n\t\t").append(conn);
        }
        current = current.next;
      }
    } finally {
      lock.unlock();
    }
  }
}
