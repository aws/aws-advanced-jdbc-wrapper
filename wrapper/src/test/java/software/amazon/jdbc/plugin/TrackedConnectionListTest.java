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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.sql.Connection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TrackedConnectionListTest {

  private TrackedConnectionList list;

  @BeforeEach
  void setUp() {
    list = new TrackedConnectionList();
  }

  @Test
  void testNewListIsEmpty() {
    assertTrue(list.isEmpty());
    assertEquals(0, list.size());
  }

  @Test
  void testAddSingleElement() {
    final Connection conn = mock(Connection.class);
    final TrackedConnectionList.Node node = list.add(conn);

    assertNotNull(node);
    assertFalse(list.isEmpty());
    assertEquals(1, list.size());
    assertSame(list, node.ownerList);
    assertSame(conn, node.connectionRef.get());
  }

  @Test
  void testAddMultipleElements() {
    final Connection conn1 = mock(Connection.class);
    final Connection conn2 = mock(Connection.class);
    final Connection conn3 = mock(Connection.class);

    list.add(conn1);
    list.add(conn2);
    list.add(conn3);

    assertEquals(3, list.size());
  }

  @Test
  void testRemoveHead() {
    final Connection conn1 = mock(Connection.class);
    final Connection conn2 = mock(Connection.class);
    final TrackedConnectionList.Node node1 = list.add(conn1);
    list.add(conn2);

    list.remove(node1);

    assertEquals(1, list.size());
    final List<Connection> drained = list.drainAll();
    assertEquals(1, drained.size());
    assertSame(conn2, drained.get(0));
  }

  @Test
  void testRemoveTail() {
    final Connection conn1 = mock(Connection.class);
    final Connection conn2 = mock(Connection.class);
    list.add(conn1);
    final TrackedConnectionList.Node node2 = list.add(conn2);

    list.remove(node2);

    assertEquals(1, list.size());
    final List<Connection> drained = list.drainAll();
    assertEquals(1, drained.size());
    assertSame(conn1, drained.get(0));
  }

  @Test
  void testRemoveMiddle() {
    final Connection conn1 = mock(Connection.class);
    final Connection conn2 = mock(Connection.class);
    final Connection conn3 = mock(Connection.class);
    list.add(conn1);
    final TrackedConnectionList.Node node2 = list.add(conn2);
    list.add(conn3);

    list.remove(node2);

    assertEquals(2, list.size());
    final List<Connection> drained = list.drainAll();
    assertEquals(2, drained.size());
    assertSame(conn1, drained.get(0));
    assertSame(conn3, drained.get(1));
  }

  @Test
  void testRemoveOnlyElement() {
    final Connection conn = mock(Connection.class);
    final TrackedConnectionList.Node node = list.add(conn);

    list.remove(node);

    assertTrue(list.isEmpty());
    assertEquals(0, list.size());
  }

  @Test
  void testRemoveSameNodeTwiceIsIdempotent() {
    final Connection conn1 = mock(Connection.class);
    final Connection conn2 = mock(Connection.class);
    list.add(conn1);
    final TrackedConnectionList.Node node2 = list.add(conn2);

    list.remove(node2);
    list.remove(node2);

    assertEquals(1, list.size());
  }

  @Test
  void testRemoveNullIsNoOp() {
    list.add(mock(Connection.class));
    list.remove(null);
    assertEquals(1, list.size());
  }

  @Test
  void testDrainAllReturnsAllConnections() {
    final Connection conn1 = mock(Connection.class);
    final Connection conn2 = mock(Connection.class);
    list.add(conn1);
    list.add(conn2);

    final List<Connection> drained = list.drainAll();

    assertEquals(2, drained.size());
    assertSame(conn1, drained.get(0));
    assertSame(conn2, drained.get(1));
    assertTrue(list.isEmpty());
    assertEquals(0, list.size());
  }

  @Test
  void testDrainAllOnEmptyList() {
    final List<Connection> drained = list.drainAll();
    assertTrue(drained.isEmpty());
  }

  @Test
  void testDrainAllMarksNodesAsRemoved() {
    final Connection conn = mock(Connection.class);
    final TrackedConnectionList.Node node = list.add(conn);

    list.drainAll();

    assertTrue(node.removed);
  }

  @Test
  void testRemoveIfMatchingPredicate() {
    final Connection conn1 = mock(Connection.class);
    final Connection conn2 = mock(Connection.class);
    final Connection conn3 = mock(Connection.class);
    list.add(conn1);
    list.add(conn2);
    list.add(conn3);

    list.removeIf(ref -> ref.get() == conn2);

    assertEquals(2, list.size());
    final List<Connection> drained = list.drainAll();
    assertSame(conn1, drained.get(0));
    assertSame(conn3, drained.get(1));
  }

  @Test
  void testRemoveIfNoMatch() {
    final Connection conn1 = mock(Connection.class);
    list.add(conn1);

    list.removeIf(ref -> false);

    assertEquals(1, list.size());
  }

  @Test
  void testRemoveIfAllMatch() {
    list.add(mock(Connection.class));
    list.add(mock(Connection.class));
    list.add(mock(Connection.class));

    list.removeIf(ref -> true);

    assertTrue(list.isEmpty());
  }

  @Test
  void testRemoveIfGarbageCollectedRefs() {
    // Add a connection without keeping a strong reference
    list.add(mock(Connection.class));
    final Connection kept = mock(Connection.class);
    list.add(kept);

    // Remove entries where the weak ref has been cleared (simulated by null check)
    list.removeIf(ref -> ref.get() == null);

    // The mock is still strongly referenced by the test, so nothing should be removed
    assertEquals(2, list.size());
  }

  @Test
  void testRemoveIfRemovesHeadAndTail() {
    final Connection conn1 = mock(Connection.class);
    final Connection conn2 = mock(Connection.class);
    final Connection conn3 = mock(Connection.class);
    list.add(conn1);
    list.add(conn2);
    list.add(conn3);

    list.removeIf(ref -> ref.get() == conn1 || ref.get() == conn3);

    assertEquals(1, list.size());
    final List<Connection> drained = list.drainAll();
    assertSame(conn2, drained.get(0));
  }

  @Test
  void testAppendToEmptyList() {
    final StringBuilder builder = new StringBuilder();
    list.appendTo(builder);
    assertEquals("", builder.toString());
  }

  @Test
  void testAppendToWithElements() {
    list.add(mock(Connection.class));
    list.add(mock(Connection.class));

    final StringBuilder builder = new StringBuilder();
    list.appendTo(builder);

    // Each entry should produce a "\n\t\t" prefix
    final String result = builder.toString();
    assertTrue(result.startsWith("\n\t\t"));
    // Two entries means two occurrences of the prefix
    assertEquals(2, result.split("\n\t\t").length - 1);
  }

  @Test
  void testAddAfterDrainAll() {
    list.add(mock(Connection.class));
    list.drainAll();

    final Connection conn = mock(Connection.class);
    list.add(conn);

    assertEquals(1, list.size());
    final List<Connection> drained = list.drainAll();
    assertSame(conn, drained.get(0));
  }

  @Test
  void testAddAfterRemoveAll() {
    final TrackedConnectionList.Node node = list.add(mock(Connection.class));
    list.remove(node);

    assertTrue(list.isEmpty());

    final Connection conn = mock(Connection.class);
    list.add(conn);
    assertEquals(1, list.size());
  }

  @Test
  void testRemoveAfterDrainIsNoOp() {
    final Connection conn = mock(Connection.class);
    final TrackedConnectionList.Node node = list.add(conn);

    list.drainAll();
    // Node is already marked removed by drainAll, this should be a no-op
    list.remove(node);

    assertTrue(list.isEmpty());
  }

  @Test
  void testConcurrentAddAndRemove() throws InterruptedException {
    final int threadCount = 10;
    final int opsPerThread = 100;
    final ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch doneLatch = new CountDownLatch(threadCount);

    for (int t = 0; t < threadCount; t++) {
      executor.submit(() -> {
        try {
          startLatch.await();
          for (int i = 0; i < opsPerThread; i++) {
            final TrackedConnectionList.Node node = list.add(mock(Connection.class));
            list.remove(node);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          doneLatch.countDown();
        }
      });
    }

    startLatch.countDown();
    assertTrue(doneLatch.await(10, TimeUnit.SECONDS));
    executor.shutdown();

    // All adds were followed by removes, list should be empty
    assertTrue(list.isEmpty());
    assertEquals(0, list.size());
  }

  @Test
  void testConcurrentAddAndDrain() throws InterruptedException {
    final int addThreads = 5;
    final int connectionsPerThread = 50;
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch addDoneLatch = new CountDownLatch(addThreads);
    final ExecutorService executor = Executors.newFixedThreadPool(addThreads + 1);

    for (int t = 0; t < addThreads; t++) {
      executor.submit(() -> {
        try {
          startLatch.await();
          for (int i = 0; i < connectionsPerThread; i++) {
            list.add(mock(Connection.class));
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          addDoneLatch.countDown();
        }
      });
    }

    startLatch.countDown();
    assertTrue(addDoneLatch.await(10, TimeUnit.SECONDS));

    // After all adds complete, drain should get everything
    final List<Connection> drained = list.drainAll();
    assertEquals(addThreads * connectionsPerThread, drained.size());
    assertTrue(list.isEmpty());

    executor.shutdown();
  }
}
