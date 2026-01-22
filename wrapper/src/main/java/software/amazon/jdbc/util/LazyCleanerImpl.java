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

/* changes were made to move it into the software.amazon.jdbc.util package
 *
 * Copyright 2022 Juan Lopes
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package software.amazon.jdbc.util;

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.time.Duration;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BooleanSupplier;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LazyCleanerImpl implements LazyCleaner {
  private static final Logger LOGGER = Logger.getLogger(LazyCleanerImpl.class.getName());

  private static final LazyCleanerImpl instance = new LazyCleanerImpl(
      "AWS-JDBC-Cleaner",
      Duration.ofMillis(Long.getLong("aws.jdbc.config.cleanup.thread.ttl", 30000))
  );

  private final ReferenceQueue<Object> queue = new ReferenceQueue<>();
  private final String threadName;
  private final Duration threadTtl;
  private boolean threadRunning;
  private Node<?> first;

  public LazyCleanerImpl(String threadName, Duration threadTtl) {
    this.threadName = threadName;
    this.threadTtl = threadTtl;
  }

  public static LazyCleanerImpl getInstance() {
    return instance;
  }

  @Override
  public Cleanable register(Object obj, CleaningAction action) {
    return add(new Node<>(obj, action));
  }

  public synchronized boolean isThreadRunning() {
    return threadRunning;
  }

  private synchronized boolean checkEmpty() {
    if (first == null) {
      threadRunning = false;
      return true;
    }
    return false;
  }

  private synchronized <T> Node<T> add(Node<T> node) {
    if (first != null) {
      node.next = first;
      first.prev = node;
    }
    first = node;
    if (!threadRunning) {
      threadRunning = startThread();
    }
    return node;
  }

  private boolean startThread() {
    ForkJoinPool.commonPool().execute(() -> {
      Thread.currentThread().setContextClassLoader(null);
      RefQueueBlocker<Object> blocker = new RefQueueBlocker<>(queue, threadName, threadTtl, this::checkEmpty);
      while (!checkEmpty()) {
        try {
          ForkJoinPool.managedBlock(blocker);
          @SuppressWarnings("unchecked")
          Node<Object> ref = (Node<Object>) blocker.drainOne();
          if (ref != null) {
            ref.onClean(true);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          if (!blocker.isReleasable()) {
            LOGGER.log(Level.FINE, Messages.get("LazyCleanerImpl.interruptEmptyQueue"));
            break;
          }
          LOGGER.log(Level.FINE, Messages.get("LazyCleanerImpl.interruptNotEmptyQueue"));
        } catch (Throwable e) {
          LOGGER.log(Level.WARNING, Messages.get("LazyCleanerImpl.unexpectedError"), e);
        }
      }
    });
    return true;
  }

  private synchronized boolean remove(Node<?> node) {
    if (node.next == node) {
      return false; // Already removed
    }
    if (first == node) {
      first = node.next;
    }

    // We don't need to traverse entire list since it's a double-linked list
    // so the node has all necessary pointers to efficiently remove itself from the list.
    if (node.next != null) {
      node.next.prev = node.prev;
    }
    if (node.prev != null) {
      node.prev.next = node.next;
    }

    // Mark as removed
    node.next = node;
    node.prev = node;
    return true;
  }

  private class Node<T> extends PhantomReference<T> implements Cleanable, CleaningAction {
    private final CleaningAction action;
    private Node<?> prev;
    private Node<?> next;

    Node(T referent, CleaningAction action) {
      super(referent, queue);
      this.action = action;
    }

    @Override
    public void clean() throws Exception {
      onClean(false);
    }

    @Override
    public void onClean(boolean leak) throws Exception {
      if (!remove(this)) {
        return;
      }
      action.onClean(leak);
    }
  }

  private static class RefQueueBlocker<T> implements ForkJoinPool.ManagedBlocker {
    private final ReferenceQueue<T> queue;
    private final String threadName;
    private Reference<? extends T> ref;
    private final long blockTimeoutMillis;
    private final BooleanSupplier shouldTerminate;

    RefQueueBlocker(
        ReferenceQueue<T> queue,
        String threadName,
        Duration blockTimeout,
        BooleanSupplier shouldTerminate) {
      this.queue = queue;
      this.threadName = threadName;
      this.blockTimeoutMillis = blockTimeout.toMillis();
      this.shouldTerminate = shouldTerminate;
    }

    @Override
    public boolean isReleasable() {
      if (ref != null || shouldTerminate.getAsBoolean()) {
        return true;
      }
      ref = queue.poll();
      return ref != null;
    }

    @Override
    public boolean block() throws InterruptedException {
      if (isReleasable()) {
        return true;
      }
      Thread currentThread = Thread.currentThread();
      String oldName = currentThread.getName();
      try {
        currentThread.setName(threadName);
        ref = queue.remove(blockTimeoutMillis);
      } finally {
        currentThread.setName(oldName);
      }
      return false;
    }

    public Reference<? extends T> drainOne() {
      Reference<? extends T> ref = this.ref;
      this.ref = null;
      return ref;
    }
  }
}
