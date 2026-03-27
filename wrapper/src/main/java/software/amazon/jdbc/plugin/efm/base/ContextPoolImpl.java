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

package software.amazon.jdbc.plugin.efm.base;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.util.telemetry.NullTelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryGauge;

public class ContextPoolImpl implements ContextPool {

  private static final TelemetryFactory NULL_TELEMETRY_FACTORY = new NullTelemetryFactory();

  private final Queue<ConnectionContext> contextQueue = new ConcurrentLinkedDeque<>();
  private final Supplier<ConnectionContext> contextSupplier;
  private final int maxIdleCount;
  private final boolean lazyInitialization;
  private final ReentrantLock lock = new ReentrantLock();
  private volatile boolean initialized = false;
  private final TelemetryGauge poolSizeGauge;

  public ContextPoolImpl(int maxIdleCount, Supplier<ConnectionContext> contextSupplier) {
    this(maxIdleCount, false, contextSupplier, NULL_TELEMETRY_FACTORY);
  }

  public ContextPoolImpl(int maxIdleCount, boolean lazyInitialization, Supplier<ConnectionContext> contextSupplier) {
    this(maxIdleCount, lazyInitialization, contextSupplier, NULL_TELEMETRY_FACTORY);
  }

  public ContextPoolImpl(
      int maxIdleCount,
      boolean lazyInitialization,
      Supplier<ConnectionContext> contextSupplier,
      TelemetryFactory telemetryFactory) {
    this.contextSupplier = contextSupplier;
    this.maxIdleCount = maxIdleCount;
    this.lazyInitialization = lazyInitialization;
    this.poolSizeGauge = telemetryFactory.createGauge("efm.contextPool.size", () -> (long) contextQueue.size());
  }

  private void initializePool(final int contextCount) {
    if (!initialized) {
      lock.lock();
      try {
        if (!initialized) {
          // Initialize with an extra context to be used right away
          int i = 0;
          while (i++ < contextCount && contextQueue.size() < maxIdleCount) {
            contextQueue.add(contextSupplier.get());
          }
          initialized = contextQueue.size() >= maxIdleCount;
        }
      } finally {
        lock.unlock();
      }
    }
  }

  @Override
  public ConnectionContext acquire() {
    if (!lazyInitialization) {
      // Initialize all contexts right away.
      initializePool(maxIdleCount);
    }

    ConnectionContext context = contextQueue.poll();

    if (context == null) {
      context = contextSupplier.get();
    }
    
    this.modifyPoolSize();

    return context;
  }

  private void modifyPoolSize() {
    lock.lock();
    try {
      if (contextQueue.size() < maxIdleCount) {
        // Expand pool using lazy initialization.
        // Add at most two idle contexts every acquire call until maxIdleCount has been reached.
        initializePool(2);
      } else if (contextQueue.size() > maxIdleCount) {
        // Shrink pool if over capacity.
        ConnectionContext extra = contextQueue.poll();
        if (extra != null) {
          extra.setInactive();
        }
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean release(final ConnectionContext context) {
    if (context == null) {
      return false;
    }

    lock.lock();
    try {
      if (contextQueue.size() < maxIdleCount) {
        // Only return to the pool if pool still has capacity.
        contextQueue.add(context);
        return true;
      }
    } finally {
      lock.unlock();
    }

    return false;
  }

  @Override
  public int size() {
    return contextQueue.size();
  }

  @Override
  public void clearPool() {
    lock.lock();
    try {
      ConnectionContext context;
      while ((context = contextQueue.poll()) != null) {
        context.setInactive();
      }
      contextQueue.clear();
    } finally {
      lock.unlock();
      this.initialized = false;
    }
  }
}
