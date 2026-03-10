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
import java.util.function.Supplier;
import java.util.logging.Logger;
import software.amazon.jdbc.util.Messages;

public class ContextPoolImpl implements ContextPool {

  private static final Logger LOGGER = Logger.getLogger(ContextPoolImpl.class.getName());
  private final Queue<ConnectionContext> contextQueue = new ConcurrentLinkedDeque<>();
  private final Supplier<ConnectionContext> contextSupplier;
  private final int maxIdleCount;
  private final Object lock = new Object();

  public ContextPoolImpl(
      int initialCapacity,
      int maxIdleCount,
      Supplier<ConnectionContext> contextSupplier) {
    synchronized (lock) {
      for (int i = 0; i < initialCapacity; i++) {
        contextQueue.add(contextSupplier.get());
      }
    }

    this.contextSupplier = contextSupplier;
    this.maxIdleCount = maxIdleCount;
  }

  @Override
  public ConnectionContext acquire() {
    ConnectionContext context = contextQueue.poll();
    synchronized (lock) {
      if (contextQueue.size() > this.maxIdleCount) {
        ConnectionContext extraContext = contextQueue.poll();
        if (extraContext != null) {
          extraContext.setInactive();
          LOGGER.finest(() -> Messages.get("ContextPoolImpl.releasedExtraContext"));
        }
      }
    }
    
    if (context == null) {
      context = this.contextSupplier.get();
    }
    return context;
  }

  @Override
  public boolean release(final ConnectionContext context) {
    if (context != null) {
      context.setInactive();
      contextQueue.add(context);
      return true;
    }
    return false;
  }

  @Override
  public int size() {
    return contextQueue.size();
  }

  @Override
  public void clearPool() {
    contextQueue.clear();
  }

  private void logContextQueue() {
    LOGGER.finest(() -> Messages.get("ContextPoolImpl.contextQueueSize", new Object[] {contextQueue.size()}));
  }
}
