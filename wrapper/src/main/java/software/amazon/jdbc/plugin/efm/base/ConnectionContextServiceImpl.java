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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jetbrains.annotations.NotNull;
import software.amazon.jdbc.plugin.efm.v1.HostMonitorConnectionContextV1;
import software.amazon.jdbc.plugin.efm.v2.HostMonitorConnectionContextV2;
import software.amazon.jdbc.util.Messages;

public class ConnectionContextServiceImpl implements ConnectionContextService {

  private static final Logger LOGGER = Logger.getLogger(ConnectionContextServiceImpl.class.getName());

  protected static final Map<Class<? extends ConnectionContext>, Supplier<ConnectionContext>> defaultSuppliers;
  protected static final int DEFAULT_INITIAL_CAPACITY = 30;
  protected static final int DEFAULT_MAX_IDLE_COUNT = 10;
  protected static ContextPool contextPool;
  protected Supplier<ConnectionContext> contextSupplier;
  protected final int initialCapacity;
  protected final int maxIdleCount;

  static {
    Map<Class<? extends ConnectionContext>, Supplier<ConnectionContext>> suppliers = new HashMap<>();
    suppliers.put(HostMonitorConnectionContextV1.class, HostMonitorConnectionContextV1::new);
    suppliers.put(HostMonitorConnectionContextV2.class, HostMonitorConnectionContextV2::new);
    defaultSuppliers = Collections.unmodifiableMap(suppliers);
  }

  public ConnectionContextServiceImpl() {
    initialCapacity = Integer.parseInt(
        System.getProperty("aws.jdbc.efm.contextPool.initialCapacity",
            String.valueOf(DEFAULT_INITIAL_CAPACITY)));
    maxIdleCount = Integer.parseInt(
        System.getProperty("aws.jdbc.efm.contextPool.maxIdleCount",
            String.valueOf(DEFAULT_MAX_IDLE_COUNT)));

    if (initialCapacity < 1) {
      final String errMsg = Messages.get(
          "ConnectionContextServiceImpl.invalidInitialCapacity",
          new Object[] {initialCapacity, DEFAULT_INITIAL_CAPACITY});
      LOGGER.warning(errMsg);
      throw new IllegalArgumentException(errMsg);
    }

    if (maxIdleCount < 1) {
      final String errMsg = Messages.get(
          "ConnectionContextServiceImpl.invalidMaxIdleCount",
          new Object[] {maxIdleCount, DEFAULT_MAX_IDLE_COUNT});
      LOGGER.warning(errMsg);
      throw new IllegalArgumentException(errMsg);
    }
  }

  @Override
  public ConnectionContext acquire(@NotNull Class<? extends ConnectionContext> contextClass) {
    return this.acquire(contextClass, null);
  }

  @Override
  public ConnectionContext acquire(
      @NotNull Class<? extends ConnectionContext> contextClass,
      @Nullable Consumer<ConnectionContext> initializer) {
    if (contextPool == null) {
      this.contextSupplier = defaultSuppliers.get(contextClass);
      contextPool = new ContextPoolImpl(initialCapacity, maxIdleCount, contextSupplier);
    }
    final ConnectionContext context = contextPool.acquire();
    if (initializer != null) {
      initializer.accept(context);
    }
    return context;
  }

  @Override
  public boolean release(ConnectionContext context) {
    return contextPool.release(context);
  }

  public static void clear() {
    contextPool.clearPool();
  }
}
