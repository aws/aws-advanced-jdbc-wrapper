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
import java.util.concurrent.ConcurrentHashMap;
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
  protected static final int DEFAULT_MAX_IDLE_COUNT = 30;
  protected static final boolean DEFAULT_LAZY_INITIALIZATION = true;
  protected static final String MAX_IDLE_COUNT_PROPERTY = "efm.contextPool.maxIdleCount";
  protected static final String LAZY_INIT_PROPERTY = "efm.contextPool.lazyInitialization";
  protected static final Map<Class<? extends ConnectionContext>, ContextPool> contextPoolMap =
      new ConcurrentHashMap<>();
  protected final int maxIdleCount;
  protected final boolean lazyInitialization;

  static {
    Map<Class<? extends ConnectionContext>, Supplier<ConnectionContext>> suppliers = new HashMap<>();
    suppliers.put(HostMonitorConnectionContextV1.class, HostMonitorConnectionContextV1::new);
    suppliers.put(HostMonitorConnectionContextV2.class, HostMonitorConnectionContextV2::new);
    defaultSuppliers = Collections.unmodifiableMap(suppliers);
  }

  public ConnectionContextServiceImpl() {
    maxIdleCount = Integer.parseInt(
        System.getProperty(MAX_IDLE_COUNT_PROPERTY,
            String.valueOf(DEFAULT_MAX_IDLE_COUNT)));
    lazyInitialization = Boolean.parseBoolean(
        System.getProperty(LAZY_INIT_PROPERTY,
            String.valueOf(DEFAULT_LAZY_INITIALIZATION)));

    if (maxIdleCount < 1) {
      final String errMsg = Messages.get(
          "ConnectionContextServiceImpl.invalidMaxIdleCount",
          new Object[] {maxIdleCount, DEFAULT_MAX_IDLE_COUNT});
      LOGGER.warning(errMsg);
      throw new IllegalArgumentException(errMsg);
    }
  }

  @Override
  public <T extends ConnectionContext> T acquire(@NotNull Class<T> contextClass) {
    return this.acquire(contextClass, null);
  }

  @Override
  public <T extends ConnectionContext> T acquire(
      @NotNull Class<T> contextClass,
      @Nullable Consumer<T> initializer) {
    final Supplier<ConnectionContext> supplier = defaultSuppliers.get(contextClass);
    final ContextPool contextPool = contextPoolMap.computeIfAbsent(
        contextClass,
        ctxClass -> new ContextPoolImpl(maxIdleCount, lazyInitialization, supplier));

    final T context = (T) contextPool.acquire();
    if (initializer != null) {
      initializer.accept(context);
    }
    return context;
  }

  @Override
  public boolean release(ConnectionContext context) {
    if (context == null) {
      return false;
    }
    final ContextPool contextPool = contextPoolMap.get(context.getClass());
    if (contextPool != null) {
      return contextPool.release(context);
    }
    return false;
  }

  public static void clear() {
    contextPoolMap.values().forEach(ContextPool::clearPool);
    contextPoolMap.clear();
  }
}
