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
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.plugin.efm.v1.HostMonitorConnectionContextV1;
import software.amazon.jdbc.plugin.efm.v2.HostMonitorConnectionContextV2;
import software.amazon.jdbc.util.Messages;

/**
 * A non-pooled implementation of {@link ConnectionContextService} that creates a new context
 * instance on each acquire call.
 */
public class SimpleConnectionContextServiceImpl implements ConnectionContextService {

  protected static final Map<Class<? extends ConnectionContext>, Supplier<ConnectionContext>> defaultSuppliers;

  static {
    Map<Class<? extends ConnectionContext>, Supplier<ConnectionContext>> suppliers = new HashMap<>();
    suppliers.put(HostMonitorConnectionContextV1.class, HostMonitorConnectionContextV1::new);
    suppliers.put(HostMonitorConnectionContextV2.class, HostMonitorConnectionContextV2::new);
    defaultSuppliers = Collections.unmodifiableMap(suppliers);
  }

  @Override
  public <T extends ConnectionContext> T acquire(@NonNull Class<T> contextClass) {
    return this.acquire(contextClass, null);
  }

  @Override
  public <T extends ConnectionContext> T acquire(
      @NonNull Class<T> contextClass,
      @Nullable Consumer<T> initializer) {
    final Supplier<ConnectionContext> supplier = defaultSuppliers.get(contextClass);
    if (supplier == null) {
      throw new IllegalArgumentException(
          Messages.get("ConnectionContextService.noSupplierRegistered",
              new Object[] {contextClass.getName()}));
    }

    final T context = contextClass.cast(supplier.get());
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
    context.setInactive();
    return true;
  }
}
