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

import java.util.function.Consumer;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Service for managing a pool of reusable connection contexts to reduce memory allocation overhead.
 * This service provides efficient context lifecycle management through pooling, minimizing the cost
 * of repeated allocations during monitoring operations.
 */
public interface ConnectionContextService {

  /**
   * Acquire a connection context from the pool. If the pool is empty, a new context is created.
   *
   * @param <T> the type of connection context
   * @param contextClass the class type of the connection context to acquire
   * @return a connection context ready for use
   */
  <T extends ConnectionContext> T acquire(@NonNull Class<T> contextClass);

  /**
   * Acquire a connection context from the pool with optional initialization. If the pool is empty,
   * a new context is created. The initializer is invoked on the context before returning it.
   *
   * @param <T> the type of connection context
   * @param contextClass the class type of the connection context to acquire
   * @param initializer optional consumer to initialize the context before use; may be null
   * @return a connection context ready for use
   */
  <T extends ConnectionContext> T acquire(@NonNull Class<T> contextClass, @Nullable Consumer<T> initializer);

  /**
   * Release a connection context back to the pool for reuse. The context is reset and made
   * available for subsequent acquire operations.
   *
   * @param context the context to release
   * @return true if the context was successfully released, false otherwise
   */
  boolean release(ConnectionContext context);
}
