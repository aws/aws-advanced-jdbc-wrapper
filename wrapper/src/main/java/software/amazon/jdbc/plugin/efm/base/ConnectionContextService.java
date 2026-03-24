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
 * Service for managing connection contexts used during host monitoring.
 *
 * <p>Implementing classes may choose to use pooling to reduce memory allocation overhead,
 * or may create new context instances on each acquire call (non-pooled). The choice of
 * implementation affects memory usage and allocation patterns:
 *
 * <ul>
 *   <li><b>Pooled implementations</b> reuse context objects to minimize garbage collection
 *       pressure and allocation overhead. Released contexts are returned to the pool for
 *       subsequent reuse.</li>
 *   <li><b>Non-pooled implementations</b> create a new context instance on each acquire call.
 *       The release operation may simply mark the context as inactive without retaining it.</li>
 * </ul>
 *
 * <p>The Host Monitoring Plugins use a pooled connection context service by default.
 *
 * @see PoolConnectionContextServiceImpl
 */
public interface ConnectionContextService {

  /**
   * Acquire a connection context. Depending on the implementation, this may return a context
   * from a pool or create a new instance.
   *
   * @param <T> the type of connection context
   * @param contextClass the class type of the connection context to acquire
   * @return a connection context ready for use
   */
  <T extends ConnectionContext> T acquire(@NonNull Class<T> contextClass);

  /**
   * Acquire a connection context with optional initialization. Depending on the implementation,
   * this may return a context from a pool or create a new instance. The initializer is invoked
   * on the context before returning it.
   *
   * @param <T> the type of connection context
   * @param contextClass the class type of the connection context to acquire
   * @param initializer optional consumer to initialize the context before use; may be null
   * @return a connection context ready for use
   */
  <T extends ConnectionContext> T acquire(@NonNull Class<T> contextClass, @Nullable Consumer<T> initializer);

  /**
   * Release a connection context. For pooled implementations, the context is reset and made
   * available for subsequent acquire operations. For non-pooled implementations, the context
   * may simply be marked as inactive.
   *
   * @param context the context to release
   * @return true if the context was successfully released, false otherwise
   */
  boolean release(ConnectionContext context);
}
