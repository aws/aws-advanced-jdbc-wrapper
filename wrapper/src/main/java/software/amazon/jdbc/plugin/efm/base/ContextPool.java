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

/**
 * Pool for managing reusable connection context objects.
 */
public interface ContextPool {

  /**
   * Acquire a connection context from the pool.
   *
   * @return a connection context
   */
  ConnectionContext acquire();

  /**
   * Release a connection context back to the pool.
   *
   * @param context the context to release
   * @return true if successfully released, false otherwise
   */
  boolean release(ConnectionContext context);

  /**
   * Get the current size of the pool.
   *
   * @return the number of contexts in the pool
   */
  int size();

  /**
   * Clear all contexts from the pool.
   */
  void clearPool();
}
