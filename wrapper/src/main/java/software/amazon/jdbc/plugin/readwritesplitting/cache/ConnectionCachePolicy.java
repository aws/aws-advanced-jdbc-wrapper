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

package software.amazon.jdbc.plugin.readwritesplitting.cache;

/**
 * Computes the keep-alive deadline for a cached connection ("when to keep or release
 * connections?").
 */
public interface ConnectionCachePolicy {

  /**
   * Returns the {@link System#nanoTime()}-based deadline after which a cached connection may be
   * released, or {@code 0} to keep it (pool-managed, or reuse indefinitely).
   *
   * @param fromPool whether the connection came from an internal connection pool
   * @return the deadline in nanoTime units, or {@code 0}
   */
  long keepAliveDeadlineNanos(boolean fromPool);
}
