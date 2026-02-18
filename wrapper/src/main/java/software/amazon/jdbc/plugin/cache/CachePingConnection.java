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

package software.amazon.jdbc.plugin.cache;

/**
 * Abstraction for a cache connection that can be pinged.
 * Hides cache-client implementation details (Lettuce/Glide) from CacheMonitor.
 */
public interface CachePingConnection {
  /**
   * Pings the cache server to check health.
   *
   * @return true if ping successful (PONG received), false otherwise
   */
  boolean ping();

  /**
   * Checks if the connection is open.
   *
   * @return true if connection is open, false otherwise
   */
  boolean isOpen();

  /**
   * Closes the ping connection and releases resources.
   */
  void close();
}
