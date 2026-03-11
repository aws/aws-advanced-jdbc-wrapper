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

import java.sql.Connection;

/**
 * Interface for connection context that manages connection state and health monitoring.
 */
public interface ConnectionContext {

  /**
   * Check if the database node is unhealthy.
   *
   * @return true if the node is unhealthy, false otherwise
   */
  boolean isNodeUnhealthy();

  /**
   * Set the health status of the database node.
   *
   * @param nodeUnhealthy true if the node is unhealthy, false otherwise
   */
  void setNodeUnhealthy(final boolean nodeUnhealthy);

  /**
   * Check if the connection should be aborted due to node failure.
   *
   * @return true if the connection should be aborted, false otherwise
   */
  boolean shouldAbort();

  /**
   * Mark this context as inactive and release any connection references.
   */
  void setInactive();

  /**
   * Get the connection associated with this context.
   *
   * @return the connection, or null if not available
   */
  Connection getConnection();

  /**
   * Check if this context is currently active.
   *
   * @return true if active, false otherwise
   */
  boolean isActive();

}
