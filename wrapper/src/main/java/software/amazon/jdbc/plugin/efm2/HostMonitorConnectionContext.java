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

package software.amazon.jdbc.plugin.efm2;

import java.lang.ref.WeakReference;
import java.sql.Connection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Monitoring context for each connection. This contains each connection's criteria for whether a
 * server should be considered unhealthy. The context is shared between the main thread and the monitor thread.
 */
public class HostMonitorConnectionContext {

  private final AtomicReference<WeakReference<Connection>> connectionToAbortRef;
  private final AtomicBoolean nodeUnhealthy = new AtomicBoolean(false);

  /**
   * Constructor.
   *
   * @param connectionToAbort A reference to the connection associated with this context that will be aborted.
   */
  public HostMonitorConnectionContext(final Connection connectionToAbort) {
    this.connectionToAbortRef = new AtomicReference<>(new WeakReference<>(connectionToAbort));
  }

  public boolean isNodeUnhealthy() {
    return this.nodeUnhealthy.get();
  }

  void setNodeUnhealthy(final boolean nodeUnhealthy) {
    this.nodeUnhealthy.set(nodeUnhealthy);
  }

  public boolean shouldAbort() {
    return this.nodeUnhealthy.get() && this.connectionToAbortRef.get() != null;
  }

  public void setInactive() {
    this.connectionToAbortRef.set(null);
  }

  public Connection getConnection() {
    WeakReference<Connection> copy = this.connectionToAbortRef.get();
    return copy == null ? null : copy.get();
  }

  public boolean isActive() {
    WeakReference<Connection> copy = this.connectionToAbortRef.get();
    return copy != null && copy.get() != null;
  }
}
