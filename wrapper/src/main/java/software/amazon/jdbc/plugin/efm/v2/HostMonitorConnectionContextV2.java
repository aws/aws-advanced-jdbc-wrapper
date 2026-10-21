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

package software.amazon.jdbc.plugin.efm.v2;

import java.lang.ref.WeakReference;
import java.sql.Connection;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.plugin.efm.base.HostMonitorConnectionContext;

/**
 * Monitoring context for each connection. This contains each connection's criteria for whether a server should be
 * considered unhealthy. The context is shared between the main thread and the monitor thread.
 */
public class HostMonitorConnectionContextV2 extends HostMonitorConnectionContext {

  private long generation;

  public synchronized void init(Connection connectionToAbort) {
    this.generation++;
    this.connectionToAbortRef.set(new WeakReference<>(connectionToAbort));
    this.nodeUnhealthy.set(false);
  }

  @Override
  public synchronized void setInactive() {
    this.generation++;
    super.setInactive();
  }

  synchronized long getGeneration() {
    return this.generation;
  }

  synchronized boolean isActive(final long expectedGeneration) {
    return this.generation == expectedGeneration && this.isActive();
  }

  synchronized @Nullable Connection claimConnectionToAbort(final long expectedGeneration) {
    if (this.generation != expectedGeneration) {
      return null;
    }
    final Connection connectionToAbort = this.getConnection();
    this.connectionToAbortRef.set(null);
    this.nodeUnhealthy.set(true);
    return connectionToAbort;
  }
}
