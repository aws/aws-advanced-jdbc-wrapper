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

package software.amazon.jdbc.util.events;

import java.util.Objects;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Event published after a topology monitor successfully refreshes its cached cluster topology. Subscribers can use this
 * to invalidate any per-host state that should be recomputed once fresh load/availability data is available.
 */
public class TopologyRefreshedEvent implements Event {

  private final @NonNull String clusterId;

  public TopologyRefreshedEvent(final @NonNull String clusterId) {
    this.clusterId = clusterId;
  }

  public @NonNull String getClusterId() {
    return clusterId;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj == null) {
      return false;
    }

    if (getClass() != obj.getClass()) {
      return false;
    }

    TopologyRefreshedEvent event = (TopologyRefreshedEvent) obj;
    return Objects.equals(this.clusterId, event.clusterId);
  }

  @Override
  public int hashCode() {
    return this.clusterId.hashCode();
  }

  @Override
  public boolean isImmediateDelivery() {
    return false;
  }
}
