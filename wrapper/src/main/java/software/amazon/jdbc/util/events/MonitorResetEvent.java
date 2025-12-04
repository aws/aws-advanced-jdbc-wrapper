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
import java.util.Set;
import org.checkerframework.checker.nullness.qual.NonNull;

public class MonitorResetEvent implements Event {

  private final @NonNull String clusterId;
  private final @NonNull Set<String> endpoints;

  public MonitorResetEvent(final @NonNull String clusterId, final @NonNull Set<String> endpoints) {
    this.clusterId = clusterId;
    this.endpoints = endpoints;
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

    MonitorResetEvent event = (MonitorResetEvent) obj;
    return Objects.equals(this.clusterId, event.clusterId)
        && Objects.equals(this.endpoints, event.endpoints);
  }

  public @NonNull String getClusterId() {
    return clusterId;
  }

  public @NonNull Set<String> getEndpoints() {
    return endpoints;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + this.clusterId.hashCode();
    result = prime * result + this.endpoints.hashCode();
    return result;
  }

  @Override
  public boolean isImmediateDelivery() {
    return true;
  }
}
