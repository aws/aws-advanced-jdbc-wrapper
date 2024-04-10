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

package software.amazon.jdbc;

import java.sql.Timestamp;
import java.time.Instant;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.hostavailability.HostAvailabilityStrategy;

public class HostSpecBuilder {
  private String host;
  private String hostId;
  private int port = HostSpec.NO_PORT;
  private HostAvailability availability = HostAvailability.AVAILABLE;
  private HostRole role = HostRole.WRITER;
  private long weight = HostSpec.DEFAULT_WEIGHT; // Greater than or equal to 0. Healthier nodes have lower weights.
  private Timestamp lastUpdateTime;
  private HostAvailabilityStrategy hostAvailabilityStrategy;

  public HostSpecBuilder(final @NonNull HostAvailabilityStrategy hostAvailabilityStrategy) {
    this.hostAvailabilityStrategy = hostAvailabilityStrategy;
  }

  public HostSpecBuilder(HostSpecBuilder hostSpecBuilder) {
    this.host = hostSpecBuilder.host;
    this.port = hostSpecBuilder.port;
    this.hostId = hostSpecBuilder.hostId;
    this.availability = hostSpecBuilder.availability;
    this.role = hostSpecBuilder.role;
    this.weight = hostSpecBuilder.weight;
    this.lastUpdateTime = hostSpecBuilder.lastUpdateTime;
    this.hostAvailabilityStrategy = hostSpecBuilder.hostAvailabilityStrategy;
  }

  public HostSpecBuilder copyFrom(final HostSpec hostSpec) {
    this.host = hostSpec.host;
    this.hostId = hostSpec.hostId;
    this.role = hostSpec.role;
    this.port = hostSpec.port;
    this.availability = hostSpec.availability;
    this.lastUpdateTime = hostSpec.lastUpdateTime;
    this.weight = hostSpec.weight;
    return this;
  }

  public HostSpecBuilder host(String host) {
    this.host = host;
    return this;
  }

  public HostSpecBuilder port(int port) {
    this.port = port;
    return this;
  }

  public HostSpecBuilder hostId(String hostId) {
    this.hostId = hostId;
    return this;
  }

  public HostSpecBuilder availability(HostAvailability availability) {
    this.availability = availability;
    return this;
  }

  public HostSpecBuilder role(HostRole role) {
    this.role = role;
    return this;
  }

  public HostSpecBuilder weight(long weight) {
    this.weight = weight;
    return this;
  }

  public HostSpecBuilder hostAvailabilityStrategy(HostAvailabilityStrategy hostAvailabilityStrategy) {
    this.hostAvailabilityStrategy = hostAvailabilityStrategy;
    return this;
  }

  public HostSpecBuilder lastUpdateTime(Timestamp lastUpdateTime) {
    this.lastUpdateTime = lastUpdateTime;
    return this;
  }

  public HostSpec build() {
    checkHostIsSet();
    return new HostSpec(this.host, this.port, this.hostId, this.role, this.availability,
        this.weight, this.lastUpdateTime, this.hostAvailabilityStrategy);
  }

  private void checkHostIsSet() {
    if (this.host == null) {
      throw new IllegalArgumentException("host parameter must be set.");
    }
  }
}
