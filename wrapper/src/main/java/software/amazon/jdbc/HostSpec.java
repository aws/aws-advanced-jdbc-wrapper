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
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.hostavailability.HostAvailabilityStrategy;
import software.amazon.jdbc.util.ResourceLock;

/**
 * An object representing connection info for a given host. Modifiable fields are thread-safe to support sharing this
 * object with the EFM monitor thread.
 */
public class HostSpec {

  public static final int NO_PORT = -1;
  public static final long DEFAULT_WEIGHT = 100;

  protected final String host; // full domain name
  protected final int port;
  protected final @Nullable HostRole role;
  protected final Timestamp lastUpdateTime;
  protected volatile HostAvailability availability;
  protected long weight; // Greater or equal 0. Lesser the weight, the healthier node.
  protected @Nullable Float cpuPercent;
  protected @Nullable Float lagMs;
  // id; could be a node name, node domain name, or some gibberish code
  protected final @Nullable String hostId;
  protected HostAvailabilityStrategy hostAvailabilityStrategy;

  protected final ResourceLock resourceLock = new ResourceLock();
  protected volatile @Nullable String hostAndPort = null;
  protected volatile @Nullable String url = null;
  protected volatile @Nullable String toString = null;

  private HostSpec(
      final String host,
      final int port,
      final @Nullable String hostId,
      final @Nullable HostRole role,
      final HostAvailability availability,
      final HostAvailabilityStrategy hostAvailabilityStrategy) {

    this(host, port, hostId, role, availability, DEFAULT_WEIGHT,
        Timestamp.from(Instant.now()), hostAvailabilityStrategy);
  }

  HostSpec(
      final String host,
      final int port,
      final @Nullable String hostId,
      final @Nullable HostRole role,
      final HostAvailability availability,
      final long weight,
      final Timestamp lastUpdateTime,
      final HostAvailabilityStrategy hostAvailabilityStrategy) {
    this(host, port, hostId, role, availability, weight, null,
        null, lastUpdateTime, hostAvailabilityStrategy);
  }

  HostSpec(
      final String host,
      final int port,
      final @Nullable String hostId,
      final @Nullable HostRole role,
      final HostAvailability availability,
      final long weight,
      final @Nullable Float cpuPercent,
      final @Nullable Float lagMs,
      final Timestamp lastUpdateTime,
      final HostAvailabilityStrategy hostAvailabilityStrategy) {

    this.host = host;
    this.port = port;
    this.hostId = hostId;
    this.availability = availability;
    this.role = role;
    this.weight = weight;
    this.cpuPercent = cpuPercent;
    this.lagMs = lagMs;
    this.lastUpdateTime = lastUpdateTime;
    this.hostAvailabilityStrategy = hostAvailabilityStrategy;
  }

  /**
   * Creates a copy of the passed in {@link HostSpec} but with the specified role.
   *
   * @param copyHost the host whose details to copy.
   * @param role     the role of this host (writer or reader).
   */
  public HostSpec(final HostSpec copyHost, final @Nullable HostRole role) {
    this(copyHost.getHost(), copyHost.getPort(), copyHost.getHostId(), role, copyHost.getAvailability(),
        copyHost.getHostAvailabilityStrategy());
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public boolean isPortSpecified() {
    return port != NO_PORT;
  }

  public @Nullable HostRole getRole() {
    return this.role;
  }

  public HostAvailability getAvailability() {
    if (this.hostAvailabilityStrategy != null) {
      return this.hostAvailabilityStrategy.getHostAvailability(this.availability);
    }
    return this.availability;
  }

  public HostAvailability getRawAvailability() {
    return this.availability;
  }

  public void setAvailability(final HostAvailability availability) {
    try (ResourceLock ignored = this.resourceLock.obtain()) {
      this.toString = null;
      this.availability = availability;
    }
    if (this.hostAvailabilityStrategy != null) {
      this.hostAvailabilityStrategy.setHostAvailability(availability);
    }
  }

  public HostAvailabilityStrategy getHostAvailabilityStrategy() {
    return this.hostAvailabilityStrategy;
  }

  public void setHostAvailabilityStrategy(final HostAvailabilityStrategy hostAvailabilityStrategy) {
    this.hostAvailabilityStrategy = hostAvailabilityStrategy;
  }

  public Timestamp getLastUpdateTime() {
    return this.lastUpdateTime;
  }

  public long getWeight() {
    return this.weight;
  }

  public void setWeight(long weight) {
    try (ResourceLock ignored = this.resourceLock.obtain()) {
      this.toString = null;
      this.weight = weight;
    }
  }

  public @Nullable Float getCpuPercent() {
    return this.cpuPercent;
  }

  public @Nullable Float getLagMs() {
    return this.lagMs;
  }

  public String getUrl() {
    String result = this.url;
    if (result == null) {
      try (ResourceLock ignored = this.resourceLock.obtain()) {
        result = this.url;
        if (result == null) {
          result = this.getHostAndPort() + "/";
          this.url = result;
        }
      }
    }
    return result;
  }

  public String getHostAndPort() {
    String result = this.hostAndPort;
    if (result == null) {
      try (ResourceLock ignored = this.resourceLock.obtain()) {
        result = this.hostAndPort;
        if (result == null) {
          result = this.isPortSpecified() ? host + ":" + port : host;
          this.hostAndPort = result;
        }
      }
    }
    return result;
  }

  public @Nullable String getHostId() {
    return this.hostId;
  }

  public String toString() {
    String result = this.toString;
    if (result == null) {
      try (ResourceLock ignored = this.resourceLock.obtain()) {
        result = this.toString;
        if (result == null) {
          result = String.format(
              "HostSpec@%s [hostId=%s, host=%s, port=%d, %s, %s, weight=%d, cpuPercent=%s, lagMs=%s, %s]",
              Integer.toHexString(System.identityHashCode(this)),
              this.hostId, this.host, this.port, this.role, this.availability, this.weight,
              this.cpuPercent != null ? String.format("%.2f", this.cpuPercent) : "N/A",
              this.lagMs != null ? String.format("%.2f", this.lagMs) : "N/A",
              this.lastUpdateTime);
          this.toString = result;
        }
      }
    }
    return result;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.host, this.port, this.role);
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof HostSpec)) {
      return false;
    }

    final HostSpec spec = (HostSpec) obj;
    return Objects.equals(this.host, spec.host)
        && this.port == spec.port
        && this.availability == spec.availability
        && this.role == spec.role
        && this.weight == spec.weight
        && Objects.equals(this.cpuPercent, spec.cpuPercent)
        && Objects.equals(this.lagMs, spec.lagMs);
  }
}
