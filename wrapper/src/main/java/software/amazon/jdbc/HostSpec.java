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
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An object representing connection info for a given host. Modifiable fields are thread-safe to support sharing this
 * object with the EFM monitor thread.
 */
public class HostSpec {

  public static final int NO_PORT = -1;
  private static final long DEFAULT_WEIGHT = 100;

  protected final String host;
  protected final int port;
  protected volatile HostAvailability availability;
  protected HostRole role;
  protected Set<String> aliases = ConcurrentHashMap.newKeySet();
  protected Set<String> allAliases = ConcurrentHashMap.newKeySet();
  protected long weight; // Greater or equal 0. Lesser the weight, the healthier node.
  protected String hostId;
  protected String lastUpdateTime;
  private final DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

  public HostSpec(final String host) {
    this.host = host;
    this.port = NO_PORT;
    this.availability = HostAvailability.AVAILABLE;
    this.role = HostRole.WRITER;
    this.allAliases.add(this.asAlias());
    this.weight = DEFAULT_WEIGHT;
    this.lastUpdateTime = formatter.format(Timestamp.from(Instant.now()).toLocalDateTime());
  }

  public HostSpec(final String host, final int port) {
    this.host = host;
    this.port = port;
    this.availability = HostAvailability.AVAILABLE;
    this.role = HostRole.WRITER;
    this.allAliases.add(this.asAlias());
    this.weight = DEFAULT_WEIGHT;
    this.lastUpdateTime = formatter.format(Timestamp.from(Instant.now()).toLocalDateTime());
  }

  public HostSpec(final String host, final int port, final HostRole role) {
    this.host = host;
    this.port = port;
    this.availability = HostAvailability.AVAILABLE;
    this.role = role;
    this.allAliases.add(this.asAlias());
    this.weight = DEFAULT_WEIGHT;
    this.lastUpdateTime = formatter.format(Timestamp.from(Instant.now()).toLocalDateTime());
  }

  public HostSpec(final String host, final int port, final HostRole role, final HostAvailability availability) {
    this.host = host;
    this.port = port;
    this.availability = availability;
    this.role = role;
    this.allAliases.add(this.asAlias());
    this.weight = DEFAULT_WEIGHT;
    this.lastUpdateTime = formatter.format(Timestamp.from(Instant.now()).toLocalDateTime());
  }

  public HostSpec(final String host, final int port, final HostRole role, final HostAvailability availability,
      final long weight) {
    this.host = host;
    this.port = port;
    this.availability = availability;
    this.role = role;
    this.allAliases.add(this.asAlias());
    this.weight = weight;
    this.lastUpdateTime = formatter.format(Timestamp.from(Instant.now()).toLocalDateTime());
  }

  public HostSpec(final String host, final int port, final HostRole role, final HostAvailability availability,
      final long weight, final String lastUpdateTime) {
    this.host = host;
    this.port = port;
    this.availability = availability;
    this.role = role;
    this.allAliases.add(this.asAlias());
    this.weight = weight;
    this.lastUpdateTime = lastUpdateTime;
  }

  /**
   * Creates a copy of the passed in {@link HostSpec} but with the specified role.
   *
   * @param copyHost the host whose details to copy.
   * @param role     the role of this host (writer or reader).
   */
  public HostSpec(final HostSpec copyHost, final HostRole role) {
    this(copyHost.getHost(), copyHost.getPort(), role, copyHost.getAvailability());
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

  public HostRole getRole() {
    return this.role;
  }

  public HostAvailability getAvailability() {
    return this.availability;
  }

  public void setAvailability(final HostAvailability availability) {
    this.availability = availability;
  }

  public String getLastUpdateTime() {
    return this.lastUpdateTime;
  }

  public Set<String> getAliases() {
    return Collections.unmodifiableSet(this.aliases);
  }

  public long getWeight() {
    return this.weight;
  }

  public void addAlias(final String... alias) {
    if (alias == null || alias.length < 1) {
      return;
    }

    Arrays.asList(alias).forEach(x -> {
      this.aliases.add(x);
      this.allAliases.add(x);
    });
  }

  public void removeAlias(final String... alias) {
    if (alias == null || alias.length < 1) {
      return;
    }
    Arrays.asList(alias).forEach(x -> {
      this.aliases.remove(x);
      this.allAliases.remove(x);
    });
  }

  public void resetAliases() {
    this.aliases.clear();
    this.allAliases.clear();
    this.allAliases.add(this.asAlias());
  }

  public String getUrl() {
    String url = isPortSpecified() ? host + ":" + port : host;
    if (!url.endsWith("/")) {
      url += "/";
    }
    return url;
  }

  public String getHostId() {
    return hostId;
  }

  public void setHostId(String hostId) {
    this.hostId = hostId;
  }

  public String asAlias() {
    return isPortSpecified() ? host + ":" + port : host;
  }

  public Set<String> asAliases() {
    return Collections.unmodifiableSet(this.allAliases);
  }

  public String toString() {
    return String.format("HostSpec[host=%s, port=%d, %s, %s, weight=%d, %s]",
        this.host, this.port, this.role, this.availability, this.weight, this.lastUpdateTime);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.host, this.port, this.availability, this.role, this.weight, this.lastUpdateTime);
  }

  @Override
  public boolean equals(final Object obj) {
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
        && this.weight == spec.weight;
  }
}
