/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class HostSpec {

  public static final int NO_PORT = -1;

  protected final String host;
  protected final int port;
  protected HostAvailability availability;
  protected HostRole role;
  protected Set<String> aliases = new HashSet<>();

  public HostSpec(String host) {
    this.host = host;
    this.port = NO_PORT;
    this.availability = HostAvailability.AVAILABLE;
    this.role = HostRole.WRITER;
  }

  public HostSpec(String host, int port) {
    this.host = host;
    this.port = port;
    this.availability = HostAvailability.AVAILABLE;
    this.role = HostRole.WRITER;
  }

  public HostSpec(String host, int port, HostRole role) {
    this.host = host;
    this.port = port;
    this.availability = HostAvailability.AVAILABLE;
    this.role = role;
  }

  public HostSpec(String host, int port, HostRole role, HostAvailability availability) {
    this.host = host;
    this.port = port;
    this.availability = availability;
    this.role = role;
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

  public void setAvailability(HostAvailability availability) {
    this.availability = availability;
  }

  public Set<String> getAliases() {
    return Collections.unmodifiableSet(this.aliases);
  }

  public void addAlias(String... alias) {
    if (alias == null || alias.length < 1) {
      return;
    }

    this.aliases.addAll(Arrays.asList(alias));
  }

  public void removeAlias(String... alias) {
    if (alias == null || alias.length < 1) {
      return;
    }
    Arrays.asList(alias).forEach(this.aliases::remove);
  }

  public String getUrl() {
    String url = isPortSpecified() ? host + ":" + port : host;
    if (!url.endsWith("/")) {
      url += "/";
    }
    return url;
  }

  public String asAlias() {
    return isPortSpecified() ? host + ":" + port : host;
  }

  public Set<String> asAliases() {
    Set<String> allAliases = new HashSet<>();
    allAliases.add(this.asAlias());
    allAliases.addAll(this.aliases);
    return Collections.unmodifiableSet(allAliases);
  }

  public String toString() {
    return String.format("HostSpec[host=%s, port=%d]", this.host, this.port);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.host, this.port, this.availability, this.role);
  }

  @Override
  public boolean equals(Object obj) {
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
        && this.role == spec.role;
  }
}
