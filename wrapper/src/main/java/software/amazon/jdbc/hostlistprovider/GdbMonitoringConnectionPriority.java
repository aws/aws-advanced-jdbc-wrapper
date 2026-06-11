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

package software.amazon.jdbc.hostlistprovider;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.util.RdsUtils;

/**
 * Represents a monitoring connection priority choice for Global Aurora Databases.
 *
 * <p>Supported values:
 * <ul>
 *   <li>{@code strict-writer-primary} — writer node in the primary region</li>
 *   <li>{@code strict-reader-primary} — reader node in the primary region</li>
 *   <li>{@code strict-reader-secondary} — reader node in any secondary region</li>
 *   <li>{@code strict-writer-<region>} — writer node in the specified region (e.g. strict-writer-us-east-1)</li>
 *   <li>{@code strict-reader-<region>} — reader node in the specified region (e.g. strict-reader-us-west-2)</li>
 *   <li>{@code <region>} — any node in the specified region (e.g. us-east-1)</li>
 * </ul>
 */
public class GdbMonitoringConnectionPriority {

  private static final String STRICT_WRITER_PREFIX = "strict-writer-";
  private static final String STRICT_READER_PREFIX = "strict-reader-";
  private static final String PRIMARY = "primary";
  private static final String SECONDARY = "secondary";

  private final @Nullable HostRole requiredRole; // null means any role
  private final @Nullable String requiredRegion; // null means any region
  private final boolean requirePrimary;   // true if "primary" keyword used
  private final boolean requireSecondary; // true if "secondary" keyword used
  private final String originalValue;

  private GdbMonitoringConnectionPriority(
      final @Nullable HostRole requiredRole,
      final @Nullable String requiredRegion,
      final boolean requirePrimary,
      final boolean requireSecondary,
      final String originalValue) {
    this.requiredRole = requiredRole;
    this.requiredRegion = requiredRegion;
    this.requirePrimary = requirePrimary;
    this.requireSecondary = requireSecondary;
    this.originalValue = originalValue;
  }

  /**
   * Parses a single priority value string.
   *
   * @param value the priority string
   * @return the parsed priority, or null if the value is invalid
   */
  public static @Nullable GdbMonitoringConnectionPriority fromValue(final @Nullable String value) {
    if (value == null || value.trim().isEmpty()) {
      return null;
    }

    final String trimmed = value.trim().toLowerCase(Locale.ROOT);

    if (trimmed.startsWith(STRICT_WRITER_PREFIX)) {
      final String suffix = trimmed.substring(STRICT_WRITER_PREFIX.length());
      if (suffix.isEmpty()) {
        return null;
      }
      if (PRIMARY.equals(suffix)) {
        return new GdbMonitoringConnectionPriority(HostRole.WRITER, null, true, false, trimmed);
      }
      if (SECONDARY.equals(suffix)) {
        // A writer in a secondary region is conceptually impossible for an Aurora Global Database
        // (only the primary region has a writer). Reject rather than treat "secondary" as a region literal.
        return null;
      }
      // strict-writer-<region>
      return new GdbMonitoringConnectionPriority(HostRole.WRITER, suffix, false, false, trimmed);
    }

    if (trimmed.startsWith(STRICT_READER_PREFIX)) {
      final String suffix = trimmed.substring(STRICT_READER_PREFIX.length());
      if (suffix.isEmpty()) {
        return null;
      }
      if (PRIMARY.equals(suffix)) {
        return new GdbMonitoringConnectionPriority(HostRole.READER, null, true, false, trimmed);
      }
      if (SECONDARY.equals(suffix)) {
        return new GdbMonitoringConnectionPriority(HostRole.READER, null, false, true, trimmed);
      }
      // strict-reader-<region>
      return new GdbMonitoringConnectionPriority(HostRole.READER, suffix, false, false, trimmed);
    }

    // Plain region name — any node in that region
    return new GdbMonitoringConnectionPriority(null, trimmed, false, false, trimmed);
  }

  /**
   * Parses a comma-separated list of priority values.
   *
   * @param value the comma-separated priority string
   * @return the ordered list of parsed priorities (invalid values are skipped)
   */
  public static List<GdbMonitoringConnectionPriority> parseList(final @Nullable String value) {
    List<GdbMonitoringConnectionPriority> result = new ArrayList<>();
    if (value == null || value.trim().isEmpty()) {
      // Default: strict-writer-primary
      result.add(new GdbMonitoringConnectionPriority(HostRole.WRITER, null, true, false, "strict-writer-primary"));
      return result;
    }
    for (String item : value.split(",")) {
      GdbMonitoringConnectionPriority priority = fromValue(item);
      if (priority != null) {
        result.add(priority);
      }
    }
    if (result.isEmpty()) {
      result.add(new GdbMonitoringConnectionPriority(HostRole.WRITER, null, true, false, "strict-writer-primary"));
    }
    return result;
  }

  /**
   * Checks if a given host satisfies this priority.
   *
   * @param host the host to check
   * @param primaryRegion the current primary region of the global database (region where the writer is)
   * @param rdsUtils utility to extract region from host
   * @return true if the host satisfies this priority
   */
  public boolean isSatisfiedBy(
      final @NonNull HostSpec host,
      final @Nullable String primaryRegion,
      final @NonNull RdsUtils rdsUtils) {

    // Check role
    if (this.requiredRole != null && host.getRole() != this.requiredRole) {
      return false;
    }

    final String hostRegion = rdsUtils.getRdsRegion(host.getHost());

    // Check primary/secondary
    if (this.requirePrimary) {
      if (primaryRegion == null || hostRegion == null
          || !primaryRegion.equalsIgnoreCase(hostRegion)) {
        return false;
      }
    }
    if (this.requireSecondary) {
      if (primaryRegion == null || hostRegion == null
          || primaryRegion.equalsIgnoreCase(hostRegion)) {
        return false;
      }
    }

    // Check specific region
    if (this.requiredRegion != null) {
      if (hostRegion == null || !this.requiredRegion.equalsIgnoreCase(hostRegion)) {
        return false;
      }
    }

    return true;
  }

  /**
   * Finds the first host from the list that satisfies this priority.
   *
   * @param hosts the available hosts
   * @param primaryRegion the current primary region
   * @param rdsUtils utility to extract region from host
   * @return a matching host, or null if none found
   */
  public @Nullable HostSpec findMatchingHost(
      final @NonNull List<HostSpec> hosts,
      final @Nullable String primaryRegion,
      final @NonNull RdsUtils rdsUtils) {
    return hosts.stream()
        .filter(h -> this.isSatisfiedBy(h, primaryRegion, rdsUtils))
        .findFirst()
        .orElse(null);
  }

  /**
   * Finds all hosts from the list that satisfy this priority.
   *
   * @param hosts the available hosts
   * @param primaryRegion the current primary region
   * @param rdsUtils utility to extract region from host
   * @return list of matching hosts (may be empty)
   */
  public List<HostSpec> findMatchingHosts(
      final @NonNull List<HostSpec> hosts,
      final @Nullable String primaryRegion,
      final @NonNull RdsUtils rdsUtils) {
    return hosts.stream()
        .filter(h -> this.isSatisfiedBy(h, primaryRegion, rdsUtils))
        .collect(java.util.stream.Collectors.toList());
  }

  public @Nullable HostRole getRequiredRole() {
    return this.requiredRole;
  }

  public @Nullable String getRequiredRegion() {
    return this.requiredRegion;
  }

  public boolean isRequirePrimary() {
    return this.requirePrimary;
  }

  public boolean isRequireSecondary() {
    return this.requireSecondary;
  }

  @Override
  public String toString() {
    return this.originalValue;
  }
}
