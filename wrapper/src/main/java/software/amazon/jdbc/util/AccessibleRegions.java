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

package software.amazon.jdbc.util;

import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.PropertyDefinition;

/**
 * Helper for parsing the {@link PropertyDefinition#GDB_ACCESSIBLE_REGIONS} configuration parameter.
 *
 * <p>The parameter value is a comma-separated list of AWS region names (e.g. {@code us-east-1,us-west-2}).
 * This class normalizes the value into a {@code Set<String>} of trimmed, lowercased region names, returning
 * {@code null} when the parameter is unset or empty (meaning "no restriction — all regions are accessible").
 *
 * <p>Centralizing this logic keeps parsing rules consistent across all consumers
 * (failover, read/write splitting, topology monitor, initial connection strategy).
 */
public final class AccessibleRegions {

  private AccessibleRegions() {
  }

  /**
   * Parses {@link PropertyDefinition#GDB_ACCESSIBLE_REGIONS} from the given properties.
   *
   * @param properties the connection properties
   * @return a set of normalized region names, or {@code null} when the property is not set or empty
   *     (which means "no restriction — all regions are accessible")
   */
  public static @Nullable Set<String> parse(final Properties properties) {
    final String accessibleRegionsStr = PropertyDefinition.GDB_ACCESSIBLE_REGIONS.getString(properties);
    if (accessibleRegionsStr == null || accessibleRegionsStr.trim().isEmpty()) {
      return null;
    }
    final Set<String> result = Arrays.stream(accessibleRegionsStr.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .map(String::toLowerCase)
        .collect(Collectors.toSet());
    return result.isEmpty() ? null : result;
  }
}
