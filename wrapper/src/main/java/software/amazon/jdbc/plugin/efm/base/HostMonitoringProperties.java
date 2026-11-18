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

package software.amazon.jdbc.plugin.efm.base;

import java.util.Properties;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.PropertyUtils;

/**
 * Builds the properties of a host monitor's own connection, shared by all host monitoring plugin versions.
 */
public final class HostMonitoringProperties {

  /** Prefix for properties that apply to the monitoring connection only, e.g. {@code monitoring-socketTimeout}. */
  public static final String MONITORING_PROPERTY_PREFIX = "monitoring-";

  private HostMonitoringProperties() {
  }

  /**
   * Returns the monitoring connection properties for a connection: a copy of its properties without host-selection
   * restrictions, with every {@code monitoring-} prefixed property applied under its unprefixed name and the prefixed
   * name itself removed.
   *
   * @param connectionProps     the properties of the connection that creates the monitor.
   * @param targetDriverDialect the target driver dialect, used to find its host-selection properties.
   * @return a new properties object; {@code connectionProps} is not modified.
   */
  public static Properties create(
      final @NonNull Properties connectionProps, final @Nullable TargetDriverDialect targetDriverDialect) {
    final Properties monitoringProps = PropertyUtils.copyProperties(connectionProps);

    // Drop inherited restrictions before the monitoring overrides are applied, so an explicit
    // 'monitoring-' value below still takes effect. A monitor watches one specific node, which may be
    // a reader, so a target driver setting that rejects read-only servers would stop it connecting.
    PropertyUtils.removeHostSelectionProperties(monitoringProps, targetDriverDialect);

    for (final String name : connectionProps.stringPropertyNames()) {
      if (!name.startsWith(MONITORING_PROPERTY_PREFIX)) {
        continue;
      }
      final String value = connectionProps.getProperty(name);
      if (value != null) {
        monitoringProps.put(name.substring(MONITORING_PROPERTY_PREFIX.length()), value);
      }
      monitoringProps.remove(name);
    }
    return monitoringProps;
  }
}
