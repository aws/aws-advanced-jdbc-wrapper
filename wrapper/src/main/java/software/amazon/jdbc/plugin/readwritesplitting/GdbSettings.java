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

package software.amazon.jdbc.plugin.readwritesplitting;

import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.AccessibleRegions;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.RdsUrlType;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.StringUtils;

/**
 * Shared configuration and region logic for Global Database read/write splitting, extracted from
 * the legacy {@code GdbReadWriteSplittingPlugin}. A single instance is shared by the GDB writer
 * resolver, reader candidate source, and initial-connection handler.
 */
public class GdbSettings {

  private static final Logger LOGGER = Logger.getLogger(GdbSettings.class.getName());

  public static final AwsWrapperProperty RW_HOME_REGION =
      new AwsWrapperProperty("gdbRwHomeRegion", null,
          "Specifies the home region for read/write splitting.");

  public static final AwsWrapperProperty RESTRICT_WRITER_TO_HOME_REGION =
      new AwsWrapperProperty("gdbRwRestrictWriterToHomeRegion", "true",
          "Prevents connections to a writer node outside of the defined home region.");

  public static final AwsWrapperProperty RESTRICT_READER_TO_HOME_REGION =
      new AwsWrapperProperty("gdbRwRestrictReaderToHomeRegion", "true",
          "Prevents connections to a reader node outside of the defined home region.");

  public static final AwsWrapperProperty ENABLE_GWF =
      new AwsWrapperProperty("gdbEnableGlobalWriteForwarding", "false",
          "Set to true to enable Global Write Forwarding when connected to"
              + " a reader connection in a secondary global region.");

  static {
    PropertyDefinition.registerPluginProperties(GdbSettings.class);
  }

  private final RdsUtils rdsHelper = new RdsUtils();
  private final boolean restrictWriterToHomeRegion;
  private final boolean restrictReaderToHomeRegion;
  private final boolean enableGwf;

  private boolean isInit = false;
  private @Nullable String homeRegion;
  private @Nullable Set<String> accessibleRegions;

  public GdbSettings(final Properties properties) {
    this.restrictWriterToHomeRegion = RESTRICT_WRITER_TO_HOME_REGION.getBoolean(properties);
    this.restrictReaderToHomeRegion = RESTRICT_READER_TO_HOME_REGION.getBoolean(properties);
    this.enableGwf = ENABLE_GWF.getBoolean(properties);
  }

  /** Resolves and validates the home region and accessible regions (idempotent). */
  public void init(final HostSpec initHostSpec, final Properties props) throws SQLException {
    if (this.isInit) {
      return;
    }
    this.isInit = true;

    this.homeRegion = RW_HOME_REGION.getString(props);
    if (StringUtils.isNullOrEmpty(this.homeRegion)) {
      final RdsUrlType rdsUrlType = this.rdsHelper.identifyRdsType(initHostSpec.getHost());
      if (rdsUrlType != null && rdsUrlType.hasRegion()) {
        this.homeRegion = this.rdsHelper.getRdsRegion(initHostSpec.getHost());
      }
    }

    if (StringUtils.isNullOrEmpty(this.homeRegion)) {
      throw new SQLException(Messages.get(
          "GdbReadWriteSplittingPlugin.missingHomeRegion",
          new Object[] {initHostSpec.getHost()}));
    }

    final String resolvedHomeRegion = this.homeRegion;
    LOGGER.finest(() -> Messages.get(
        "GdbReadWriteSplittingPlugin.parameterValue",
        new Object[] {"gdbRwHomeRegion", resolvedHomeRegion}));

    final Set<String> parsedRegions = AccessibleRegions.parse(props);
    this.accessibleRegions = parsedRegions;
    if (parsedRegions != null) {
      LOGGER.finest(() -> Messages.get(
          "GdbReadWriteSplittingPlugin.parameterValue",
          new Object[] {"gdbAccessibleRegions", parsedRegions}));
    }

    if (this.accessibleRegions != null
        && !this.accessibleRegions.contains(resolvedHomeRegion.toLowerCase(Locale.ROOT))) {
      throw new SQLException(Messages.get(
          "GdbReadWriteSplittingPlugin.homeRegionNotInAccessibleRegions",
          new Object[] {this.homeRegion, this.accessibleRegions}));
    }
  }

  public RdsUtils rdsHelper() {
    return this.rdsHelper;
  }

  public @Nullable String homeRegion() {
    return this.homeRegion;
  }

  public @Nullable Set<String> accessibleRegions() {
    return this.accessibleRegions;
  }

  public boolean restrictWriterToHomeRegion() {
    return this.restrictWriterToHomeRegion;
  }

  public boolean restrictReaderToHomeRegion() {
    return this.restrictReaderToHomeRegion;
  }

  public boolean enableGwf() {
    return this.enableGwf;
  }

  public boolean isInAccessibleRegion(final HostSpec host) {
    if (this.accessibleRegions == null) {
      return true;
    }
    final String region = this.rdsHelper.getRdsRegion(host.getHost());
    return region != null && this.accessibleRegions.contains(region.toLowerCase(Locale.ROOT));
  }

  public boolean isInHomeRegion(final HostSpec host) {
    final String region = this.rdsHelper.getRdsRegion(host.getHost());
    return region != null && this.homeRegion != null && region.equalsIgnoreCase(this.homeRegion);
  }

  /** Snapshot fragment fields shared by GDB helpers. */
  public void addSnapshotState(final List<software.amazon.jdbc.util.Pair<String, Object>> state) {
    state.add(software.amazon.jdbc.util.Pair.create("homeRegion", this.homeRegion));
    state.add(software.amazon.jdbc.util.Pair.create("restrictWriterToHomeRegion", this.restrictWriterToHomeRegion));
    state.add(software.amazon.jdbc.util.Pair.create("restrictReaderToHomeRegion", this.restrictReaderToHomeRegion));
    state.add(software.amazon.jdbc.util.Pair.create("enableGwf", this.enableGwf));
  }
}
