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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.RdsUrlType;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.StringUtils;

public class GdbReadWriteSplittingPlugin extends ReadWriteSplittingPlugin {

  private static final Logger LOGGER = Logger.getLogger(GdbReadWriteSplittingPlugin.class.getName());

  public static final AwsWrapperProperty RW_HOME_REGION =
      new AwsWrapperProperty(
          "gdbRwHomeRegion",
          null,
          "Specifies the home region for read/write splitting.");

  public static final AwsWrapperProperty RESTRICT_WRITER_TO_HOME_REGION =
      new AwsWrapperProperty(
          "gdbRwRestrictWriterToHomeRegion",
          "false",
          "Prevents connections to a writer node outside of the defined home region.");

  public static final AwsWrapperProperty RESTRICT_READER_TO_HOME_REGION =
      new AwsWrapperProperty(
          "gdbRwRestrictReaderToHomeRegion",
          "false",
          "Prevents connections to a reader node outside of the defined home region.");

  protected boolean isInit = false;
  protected final RdsUtils rdsHelper = new RdsUtils();
  protected String homeRegion;
  protected final boolean restrictWriterToHomeRegion;
  protected final boolean restrictReaderToHomeRegion;

  static {
    PropertyDefinition.registerPluginProperties(GdbReadWriteSplittingPlugin.class);
  }

  public GdbReadWriteSplittingPlugin(final PluginService pluginService, final @NonNull Properties properties) {
    super(pluginService, properties);
    this.restrictWriterToHomeRegion = RESTRICT_WRITER_TO_HOME_REGION.getBoolean(properties);
    this.restrictReaderToHomeRegion = RESTRICT_READER_TO_HOME_REGION.getBoolean(properties);
  }

  protected void initSettings(final HostSpec initHostSpec, Properties props) throws SQLException {
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

    LOGGER.finest(() -> Messages.get(
        "GdbReadWriteSplittingPlugin.parameterValue",
        new Object[] {"gdbRwHomeRegion", this.homeRegion}));
  }

  @Override
  public Connection connect(
      final String driverProtocol,
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection,
      final @NonNull JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {
    this.initSettings(hostSpec, props);
    return super.connect(driverProtocol, hostSpec, props, isInitialConnection, connectFunc);
  }

  @Override
  protected void initializeWriterConnection() throws SQLException {
    if (this.restrictWriterToHomeRegion
        && this.writerHostSpec != null
        && !this.homeRegion.equalsIgnoreCase(this.rdsHelper.getRdsRegion(this.writerHostSpec.getHost()))) {
      throw new ReadWriteSplittingSQLException(Messages.get(
          "GdbReadWriteSplittingPlugin.cantConnectWriterOutOfHomeRegion",
          new Object[] {this.writerHostSpec.getHost(), this.homeRegion}));
    }
    super.initializeWriterConnection();
  }

  @Override
  protected void setWriterConnection(final Connection conn, final HostSpec host) throws SQLException {
    if (this.restrictWriterToHomeRegion
        && this.writerHostSpec != null
        && !this.homeRegion.equalsIgnoreCase(this.rdsHelper.getRdsRegion(this.writerHostSpec.getHost()))) {
      throw new ReadWriteSplittingSQLException(Messages.get(
          "GdbReadWriteSplittingPlugin.cantConnectWriterOutOfHomeRegion",
          new Object[] {this.writerHostSpec.getHost(), this.homeRegion}));
    }
    super.setWriterConnection(conn, host);
  }

  @Override
  protected List<HostSpec> getReaderHostCandidates() throws SQLException {
    if (this.restrictReaderToHomeRegion) {
      final List<HostSpec> hostsInRegion = this.pluginService.getHosts().stream()
          .filter(x -> this.rdsHelper.getRdsRegion(x.getHost())
              .equalsIgnoreCase(this.homeRegion))
          .collect(Collectors.toList());

      if (hostsInRegion.isEmpty()) {
        throw new ReadWriteSplittingSQLException(
            Messages.get(
                "GdbReadWriteSplittingPlugin.noAvailableReadersInHomeRegion",
                new Object[]{this.homeRegion}));
      }
      return hostsInRegion;
    }
    return super.getReaderHostCandidates();
  }
}
