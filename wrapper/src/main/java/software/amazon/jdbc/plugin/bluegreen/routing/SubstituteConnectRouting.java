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

package software.amazon.jdbc.plugin.bluegreen.routing;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.ConnectionPlugin;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.plugin.bluegreen.BlueGreenRole;
import software.amazon.jdbc.plugin.iam.IamAuthConnectionPlugin;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.PropertyUtils;
import software.amazon.jdbc.util.RdsUtils;

/**
 * Open a new connection to a provided substitute host.
 * In case of IAM auth and connecting with IP address, (possible) IAM host(s) should be added to HostSpec aliases.
 */
public class SubstituteConnectRouting extends BaseConnectRouting {

  private static final Logger LOGGER = Logger.getLogger(SubstituteConnectRouting.class.getName());
  protected static final RdsUtils RDS_UTILS = new RdsUtils();

  protected final HostSpec substituteHostSpec;
  protected final List<HostSpec> iamHosts;
  protected final IamSuccessfulConnectFunc iamSuccessfulConnectNotify;

  public SubstituteConnectRouting(@Nullable String hostAndPort, @Nullable BlueGreenRole role,
      @NonNull final HostSpec substituteHostSpec, @Nullable final List<HostSpec> iamHosts,
      @Nullable IamSuccessfulConnectFunc iamSuccessfulConnectNotify) {
    super(hostAndPort, role);
    this.substituteHostSpec = substituteHostSpec;
    this.iamHosts = iamHosts;
    this.iamSuccessfulConnectNotify = iamSuccessfulConnectNotify;
  }

  @Override
  public Connection apply(ConnectionPlugin plugin, HostSpec hostSpec, Properties props, boolean isInitialConnection,
      JdbcCallable<Connection, SQLException> connectFunc, PluginService pluginService) throws SQLException {

    if (RDS_UTILS.isIPv4(this.substituteHostSpec.getHost()) || RDS_UTILS.isIPv6(this.substituteHostSpec.getHost())) {
      boolean iamInUse = pluginService.isPluginInUse(IamAuthConnectionPlugin.class);

      if (iamInUse && (this.iamHosts == null || this.iamHosts.isEmpty())) {
        throw new SQLException(Messages.get("bgd.requireIamHost"));
      }

      if (iamInUse) {
        for (HostSpec iamHost : this.iamHosts) {
          HostSpec reroutedHostSpec = pluginService.getHostSpecBuilder().copyFrom(this.substituteHostSpec)
              .hostId(iamHost.getHost())
              .availability(HostAvailability.AVAILABLE)
              .build();
          reroutedHostSpec.addAlias(iamHost.getHost());

          final Properties rerouteProperties = PropertyUtils.copyProperties(props);
          IamAuthConnectionPlugin.IAM_HOST.set(rerouteProperties, iamHost.getHost());
          if (iamHost.isPortSpecified()) {
            IamAuthConnectionPlugin.IAM_DEFAULT_PORT.set(rerouteProperties, String.valueOf(iamHost.getPort()));
          }

          try {
            Connection conn = pluginService.connect(reroutedHostSpec, rerouteProperties);

            if (this.iamSuccessfulConnectNotify != null) {
              try {
                this.iamSuccessfulConnectNotify.notify(iamHost.getHost());
              } catch (Exception ex) {
                // do nothing
              }
            }

            return conn;

          } catch (SQLException sqlException) {
            if (!pluginService.isLoginException(sqlException, pluginService.getTargetDriverDialect())) {
              throw sqlException;
            }
            // do nothing
            // try with another IAM host
          }
        }

        throw new SQLException(Messages.get("bgd.inProgressCantOpenConnection",
            new Object[] {this.substituteHostSpec.getHostAndPort()}));
      }
    }
    return pluginService.connect(this.substituteHostSpec, props, plugin);
  }

  @Override
  public String toString() {
    return String.format("%s [%s, %s, substitute: %s, iamHosts: %s]",
        this.getClass().getName() + "@" + Integer.toHexString(this.hashCode()),
        this.hostAndPort == null ? "<null>" : this.hostAndPort,
        this.role == null ? "<null>" : this.role.toString(),
        this.substituteHostSpec == null ? "<null>" : this.substituteHostSpec.getHostAndPort(),
        this.iamHosts == null
            ? "<null>"
            : this.iamHosts.stream().map(HostSpec::getHostAndPort).collect(Collectors.joining(", ")));
  }

  public interface IamSuccessfulConnectFunc {
    void notify(String iamHost);
  }
}
