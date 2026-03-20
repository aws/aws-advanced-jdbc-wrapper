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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.hostlistprovider.HostListProvider;

public class HostIdCacheServiceImpl implements HostIdCacheService {

  public static final String PROP_ENABLED = "aws.jdbc.config.host.cache.enabled";
  public static final String PROP_REGEXP  = "aws.jdbc.config.hast.cache.regexp";

  private static final ConcurrentHashMap<String, Pair<String, String>> cache = new ConcurrentHashMap<>();
  private static final boolean isEnabled = Boolean.parseBoolean(System.getProperty(PROP_ENABLED, "true"));
  private static final String hostRegexp = System.getProperty(PROP_REGEXP, ".*");
  private static final RdsUtils rdsHelper = new RdsUtils();

  @Override
  public @Nullable HostSpec identifyConnection(
      final @NonNull Connection connection,
      final @NonNull HostSpec connectionHostSpec,
      final @NonNull PluginService pluginService) throws SQLException {

    final RdsUrlType urlType = rdsHelper.identifyRdsType(connectionHostSpec.getHost());
    switch (urlType) {
      case RDS_INSTANCE:
        return connectionHostSpec;
      case IP_ADDRESS:
      case OTHER:
        if (isEnabled && connectionHostSpec.getHost().matches(hostRegexp)) {
          return this.getCachedHostSpec(connection, connectionHostSpec, pluginService);
        }
        return pluginService.identifyConnection(connection);
      default:
        // Other hosts are dynamic and may change any time so they can't be cached.
        return pluginService.identifyConnection(connection);
    }
  }

  protected @Nullable HostSpec getCachedHostSpec(
      final @NonNull Connection connection,
      final @NonNull HostSpec connectionHostSpec,
      final @NonNull PluginService pluginService) throws SQLException {

    Pair<String, String> pairIdAndName = cache.get(connectionHostSpec.getHost());
    if (pairIdAndName == null) {
      try {
        pairIdAndName = pluginService.getDialect().getHostId(connection);
        cache.put(connectionHostSpec.getHost(), (pairIdAndName == null ? Pair.create(null, null) : pairIdAndName));
      } catch (final SQLException e) {
        throw new SQLException(Messages.get("PluginServiceImpl.errorIdentifyConnection"), e);
      }
    }

    if (pairIdAndName == null) {
      return null;
    }

    final String instanceId = pairIdAndName.getValue1();
    final String instanceName = pairIdAndName.getValue2();
    if (instanceId == null && instanceName == null) {
      // We've already tried to identify connection, but we got nothing.
      return null;
    }

    List<HostSpec> topology;
    try {
      topology = pluginService.getAllHosts();
      if (topology == null) {
        topology = pluginService.getHostListProvider().forceRefresh();
        if (topology == null) {
          return null;
        }
      }
    } catch (final SQLException | TimeoutException e) {
      throw new SQLException(Messages.get("PluginServiceImpl.errorIdentifyConnection"), e);
    }

    return topology.stream()
        .filter(host -> Objects.equals(instanceId, host.getHostId())
            || Objects.equals(instanceName, host.getHost()))
        .findAny()
        .orElse(null);
  }
}
