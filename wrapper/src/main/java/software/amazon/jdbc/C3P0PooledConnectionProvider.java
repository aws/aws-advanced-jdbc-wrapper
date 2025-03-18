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

import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.cleanup.CanReleaseResources;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.targetdriverdialect.ConnectInfo;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.PropertyUtils;
import software.amazon.jdbc.util.SlidingExpirationCache;
import software.amazon.jdbc.wrapper.HighestWeightHostSelector;

public class C3P0PooledConnectionProvider implements PooledConnectionProvider, CanReleaseResources {

  private static final Logger LOGGER = Logger.getLogger(C3P0PooledConnectionProvider.class.getName());

  protected static final SlidingExpirationCache<String, ComboPooledDataSource> databasePools =
      new SlidingExpirationCache<>(null, ComboPooledDataSource::close);

  protected static final Map<String, HostSelector> acceptedStrategies =
      Collections.unmodifiableMap(new HashMap<String, HostSelector>() {
        {
          put(HighestWeightHostSelector.STRATEGY_HIGHEST_WEIGHT, new HighestWeightHostSelector());
          put(RandomHostSelector.STRATEGY_RANDOM, new RandomHostSelector());
          put(RoundRobinHostSelector.STRATEGY_ROUND_ROBIN, new RoundRobinHostSelector());
        }
      });
  protected static final long poolExpirationCheckNanos = TimeUnit.MINUTES.toNanos(30);

  @Override
  public boolean acceptsUrl(@NonNull String protocol, @NonNull HostSpec hostSpec, @NonNull Properties props) {
    return true;
  }

  @Override
  public boolean acceptsStrategy(@NonNull HostRole role, @NonNull String strategy) {
    return acceptedStrategies.containsKey(strategy);
  }

  @Override
  public HostSpec getHostSpecByStrategy(@NonNull List<HostSpec> hosts, @NonNull HostRole role, @NonNull String strategy,
      @Nullable Properties props) throws SQLException, UnsupportedOperationException {
    if (!acceptsStrategy(role, strategy)) {
      throw new UnsupportedOperationException(
          Messages.get(
              "ConnectionProvider.unsupportedHostSpecSelectorStrategy",
              new Object[] {strategy, C3P0PooledConnectionProvider.class}));
    }

    return acceptedStrategies.get(strategy).getHost(hosts, role, props);
  }

  @Override
  public Connection connect(@NonNull String protocol, @NonNull Dialect dialect,
      @NonNull TargetDriverDialect targetDriverDialect, @NonNull HostSpec hostSpec,
      @NonNull Properties props) throws SQLException {
    final Properties copy = PropertyUtils.copyProperties(props);
    dialect.prepareConnectProperties(copy, protocol, hostSpec);

    final ComboPooledDataSource ds = databasePools.computeIfAbsent(
        hostSpec.getUrl(),
        (key) -> createDataSource(protocol, hostSpec, copy, targetDriverDialect),
        poolExpirationCheckNanos
    );

    ds.setPassword(copy.getProperty(PropertyDefinition.PASSWORD.name));

    return ds.getConnection();
  }

  protected ComboPooledDataSource createDataSource(
      @NonNull String protocol,
      @NonNull HostSpec hostSpec,
      @NonNull Properties props,
      TargetDriverDialect driverDialect) {
    ConnectInfo connectInfo;
    try {
      connectInfo = driverDialect.prepareConnectInfo(protocol, hostSpec, props);
    } catch (SQLException ex) {
      throw new RuntimeException(ex);
    }

    final StringBuilder urlBuilder = new StringBuilder(connectInfo.url);

    final StringJoiner propsJoiner = new StringJoiner("&");
    connectInfo.props.forEach((k, v) -> {
      if (!PropertyDefinition.PASSWORD.name.equals(k) && !PropertyDefinition.USER.name.equals(k)) {
        propsJoiner.add(k + "=" + v);
      }
    });

    urlBuilder.append(connectInfo.url.contains("?") ? "&" : "?").append(propsJoiner);

    ComboPooledDataSource ds = new ComboPooledDataSource();
    ds.setJdbcUrl(urlBuilder.toString());

    final String user = connectInfo.props.getProperty(PropertyDefinition.USER.name);
    final String password = connectInfo.props.getProperty(PropertyDefinition.PASSWORD.name);
    if (user != null) {
      ds.setUser(user);
    }

    if (password != null) {
      ds.setPassword(password);
    }

    return ds;
  }

  @Override
  public String getTargetName() {
    return C3P0PooledConnectionProvider.class.getName();
  }

  @Override
  public void releaseResources() {
    databasePools.getEntries().forEach((key, pool) -> pool.close());
    databasePools.clear();
  }
}
