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

package software.amazon.jdbc.ds.xa;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.ConnectionInfo;
import software.amazon.jdbc.ConnectionProvider;
import software.amazon.jdbc.HighestLoadHostSelector;
import software.amazon.jdbc.HighestWeightHostSelector;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSelector;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.LowestLoadHostSelector;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.RandomHostSelector;
import software.amazon.jdbc.RoundRobinHostSelector;
import software.amazon.jdbc.WeightedRandomHostSelector;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.ds.DataSourceConfigHelper;
import software.amazon.jdbc.exceptions.SQLLoginException;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.PropertyUtils;
import software.amazon.jdbc.util.ResourceLock;
import software.amazon.jdbc.util.StringUtils;

/**
 * A {@link ConnectionProvider} for the XA datasource path. It wraps a configured target
 * {@link XADataSource}, opens a single {@link XAConnection} lazily on first use, and returns a fresh
 * logical {@link Connection} handle over that same physical XA connection on each
 * {@link #connect} call.
 *
 * <p>Because an XA branch is pinned to one physical session and the {@code XAResource} is bound to
 * this {@code XAConnection}, the same {@code XAConnection} is reused for the lifetime of this
 * provider (i.e. of the owning {@link XAConnectionWrapper}); it is never repointed to a different
 * host. The target {@code XADataSource} is expected to be fully configured before it is handed to
 * this provider.
 */
public class XADataSourceConnectionProvider implements ConnectionProvider {

  private static final Map<String, HostSelector> acceptedStrategies =
      Collections.unmodifiableMap(new HashMap<String, HostSelector>() {
        {
          put(HighestLoadHostSelector.STRATEGY_HIGHEST_LOAD, new HighestLoadHostSelector());
          put(HighestLoadHostSelector.STRATEGY_HIGHEST_LOAD_BY_CPU, HighestLoadHostSelector.byCpu());
          put(HighestLoadHostSelector.STRATEGY_HIGHEST_LOAD_BY_LAG, HighestLoadHostSelector.byLag());
          put(HighestWeightHostSelector.STRATEGY_HIGHEST_WEIGHT, new HighestWeightHostSelector());
          put(LowestLoadHostSelector.STRATEGY_LOWEST_LOAD, new LowestLoadHostSelector());
          put(LowestLoadHostSelector.STRATEGY_LOWEST_LOAD_BY_CPU, LowestLoadHostSelector.byCpu());
          put(LowestLoadHostSelector.STRATEGY_LOWEST_LOAD_BY_LAG, LowestLoadHostSelector.byLag());
          put(RandomHostSelector.STRATEGY_RANDOM, new RandomHostSelector());
          put(RoundRobinHostSelector.STRATEGY_ROUND_ROBIN, new RoundRobinHostSelector());
          put(WeightedRandomHostSelector.STRATEGY_WEIGHTED_RANDOM, new WeightedRandomHostSelector());
        }
      });

  private final @NonNull XADataSource xaDataSource;
  private final @NonNull String xaDataSourceClassName;
  private final @Nullable String resolvedUrl;
  private final ResourceLock lock = new ResourceLock();
  private volatile @Nullable XAConnection xaConnection;
  private volatile @Nullable Connection logicalConnection;

  public XADataSourceConnectionProvider(final @NonNull XADataSource xaDataSource) {
    this(xaDataSource, null);
  }

  public XADataSourceConnectionProvider(
      final @NonNull XADataSource xaDataSource, final @Nullable String resolvedUrl) {
    this.xaDataSource = xaDataSource;
    this.xaDataSourceClassName = xaDataSource.getClass().getName();
    this.resolvedUrl = resolvedUrl;
  }

  @Override
  public boolean acceptsUrl(
      final @NonNull String protocol, final @NonNull HostSpec hostSpec, final @NonNull Properties props) {
    return true;
  }

  @Override
  public boolean acceptsStrategy(final @Nullable HostRole role, final @NonNull String strategy) {
    return acceptedStrategies.containsKey(strategy);
  }

  @Override
  public @Nullable HostSpec getHostSpecByStrategy(
      final @NonNull List<HostSpec> hosts,
      final @Nullable HostRole role,
      final @NonNull String strategy,
      final @Nullable Properties props)
      throws SQLException {
    final HostSelector hostSelector = acceptedStrategies.get(strategy);
    if (hostSelector == null) {
      throw new UnsupportedOperationException(
          Messages.get(
              "ConnectionProvider.unsupportedHostSpecSelectorStrategy",
              new Object[] {strategy, XADataSourceConnectionProvider.class}));
    }
    return hostSelector.getHost(hosts, role, props);
  }

  @Override
  public @NonNull ConnectionInfo connect(
      final @NonNull String protocol,
      final @NonNull Dialect dialect,
      final @NonNull TargetDriverDialect targetDriverDialect,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props)
      throws SQLException {

    // Open through the connect pipeline's props so plugin-provided credentials reach the target
    // XADataSource. In particular, the IAM authentication plugin generates a short-lived token and
    // sets it as the password on these per-connect props; without applying them here the target
    // XADataSource would keep the static password configured at getXAConnection() time and IAM
    // authentication would fail.
    final XAConnection xaConn = openXaConnection(props);
    return new ConnectionInfo(getOrCreateLogicalConnection(xaConn), false, xaConn);
  }

  /**
   * Returns the current logical {@link Connection} over the owning XA connection, creating it on
   * first use and reusing it while it is open.
   *
   * <p>A single physical XA connection supports only one live logical connection at a time, and each
   * {@code XAConnection.getConnection()} closes the previously handed-out logical connection. The
   * connect pipeline can call {@link #connect} more than once while establishing a single handle
   * (for example the IAM authentication plugin retries the connect after regenerating its token). If
   * each such call opened a fresh logical connection, the earlier one — which the caller is about to
   * use — would be closed out from under it ("Physical Connection doesn't exist" / "This
   * PooledConnection has already been closed"). Caching the logical connection until it is closed
   * makes repeated connect calls idempotent. A new logical connection is created for the next
   * {@code XAConnectionWrapper.getConnection()} because closing that handle closes this connection.
   */
  private @NonNull Connection getOrCreateLogicalConnection(final @NonNull XAConnection xaConn)
      throws SQLException {
    try (ResourceLock ignored = this.lock.obtain()) {
      final Connection existing = this.logicalConnection;
      if (existing != null && !existing.isClosed()) {
        return existing;
      }
      final Connection conn = xaConn.getConnection();
      if (conn == null) {
        throw new SQLLoginException(Messages.get("XADataSourceConnectionProvider.noConnection"));
      }
      this.logicalConnection = conn;
      return conn;
    }
  }

  /**
   * Returns the single owning {@link XAConnection}, opening it on first use. Reused for the lifetime
   * of this provider so the {@code XAResource} and every logical handle share one physical session.
   *
   * @return the owning XA connection.
   * @throws SQLException if the XA connection cannot be opened.
   */
  public @NonNull XAConnection getOrOpenXaConnection() throws SQLException {
    return openXaConnection(null);
  }

  /**
   * Opens (once) and returns the owning {@link XAConnection}. When {@code props} is non-null, the
   * user/password from those per-connect properties are applied to the target XADataSource before it
   * is opened, so plugin-provided credentials (e.g. an IAM token) take effect. The physical session
   * is pinned for the lifetime of this provider, so credentials are only applied on the first open.
   */
  private @NonNull XAConnection openXaConnection(final @Nullable Properties props) throws SQLException {
    XAConnection xaConn = this.xaConnection;
    if (xaConn == null) {
      try (ResourceLock ignored = this.lock.obtain()) {
        xaConn = this.xaConnection;
        if (xaConn == null) {
          if (props != null) {
            applyCredentials(props);
            applyCleanUrl();
          }
          xaConn = this.xaDataSource.getXAConnection();
          if (xaConn == null) {
            throw new SQLLoginException(Messages.get("XADataSourceConnectionProvider.noConnection"));
          }
          this.xaConnection = xaConn;
        }
      }
    }
    return xaConn;
  }

  /**
   * Applies the effective user/password from the per-connect properties to the target XADataSource
   * via its bean setters. These may have been rewritten by the connect pipeline (for example the IAM
   * plugin replaces the password with a generated authentication token).
   */
  private void applyCredentials(final @NonNull Properties props) {
    final List<Method> methods = Arrays.asList(this.xaDataSource.getClass().getMethods());
    final String user = PropertyDefinition.USER.getString(props);
    if (user != null) {
      PropertyUtils.setPropertyOnTarget(this.xaDataSource, PropertyDefinition.USER.name, user, methods);
    }
    final String password = PropertyDefinition.PASSWORD.getString(props);
    if (password != null) {
      PropertyUtils.setPropertyOnTarget(this.xaDataSource, PropertyDefinition.PASSWORD.name, password, methods);
    }
  }

  /**
   * Re-applies the target URL with AWS Wrapper-specific query parameters removed. This runs at
   * connect time, when the configured plugin classes are loaded and every wrapper property name is
   * registered, so the cleaning is complete (the best-effort cleaning done earlier at
   * {@code getXAConnection()} time may miss plugin-specific properties whose classes were not yet
   * loaded).
   */
  private void applyCleanUrl() {
    if (StringUtils.isNullOrEmpty(this.resolvedUrl)) {
      return;
    }
    final String cleanUrl = DataSourceConfigHelper.removeWrapperPropertiesFromUrl(this.resolvedUrl);
    if (StringUtils.isNullOrEmpty(cleanUrl)) {
      return;
    }
    final List<Method> methods = Arrays.asList(this.xaDataSource.getClass().getMethods());
    PropertyUtils.setPropertyOnTarget(this.xaDataSource, "url", cleanUrl, methods);
  }

  /**
   * Returns the owning {@link XAConnection} if it has been opened, otherwise null.
   *
   * @return the owning XA connection, or null if not yet opened.
   */
  public @Nullable XAConnection getXaConnectionOrNull() {
    return this.xaConnection;
  }

  @Override
  public String getTargetName() {
    return this.xaDataSourceClassName;
  }

  @Override
  public List<Pair<String, Object>> getSnapshotState() {
    final List<Pair<String, Object>> state = new ArrayList<>();
    state.add(Pair.create("xaDataSourceClassName", this.xaDataSourceClassName));
    return state;
  }
}
