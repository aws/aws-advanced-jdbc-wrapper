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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.SqlState;
import software.amazon.jdbc.util.Utils;

public class QueryLoadBalancingReadWriteSplittingPlugin extends ReadWriteSplittingPlugin {

  private static final Logger LOGGER = Logger.getLogger(QueryLoadBalancingReadWriteSplittingPlugin.class.getName());

  private static final Set<String> subscribedMethods =
      Collections.unmodifiableSet(new HashSet<String>() {
        {
          add(JdbcMethod.INITHOSTPROVIDER.methodName);
          add(JdbcMethod.CONNECT.methodName);
          add(JdbcMethod.NOTIFYCONNECTIONCHANGED.methodName);
          add(JdbcMethod.CONNECTION_SETREADONLY.methodName);
          add(JdbcMethod.CONNECTION_CLEARWARNINGS.methodName);
          add(JdbcMethod.STATEMENT_EXECUTE.methodName);
          add(JdbcMethod.STATEMENT_EXECUTEQUERY.methodName);
          add(JdbcMethod.STATEMENT_EXECUTEBATCH.methodName);
          add(JdbcMethod.STATEMENT_EXECUTEUPDATE.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_EXECUTE.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_EXECUTEUPDATE.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_EXECUTELARGEUPDATE.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_EXECUTEQUERY.methodName);
          add(JdbcMethod.PREPAREDSTATEMENT_EXECUTEBATCH.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_EXECUTE.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_EXECUTEQUERY.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_EXECUTELARGEUPDATE.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_EXECUTEBATCH.methodName);
          add(JdbcMethod.CALLABLESTATEMENT_EXECUTEUPDATE.methodName);
          add(JdbcMethod.CONNECTION_SETAUTOCOMMIT.methodName);

          // Extra methods to handle
          add(JdbcMethod.CONNECTION_CREATESTATEMENT.methodName);
          add(JdbcMethod.CONNECTION_PREPARESTATEMENT.methodName);
          add(JdbcMethod.CONNECTION_PREPARECALL.methodName);
        }
      });

  public static final AwsWrapperProperty READER_HOST_SELECTOR_STRATEGY =
      new AwsWrapperProperty(
          "includeWriterToLoadBalancing",
          "false",
          "Set to true to allow writer node to be included in query load balancing.");

  private final boolean includeWriterToLoadBalancing;

  static {
    PropertyDefinition.registerPluginProperties(QueryLoadBalancingReadWriteSplittingPlugin.class);
  }

  public QueryLoadBalancingReadWriteSplittingPlugin(final PluginService pluginService, final @NonNull Properties properties) {
    super(pluginService, properties);
    this.includeWriterToLoadBalancing = READER_HOST_SELECTOR_STRATEGY.getBoolean(properties);
  }

  @Override
  public Set<String> getSubscribedMethods() {
    return subscribedMethods;
  }

  @Override
  public void switchConnectionIfRequired(
      final Object methodInvokeOn,
      final String methodName,
      final Object[] args) throws SQLException {
    final Connection currentConnection = this.pluginService.getCurrentConnection();
    final HostSpec currentHostSpec = this.pluginService.getCurrentHostSpec();
    if (currentConnection == null || currentConnection.isClosed()) {
      // do nothing; let normal workflow to handle it
      return;
    }

    // Check if it's safe to switch to a new reader connection
    if (this.inReadWriteSplit
        && !pluginService.isInTransaction()
        && (JdbcMethod.CONNECTION_CREATESTATEMENT.methodName.equals(methodName)
            || JdbcMethod.CONNECTION_PREPARESTATEMENT.methodName.equals(methodName)
            || JdbcMethod.CONNECTION_PREPARECALL.methodName.equals(methodName))
        ) {
      this.refreshAndStoreTopology(currentConnection);

      try {
        this.forceCloseReaderConnection();
        this.initializeReaderConnection();
        this.switchToReaderConnection();

      } catch (final SQLException e) {
        if (!isConnectionUsable(currentConnection)) {
          logAndThrowException(
              Messages.get("ReadWriteSplittingPlugin.errorSwitchingToReader", new Object[] {e.getMessage()}),
              e);
          return;
        }

        // Failed to switch to a reader. The current connection will be used as a fallback.
        LOGGER.fine(() -> Messages.get(
            "ReadWriteSplittingPlugin.fallbackToCurrentConnection",
            new Object[] {
                currentHostSpec.getHostAndPort(),
                e.getMessage()}));
        this.setReaderConnection(currentConnection, currentHostSpec);
      }
    }
  }

  protected void forceCloseReaderConnection() {
    if (this.readerCacheItem == null) {
      return;
    }

    final Connection readerConnection = this.readerCacheItem.get(true);

    if (readerConnection != null) {
      try {
        if (!readerConnection.isClosed()) {
          // readerConnection is open but is not currently in use, so we close it.
          readerConnection.close();
        }
      } catch (SQLException e) {
        // Do nothing.
      }
      this.readerCacheItem = null;
    }
  }

  @Override
  protected HostSpec getWriterHost(final @NonNull List<HostSpec> hosts) throws SQLException {
    HostSpec writerHost = Utils.getWriter(hosts);
    if (writerHost == null) {
      if (hosts.isEmpty()) {
        logAndThrowException(Messages.get("ReadWriteSplittingPlugin.noWriterFound"));
      }
      writerHost = hosts.get(0);
    }

    return writerHost;
  }

  @Override
  protected @Nullable HostRole getReaderCandidateRole() {
    return this.includeWriterToLoadBalancing ? null : HostRole.READER;
  }
}
