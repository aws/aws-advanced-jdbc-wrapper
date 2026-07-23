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

package software.amazon.jdbc.plugin.readwritesplitting.handler;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.plugin.readwritesplitting.RwSplitContext;
import software.amazon.jdbc.plugin.readwritesplitting.cache.NanoTimeSource;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.WrapperUtils;

/**
 * Retry-based role verification for endpoint (Simple) read/write splitting, ported from the legacy
 * {@code SimpleReadWriteSplittingPlugin.getVerifiedConnection}. Repeatedly opens a connection and
 * checks its {@link HostRole} until the deadline; a login exception aborts immediately. The clock
 * is injectable via {@link NanoTimeSource} for deterministic unit tests.
 */
public class EndpointConnectionVerifier {

  private static final Logger LOGGER = Logger.getLogger(EndpointConnectionVerifier.class.getName());

  private final long connectRetryTimeoutMs;
  private final int connectRetryIntervalMs;
  private final NanoTimeSource timeSource;

  public EndpointConnectionVerifier(
      final long connectRetryTimeoutMs,
      final int connectRetryIntervalMs,
      final NanoTimeSource timeSource) {
    this.connectRetryTimeoutMs = connectRetryTimeoutMs;
    this.connectRetryIntervalMs = connectRetryIntervalMs;
    this.timeSource = timeSource;
  }

  /**
   * Opens a verified connection to {@code hostSpec} (or via {@code connectFunc} when non-null),
   * retrying until the timeout. Returns {@code null} if verification could not be completed.
   */
  public @Nullable Connection getVerifiedConnection(
      final RwSplitContext ctx,
      final Properties props,
      final @Nullable HostSpec hostSpec,
      final HostRole hostRole,
      final @Nullable JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {

    final long endTimeNano = this.timeSource.nanoTime()
        + TimeUnit.MILLISECONDS.toNanos(this.connectRetryTimeoutMs);

    while (this.timeSource.nanoTime() < endTimeNano) {
      Connection candidateConn = null;
      try {
        if (connectFunc != null) {
          candidateConn = connectFunc.call();
        } else if (hostSpec != null) {
          candidateConn = ctx.connect(hostSpec, props);
        } else {
          break;
        }

        if (candidateConn == null
            || ctx.pluginService().getHostRole(candidateConn) != hostRole) {
          this.closeConnection(candidateConn);
          this.delay();
          continue;
        }
        return candidateConn;
      } catch (final SQLException ex) {
        this.closeConnection(candidateConn);
        if (ctx.pluginService().isLoginException(ex, ctx.pluginService().getTargetDriverDialect())) {
          throw WrapperUtils.wrapExceptionIfNeeded(SQLException.class, ex);
        }
        this.delay();
      } catch (final Throwable ex) {
        this.closeConnection(candidateConn);
        throw ex;
      }
    }

    LOGGER.fine(() -> Messages.get("SimpleReadWriteSplittingPlugin.verificationFailed",
        new Object[] {hostRole, this.connectRetryTimeoutMs}));
    return null;
  }

  private void closeConnection(final @Nullable Connection connection) {
    if (connection != null) {
      try {
        connection.close();
      } catch (final SQLException ex) {
        // ignore
      }
    }
  }

  private void delay() {
    try {
      TimeUnit.MILLISECONDS.sleep(this.connectRetryIntervalMs);
    } catch (final InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
  }
}
