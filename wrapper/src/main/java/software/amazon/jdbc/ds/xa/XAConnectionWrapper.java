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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Logger;
import javax.sql.ConnectionEventListener;
import javax.sql.StatementEventListener;
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.wrapper.ConnectionWrapper;

/**
 * A {@link XAConnection} that exposes the wrapper's plugin pipeline to the application while
 * delegating the XA control surface to the target driver's {@code XAConnection}.
 *
 * <p>Per the JDBC {@code PooledConnection} contract, each {@link #getConnection()} returns a new
 * logical connection over the same physical XA connection, and the previous logical connection is
 * closed. In this implementation each logical handle is a fresh {@link ConnectionWrapper} with its
 * own plugin pipeline (built by {@code logicalConnectionBuilder}), so no plugin state leaks between
 * successive checkouts. The physical XA connection is owned by the
 * {@link XADataSourceConnectionProvider} and reused across handles.
 */
public class XAConnectionWrapper implements XAConnection {

  private static final Logger LOGGER = Logger.getLogger(XAConnectionWrapper.class.getName());

  /** Builds a fresh logical connection (a {@link ConnectionWrapper} with a fresh pipeline). */
  @FunctionalInterface
  public interface LogicalConnectionBuilder {
    ConnectionWrapper build() throws SQLException;
  }

  private final XADataSourceConnectionProvider provider;
  private final LogicalConnectionBuilder logicalConnectionBuilder;
  private volatile @Nullable ConnectionWrapper currentHandle;

  public XAConnectionWrapper(
      final XADataSourceConnectionProvider provider,
      final LogicalConnectionBuilder logicalConnectionBuilder) {
    this.provider = provider;
    this.logicalConnectionBuilder = logicalConnectionBuilder;
  }

  @Override
  public Connection getConnection() throws SQLException {
    // Per the PooledConnection contract, only one logical connection may be open at a time; close
    // the previous handle before returning a new one.
    closeCurrentHandleQuietly();
    final ConnectionWrapper handle = this.logicalConnectionBuilder.build();
    this.currentHandle = handle;
    return handle;
  }

  @Override
  public XAResource getXAResource() throws SQLException {
    final XAConnection targetXaConnection = this.provider.getOrOpenXaConnection();
    return new XAResourceWrapper(
        targetXaConnection.getXAResource(),
        active -> {
          final ConnectionWrapper handle = this.currentHandle;
          if (handle != null) {
            handle.getServicesContainer().getPluginService().setXaTransactionActive(active);
          }
        });
  }

  @Override
  public void close() throws SQLException {
    closeCurrentHandleQuietly();
    final XAConnection targetXaConnection = this.provider.getXaConnectionOrNull();
    if (targetXaConnection != null) {
      targetXaConnection.close();
    }
  }

  private void closeCurrentHandleQuietly() {
    final ConnectionWrapper handle = this.currentHandle;
    this.currentHandle = null;
    if (handle == null) {
      return;
    }
    try {
      if (!handle.isClosed()) {
        handle.close();
      }
    } catch (final SQLException e) {
      LOGGER.finest(() -> "Ignoring error while closing the previous logical XA connection handle: "
          + e.getMessage());
    }
  }

  @Override
  public void addConnectionEventListener(final ConnectionEventListener listener) {
    delegateListenerOp(xaConn -> xaConn.addConnectionEventListener(listener));
  }

  @Override
  public void removeConnectionEventListener(final ConnectionEventListener listener) {
    delegateListenerOp(xaConn -> xaConn.removeConnectionEventListener(listener));
  }

  @Override
  public void addStatementEventListener(final StatementEventListener listener) {
    delegateListenerOp(xaConn -> xaConn.addStatementEventListener(listener));
  }

  @Override
  public void removeStatementEventListener(final StatementEventListener listener) {
    delegateListenerOp(xaConn -> xaConn.removeStatementEventListener(listener));
  }

  @FunctionalInterface
  private interface ListenerOp {
    void apply(XAConnection xaConn) throws SQLException;
  }

  private void delegateListenerOp(final ListenerOp op) {
    try {
      op.apply(this.provider.getOrOpenXaConnection());
    } catch (final SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
