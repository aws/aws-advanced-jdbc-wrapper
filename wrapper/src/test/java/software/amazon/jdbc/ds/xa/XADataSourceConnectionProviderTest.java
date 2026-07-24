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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.ConnectionInfo;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;

/** Unit tests for {@link XADataSourceConnectionProvider}. */
public class XADataSourceConnectionProviderTest {

  private AutoCloseable closeable;

  @Mock private XADataSource xaDataSource;
  @Mock private XAConnection xaConnection;
  @Mock private Dialect dialect;
  @Mock private TargetDriverDialect targetDriverDialect;
  @Mock private HostSpec hostSpec;

  private final Properties props = new Properties();

  @BeforeEach
  void setUp() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);
    when(xaDataSource.getXAConnection()).thenReturn(xaConnection);
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  @Test
  void connect_isIdempotent_reusesLogicalConnectionWhileOpen() throws SQLException {
    // Repeated connect() calls while establishing one handle (e.g. the IAM plugin retrying after
    // regenerating its token) must reuse the same logical connection. Opening a fresh one each time
    // would close the previous one out from under the caller.
    final Connection logical1 = mock(Connection.class);
    when(logical1.isClosed()).thenReturn(false);
    final Connection logical2 = mock(Connection.class);
    when(xaConnection.getConnection()).thenReturn(logical1, logical2);

    final XADataSourceConnectionProvider provider = new XADataSourceConnectionProvider(xaDataSource);

    final ConnectionInfo info1 = provider.connect("jdbc:postgresql://", dialect, targetDriverDialect, hostSpec, props);
    final ConnectionInfo info2 = provider.connect("jdbc:postgresql://", dialect, targetDriverDialect, hostSpec, props);

    // The physical XA connection is opened once, and the logical connection is created once and reused.
    verify(xaDataSource, times(1)).getXAConnection();
    verify(xaConnection, times(1)).getConnection();
    assertSame(logical1, info1.getConnection());
    assertSame(logical1, info2.getConnection());
    assertSame(xaConnection, info1.getXaConnection());
  }

  @Test
  void connect_createsFreshLogicalConnection_afterPreviousClosed() throws SQLException {
    // Once the current logical connection is closed (the owning handle was closed), the next connect
    // creates a fresh logical connection over the same physical XA connection.
    final Connection logical1 = mock(Connection.class);
    when(logical1.isClosed()).thenReturn(true); // simulate the previous handle having been closed
    final Connection logical2 = mock(Connection.class);
    when(logical2.isClosed()).thenReturn(false);
    when(xaConnection.getConnection()).thenReturn(logical1, logical2);

    final XADataSourceConnectionProvider provider = new XADataSourceConnectionProvider(xaDataSource);

    final ConnectionInfo info1 = provider.connect("jdbc:postgresql://", dialect, targetDriverDialect, hostSpec, props);
    final ConnectionInfo info2 = provider.connect("jdbc:postgresql://", dialect, targetDriverDialect, hostSpec, props);

    verify(xaDataSource, times(1)).getXAConnection();
    verify(xaConnection, times(2)).getConnection();
    assertNotSame(info1.getConnection(), info2.getConnection());
  }

  @Test
  void getXaConnectionOrNull_nullBeforeOpen_thenSet() throws SQLException {
    when(xaConnection.getConnection()).thenReturn(mock(Connection.class));
    final XADataSourceConnectionProvider provider = new XADataSourceConnectionProvider(xaDataSource);

    org.junit.jupiter.api.Assertions.assertNull(provider.getXaConnectionOrNull());
    provider.getOrOpenXaConnection();
    assertSame(xaConnection, provider.getXaConnectionOrNull());
  }

  @Test
  void connect_throwsWhenXaConnectionNull() throws SQLException {
    when(xaDataSource.getXAConnection()).thenReturn(null);
    final XADataSourceConnectionProvider provider = new XADataSourceConnectionProvider(xaDataSource);

    assertThrows(SQLException.class,
        () -> provider.connect("jdbc:postgresql://", dialect, targetDriverDialect, hostSpec, props));
  }

  @Test
  void connect_appliesPerConnectCredentialsToTarget() throws SQLException {
    // Simulates the IAM plugin having rewritten the password to a generated token on the per-connect
    // properties: the provider must apply it to the target XADataSource before opening it.
    final RecordingXaDataSource recording = new RecordingXaDataSource(xaConnection);
    when(xaConnection.getConnection()).thenReturn(mock(Connection.class));
    final XADataSourceConnectionProvider provider = new XADataSourceConnectionProvider(recording);

    final Properties p = new Properties();
    p.setProperty("user", "iam_user");
    p.setProperty("password", "generated-iam-token");

    provider.connect("jdbc:postgresql://", dialect, targetDriverDialect, hostSpec, p);

    assertEquals("iam_user", recording.user);
    assertEquals("generated-iam-token", recording.password);
  }

  /**
   * A concrete {@link XADataSource} that records the user/password applied via its bean setters.
   * Must be public so {@code PropertyUtils} (in another package) can invoke the setters reflectively,
   * as it does for real target XADataSource classes.
   */
  public static class RecordingXaDataSource implements XADataSource {
    private final XAConnection xaConnection;
    String user;
    String password;

    RecordingXaDataSource(final XAConnection xaConnection) {
      this.xaConnection = xaConnection;
    }

    public void setUser(final String user) {
      this.user = user;
    }

    public void setPassword(final String password) {
      this.password = password;
    }

    @Override
    public XAConnection getXAConnection() {
      return this.xaConnection;
    }

    @Override
    public XAConnection getXAConnection(final String user, final String password) {
      return this.xaConnection;
    }

    @Override
    public java.io.PrintWriter getLogWriter() {
      return null;
    }

    @Override
    public void setLogWriter(final java.io.PrintWriter out) {
      // no-op
    }

    @Override
    public void setLoginTimeout(final int seconds) {
      // no-op
    }

    @Override
    public int getLoginTimeout() {
      return 0;
    }

    @Override
    public java.util.logging.Logger getParentLogger() {
      return null;
    }
  }
}
