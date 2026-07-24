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

package software.amazon.jdbc.ds;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.ConnectionProvider;
import software.amazon.jdbc.Driver;
import software.amazon.jdbc.ds.AwsWrapperXADataSource.ResolvedConfig;

/** Unit tests for {@link AwsWrapperXADataSource} configuration and fail-fast validation. */
public class AwsWrapperXADataSourceTest {

  @AfterEach
  void tearDown() {
    Driver.resetCustomConnectionProvider();
  }

  @Test
  void missingTargetClass_failsFast() {
    final AwsWrapperXADataSource ds = new AwsWrapperXADataSource();
    ds.setJdbcUrl("jdbc:aws-wrapper:postgresql://host/db");
    final SQLException ex = assertThrows(SQLException.class, ds::getXAConnection);
    assertTrue(ex.getMessage().contains("XADataSource"));
  }

  @Test
  void targetNotXaDataSource_failsFast() {
    final AwsWrapperXADataSource ds = new AwsWrapperXADataSource();
    ds.setJdbcUrl("jdbc:aws-wrapper:postgresql://host/db");
    ds.setTargetDataSourceClassName("java.lang.String");
    final SQLException ex = assertThrows(SQLException.class, ds::getXAConnection);
    assertTrue(ex.getMessage().contains("java.lang.String"));
  }

  @Test
  void internalConnectionPool_failsFast() {
    final AwsWrapperXADataSource ds = new AwsWrapperXADataSource();
    ds.setJdbcUrl("jdbc:aws-wrapper:postgresql://host/db");
    ds.setTargetDataSourceClassName(FakeXaDataSource.class.getName());
    final Properties props = new Properties();
    props.setProperty("connectionPoolType", "HIKARI");
    ds.setTargetDataSourceProperties(props);

    final SQLException ex = assertThrows(SQLException.class, ds::getXAConnection);
    assertTrue(ex.getMessage().contains("internal connection pool"));
  }

  @Test
  void customConnectionProvider_failsFast() {
    Driver.setCustomConnectionProvider(org.mockito.Mockito.mock(ConnectionProvider.class));

    final AwsWrapperXADataSource ds = new AwsWrapperXADataSource();
    ds.setJdbcUrl("jdbc:aws-wrapper:postgresql://host/db");
    ds.setTargetDataSourceClassName(FakeXaDataSource.class.getName());

    final SQLException ex = assertThrows(SQLException.class, ds::getXAConnection);
    assertTrue(ex.getMessage().contains("custom connection provider"));
  }

  @Test
  void createTargetXADataSource_returnsInstance() throws SQLException {
    final AwsWrapperXADataSource ds = new AwsWrapperXADataSource();
    ds.setTargetDataSourceClassName(FakeXaDataSource.class.getName());
    final XADataSource target = ds.createTargetXADataSource();
    assertTrue(target instanceof FakeXaDataSource);
  }

  @Test
  void configureTargetXaDataSource_appliesUrlAndCredentials() throws SQLException {
    final AwsWrapperXADataSource ds = new AwsWrapperXADataSource();
    ds.setJdbcUrl("jdbc:aws-wrapper:postgresql://host:5432/mydb");
    ds.setTargetDataSourceClassName(FakeXaDataSource.class.getName());
    ds.setUser("alice");
    ds.setPassword("secret");

    final FakeXaDataSource target = new FakeXaDataSource();
    final ResolvedConfig config = ds.resolveConfig();
    ds.configureTargetXaDataSource(target, config);

    assertEquals("jdbc:postgresql://host:5432/mydb", target.getUrl());
    assertEquals("alice", target.getUser());
    assertEquals("secret", target.getPassword());
  }

  @Test
  void configureTargetXaDataSource_stripsWrapperParamsFromTargetUrl() throws SQLException {
    // Wrapper-specific query parameters must not leak into the URL handed to the target driver;
    // target-driver parameters (ssl, ApplicationName, ...) must be preserved.
    final AwsWrapperXADataSource ds = new AwsWrapperXADataSource();
    ds.setJdbcUrl(
        "jdbc:aws-wrapper:postgresql://host:5432/mydb?wrapperPlugins=iam&ssl=true&ApplicationName=myapp");
    ds.setTargetDataSourceClassName(FakeXaDataSource.class.getName());

    final FakeXaDataSource target = new FakeXaDataSource();
    final ResolvedConfig config = ds.resolveConfig();
    ds.configureTargetXaDataSource(target, config);

    final String targetUrl = target.getUrl();
    assertFalse(targetUrl.contains("wrapperPlugins"), "wrapper parameter leaked to target URL: " + targetUrl);
    assertTrue(targetUrl.contains("ssl=true"), "target parameter was dropped: " + targetUrl);
    assertTrue(targetUrl.contains("ApplicationName=myapp"), "target parameter was dropped: " + targetUrl);
    assertTrue(targetUrl.startsWith("jdbc:postgresql://host:5432/mydb"), "unexpected base URL: " + targetUrl);
  }

  /** A minimal fake XADataSource exposing bean setters that PropertyUtils can invoke by name. */
  public static class FakeXaDataSource implements XADataSource {
    private String url;
    private String user;
    private String password;

    public String getUrl() {
      return url;
    }

    public void setUrl(final String url) {
      this.url = url;
    }

    public String getUser() {
      return user;
    }

    public void setUser(final String user) {
      this.user = user;
    }

    public String getPassword() {
      return password;
    }

    public void setPassword(final String password) {
      this.password = password;
    }

    @Override
    public XAConnection getXAConnection() {
      return null;
    }

    @Override
    public XAConnection getXAConnection(final String user, final String password) {
      return null;
    }

    @Override
    public PrintWriter getLogWriter() {
      return null;
    }

    @Override
    public void setLogWriter(final PrintWriter out) {
    }

    @Override
    public void setLoginTimeout(final int seconds) {
    }

    @Override
    public int getLoginTimeout() {
      return 0;
    }

    @Override
    public Logger getParentLogger() {
      return null;
    }
  }
}
