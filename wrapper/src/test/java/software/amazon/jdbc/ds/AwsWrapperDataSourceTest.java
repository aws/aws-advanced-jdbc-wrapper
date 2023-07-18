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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import integration.container.TestDriver;
import integration.container.condition.DisableOnTestDriver;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.postgresql.ds.PGSimpleDataSource;
import software.amazon.jdbc.wrapper.ConnectionWrapper;

class AwsWrapperDataSourceTest {

  @Mock ConnectionWrapper mockConnection;
  @Captor ArgumentCaptor<String> urlArgumentCaptor;
  @Captor ArgumentCaptor<Properties> propertiesArgumentCaptor;

  private AutoCloseable closeable;
  private AwsWrapperDataSource ds;

  @BeforeEach
  void setUp() throws SQLException {
    closeable = MockitoAnnotations.openMocks(this);
    ds = spy(new AwsWrapperDataSource());
    ds.setTargetDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");
    doReturn(mockConnection)
        .when(ds)
        .createConnectionWrapper(
            propertiesArgumentCaptor.capture(), urlArgumentCaptor.capture(), any(), any(), any(), any(), any());
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  @Test
  public void testGetConnectionWithNewCredentialsWithDataSource() throws SQLException {
    final String expectedUrl1 = "protocol//testserver/";
    final Properties expectedProperties1 = new Properties();
    expectedProperties1.setProperty("user", "user1");
    expectedProperties1.setProperty("password", "pass1");

    final Properties expectedProperties2 = new Properties();
    expectedProperties2.setProperty("user", "user2");
    expectedProperties2.setProperty("password", "pass2");

    ds.setJdbcProtocol("protocol");
    ds.setServerName("testserver");

    ds.getConnection("user1", "pass1");
    ds.getConnection("user2", "pass2");

    final List<String> urls = urlArgumentCaptor.getAllValues();
    final List<Properties> properties = propertiesArgumentCaptor.getAllValues();
    assertEquals(2, urls.size());
    assertEquals(2, properties.size());
    assertEquals(expectedUrl1, urls.get(0));
    assertEquals(expectedUrl1, urls.get(1)); // JDBC Url doesn't get updated when we are reusing the data source.
    assertEquals(expectedProperties1, properties.get(0));
    assertEquals(expectedProperties2, properties.get(1));
  }

  @Test
  public void testGetConnectionWithNewCredentialsWithDriverManager() throws SQLException {
    final String expectedUrl = "jdbc:postgresql://testserver/";
    final Properties expectedProperties1 = new Properties();
    expectedProperties1.setProperty("user", "user1");
    expectedProperties1.setProperty("password", "pass1");

    final Properties expectedProperties2 = new Properties();
    expectedProperties2.setProperty("user", "user2");
    expectedProperties2.setProperty("password", "pass2");

    ds.setJdbcUrl("jdbc:postgresql://testserver/");

    ds.getConnection("user1", "pass1");
    ds.getConnection("user2", "pass2");

    final List<String> urls = urlArgumentCaptor.getAllValues();
    final List<Properties> properties = propertiesArgumentCaptor.getAllValues();
    assertEquals(2, urls.size());
    assertEquals(2, properties.size());
    assertEquals(expectedUrl, urls.get(0));
    assertEquals(expectedUrl, urls.get(1));
    assertEquals(expectedProperties1, properties.get(0));
    assertEquals(expectedProperties2, properties.get(1));
  }

  @Test
  public void testConnectionWithDataSourceClassNameAndUrl() throws SQLException {
    final String expectedUrl = "jdbc:postgresql://testserver/db";
    ds.setJdbcUrl(expectedUrl);

    try (final Connection conn = ds.getConnection("user", "pass")) {
      final List<String> urls = urlArgumentCaptor.getAllValues();
      final List<Properties> properties = propertiesArgumentCaptor.getAllValues();

      assertEquals(1, urls.size());
      assertEquals(1, properties.size());
      assertEquals(expectedUrl, urls.get(0));
    }
  }

  @Test
  public void testConnectionWithUrl() throws SQLException {
    final String expectedUrl = "protocol://testserver/";
    ds.setJdbcUrl("protocol://testserver/");

    try (final Connection conn = ds.getConnection("user", "pass")) {
      final List<String> urls = urlArgumentCaptor.getAllValues();
      final List<Properties> properties = propertiesArgumentCaptor.getAllValues();

      assertEquals(1, urls.size());
      assertEquals(1, properties.size());
      assertEquals(expectedUrl, urls.get(0));
    }
  }

  @Test
  public void testConnectionWithDataSourceClassNameAndServerName() throws SQLException {
    final String expectedUrl = "protocol//testServer/db";

    ds.setJdbcProtocol("protocol");
    ds.setServerName("testServer");
    ds.setDatabase("db");

    try (final Connection conn = ds.getConnection("user", "pass")) {
      final List<String> urls = urlArgumentCaptor.getAllValues();
      final List<Properties> properties = propertiesArgumentCaptor.getAllValues();

      assertEquals(1, urls.size());
      assertEquals(1, properties.size());
      assertEquals(expectedUrl, urls.get(0));
    }
  }

  @Test
  @DisableOnTestDriver(TestDriver.MARIADB)
  public void testConnectionWithDataSourceClassNameAndCredentialProperties() throws SQLException {
    final String expectedUrl = "protocol//testServer/db";
    ds.setJdbcProtocol("protocol");

    final Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("serverName", "testServer");
    targetDataSourceProps.setProperty("database", "db");
    targetDataSourceProps.setProperty("user", "user");
    targetDataSourceProps.setProperty("password", "pass");
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    try (final Connection conn = ds.getConnection()) {
      final List<String> urls = urlArgumentCaptor.getAllValues();
      final List<Properties> properties = propertiesArgumentCaptor.getAllValues();

      assertEquals(1, urls.size());
      assertEquals(1, properties.size());
      assertEquals(expectedUrl, urls.get(0));
    }
  }

  @Test
  public void testConnectionWithDataSourceClassNameMissingProtocol() {
    ds = new AwsWrapperDataSource();

    final Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("serverName", "testServer");
    targetDataSourceProps.setProperty("database", "db");
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    assertThrows(SQLException.class, () -> ds.getConnection("user", "pass"));
  }

  @Test
  public void testConnectionWithDataSourceClassNameMissingServer() {
    ds = new AwsWrapperDataSource();
    ds.setJdbcProtocol("protocol");
    ds.setDatabase("db");

    assertThrows(SQLException.class, () -> ds.getConnection("user", "pass"));
  }

  @Test
  public void testConnectionWithDataSourceClassNameMissingDatabase() {
    ds = new AwsWrapperDataSource();
    ds.setJdbcProtocol("protocol");

    final Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("serverName", "testServer");
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    assertThrows(SQLException.class, () -> ds.getConnection("user", "pass"));
  }

  @Test
  public void testConnectionWithUrlMissingPassword() {
    ds = new AwsWrapperDataSource();
    ds.setJdbcUrl("protocol://testserver/");

    assertThrows(SQLException.class, () -> ds.getConnection("user", ""));
  }

  @Test
  public void testSetLoginTimeout() throws SQLException {
    ds.setLoginTimeout(30);
    assertEquals(30, ds.getLoginTimeout());
    assertThrows(SQLException.class, () -> ds.setLoginTimeout(-100));
  }

  @Test
  @DisableOnTestDriver(TestDriver.MARIADB)
  public void testSetLoginTimeoutOnTargetDataSource() throws SQLException {
    PGSimpleDataSource simpleDS = new PGSimpleDataSource();
    doReturn(simpleDS).when(ds).createTargetDataSource();

    ds.setJdbcUrl("jdbc:postgresql://testserver/");

    try (final Connection conn = ds.getConnection()) {
      assertEquals(0, simpleDS.getLoginTimeout());
    }

    ds.setLoginTimeout(500);
    try (final Connection conn = ds.getConnection()) {
      assertEquals(500, simpleDS.getLoginTimeout());
    }
  }
}
