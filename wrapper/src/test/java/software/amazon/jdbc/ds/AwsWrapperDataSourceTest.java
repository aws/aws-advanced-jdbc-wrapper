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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import software.amazon.jdbc.DataSourceConnectionProvider;
import software.amazon.jdbc.wrapper.ConnectionWrapper;

class AwsWrapperDataSourceTest {
  @Mock ConnectionWrapper mockConnection;
  @Captor ArgumentCaptor<String> urlArgumentCaptor;
  @Captor ArgumentCaptor<Properties> propertiesArgumentCaptor;

  private AutoCloseable closeable;

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  @Test
  public void testGetConnectionWithNewCredentialsWithDataSource() throws SQLException {
    final AwsWrapperDataSource ds = Mockito.spy(new AwsWrapperDataSource());
    final String expectedUrl1 = "protocol//testserver/?user=user1&password=pass1";
    final Properties expectedProperties1 = new Properties();
    expectedProperties1.setProperty("user", "user1");
    expectedProperties1.setProperty("password", "pass1");
    expectedProperties1.setProperty("serverName", "testserver");

    final Properties expectedProperties2 = new Properties();
    expectedProperties2.setProperty("user", "user2");
    expectedProperties2.setProperty("password", "pass2");
    expectedProperties2.setProperty("serverName", "testserver");

    doReturn(mockConnection).when(ds).createConnectionWrapper(propertiesArgumentCaptor.capture(), urlArgumentCaptor.capture(), any());

    ds.setJdbcProtocol("protocol");
    ds.setServerPropertyName("serverName");

    ds.setTargetDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");

    Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty("serverName", "testserver");
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    ds.getConnection("user1", "pass1");
    ds.getConnection("user2", "pass2");

    final List<String> urls = urlArgumentCaptor.getAllValues();
    final List<Properties> properties = propertiesArgumentCaptor.getAllValues();
    assertEquals(2, urls.size());
    assertEquals(2, properties.size());
    assertEquals(expectedUrl1, urls.get(0));
    assertEquals(expectedUrl1, urls.get(1)); // JDBC Url doesn't get updated when we are reusing the connection.
    assertEquals(expectedProperties1, properties.get(0));
    assertEquals(expectedProperties2, properties.get(1));
  }

  @Test
  public void testGetConnectionWithNewCredentialsWithDriverManager() throws SQLException {
    final AwsWrapperDataSource ds = Mockito.spy(new AwsWrapperDataSource());
    final String expectedUrl ="jdbc:postgresql://testserver/";
    final Properties expectedProperties1 = new Properties();
    expectedProperties1.setProperty("user", "user1");
    expectedProperties1.setProperty("password", "pass1");

    final Properties expectedProperties2 = new Properties();
    expectedProperties2.setProperty("user", "user2");
    expectedProperties2.setProperty("password", "pass2");

    doReturn(mockConnection).when(ds).createConnectionWrapper(propertiesArgumentCaptor.capture(), urlArgumentCaptor.capture(), any());

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
}