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

package integration.container.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import integration.DriverHelper;
import integration.TestEnvironmentFeatures;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriver;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.DisableOnTestDriver;
import integration.container.condition.DisableOnTestFeature;
import integration.util.SimpleJndiContextFactory;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Hashtable;
import java.util.Properties;
import java.util.logging.Logger;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.ds.AwsWrapperDataSource;
import software.amazon.jdbc.wrapper.ConnectionWrapper;

@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@DisableOnTestFeature({
    TestEnvironmentFeatures.PERFORMANCE,
    TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
    TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT})
@Order(6)
public class DataSourceTests {

  private static final Logger LOGGER = Logger.getLogger(DataSourceTests.class.getName());

  @TestTemplate
  @DisableOnTestDriver(TestDriver.MARIADB)
  public void testConnectionWithDataSourceClassNameAndServerNameFromJndiLookup()
      throws SQLException, NamingException, IllegalAccessException {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setJdbcProtocol(DriverHelper.getDriverProtocol());
    ds.setServerName(TestEnvironment.getCurrent()
        .getInfo()
        .getDatabaseInfo()
        .getInstances()
        .get(0)
        .getHost());
    ds.setDatabase(TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());
    ds.setUser(TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername());
    ds.setPassword(TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword());
    ds.setTargetDataSourceClassName(DriverHelper.getDataSourceClassname());

    final Properties targetDataSourceProps = ConnectionStringHelper.getDefaultPropertiesWithNoPlugins();
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    final Hashtable<String, Object> env = new Hashtable<>();
    env.put(Context.INITIAL_CONTEXT_FACTORY, SimpleJndiContextFactory.class.getName());
    final InitialContext context = new InitialContext(env);
    context.bind("wrapperDataSource", ds);
    final AwsWrapperDataSource dsFromJndiLookup =
        (AwsWrapperDataSource) context.lookup("wrapperDataSource");
    assertNotNull(dsFromJndiLookup);

    assertNotSame(ds, dsFromJndiLookup);
    final Properties jndiDsProperties = dsFromJndiLookup.getTargetDataSourceProperties();

    /*
     The following properties are not passed to target driver.
     They will be dynamically added while opening a connection.
    */
    targetDataSourceProps.remove("user");
    targetDataSourceProps.remove("password");

    assertEquals(targetDataSourceProps, jndiDsProperties);

    for (Field f : ds.getClass().getFields()) {
      assertEquals(f.get(ds), f.get(dsFromJndiLookup));
    }

    try (final Connection conn =
        dsFromJndiLookup.getConnection(
            TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername(),
            TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword())) {

      assertTrue(conn.isWrapperFor(DriverHelper.getConnectionClass()));
      assertEquals(
          conn.getCatalog(),
          TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());

      assertTrue(conn.isValid(10));
    }
  }

  @TestTemplate
  public void testConnectionWithDataSourceClassNameAndUrlFromJndiLookup()
      throws SQLException, NamingException, IllegalAccessException {
    final AwsWrapperDataSource ds = new AwsWrapperDataSource();

    ds.setTargetDataSourceClassName(DriverHelper.getDataSourceClassname());
    ds.setJdbcProtocol(DriverHelper.getDriverProtocol());
    ds.setJdbcUrl(ConnectionStringHelper.getUrl());
    ds.setUser(TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername());
    ds.setPassword(TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword());

    final Properties targetDataSourceProps = new Properties();
    targetDataSourceProps.setProperty(PropertyDefinition.PLUGINS.name, "");
    ds.setTargetDataSourceProperties(targetDataSourceProps);

    final Hashtable<String, Object> env = new Hashtable<>();
    env.put(Context.INITIAL_CONTEXT_FACTORY, SimpleJndiContextFactory.class.getName());
    final InitialContext context = new InitialContext(env);
    context.bind("wrapperDataSource", ds);
    final AwsWrapperDataSource dsFromJndiLookup =
        (AwsWrapperDataSource) context.lookup("wrapperDataSource");
    assertNotNull(dsFromJndiLookup);

    assertNotSame(ds, dsFromJndiLookup);
    final Properties jndiDsProperties = dsFromJndiLookup.getTargetDataSourceProperties();

    /*
     The following properties are not passed to target driver.
     They will be dynamically added while opening a connection.
    */
    targetDataSourceProps.remove("user");
    targetDataSourceProps.remove("password");

    assertEquals(targetDataSourceProps, jndiDsProperties);

    for (Field f : ds.getClass().getFields()) {
      assertEquals(f.get(ds), f.get(dsFromJndiLookup));
    }

    try (final Connection conn =
        dsFromJndiLookup.getConnection(
            TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername(),
            TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword())) {

      assertTrue(conn instanceof ConnectionWrapper);
      assertTrue(conn.isWrapperFor(DriverHelper.getConnectionClass()));
      assertEquals(
          conn.getCatalog(),
          TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());

      assertTrue(conn.isValid(10));
    }
  }
}
