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

package reachability.software.amazon.jdbc;

import com.zaxxer.hikari.HikariConfig;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;
import javax.naming.Context;
import javax.naming.InitialContext;
import integration.DriverHelper;
import integration.container.TestDriver;
import integration.container.TestEnvironment;
import software.amazon.jdbc.ConnectionProviderManager;
import software.amazon.jdbc.HikariPooledConnectionProvider;
import software.amazon.jdbc.ds.AwsWrapperDataSource;

public class Main {

  private static final Logger LOGGER = Logger.getLogger(Main.class.getName());

  public static void main(String[] args) throws SQLException {

    if (!TestEnvironment.isAvailable()) {
      throw new RuntimeException("This tests should be run inside a test container.");
    }

    for (TestDriver testDriver : TestEnvironment.getCurrent().getAllowedTestDrivers()) {
      registerDrivers(testDriver);

      simpleWorkflows();
      simpleDataSourceWorkflow();
      jndiLookup();
      driverProfileWorkflow();
      logQueryWorkflow();
      internalPools();
      iamWorkflow();
      secretsManagerWorkflow();
      customEndpoints();
    }
  }

  private static void registerDrivers(final TestDriver testDriver) throws SQLException {
    DriverHelper.unregisterAllDrivers();
    DriverHelper.registerDriver(testDriver);
    TestEnvironment.getCurrent().setCurrentDriver(testDriver);
    LOGGER.finest("Registered " + testDriver + " driver.");
  }

  public static void simpleWorkflows() {
    Properties props = getDefaultProperties();
    final String endpoint = TestEnvironment.getCurrent()
        .getInfo()
        .getDatabaseInfo()
        .getInstances()
        .get(0)
        .getHost();
    final Set<String> simpleWorkflowPlugins = Set.of(
        "executionTime",
        "logQuery",
        "dataCache",
        "efm",
        "efm2",
        "failover",
        "failover2",
        "auroraStaleDns",
        "readWriteSplitting",
        "auroraConnectionTracker",
        "driverMetaData",
        "connectTime",
        "dev",
        "fastestResponseStrategy",
        "initialConnection"
    );

    for (String plugin : simpleWorkflowPlugins) {
      props.setProperty("wrapperPlugins", plugin);
      LOGGER.finest("Testing simple workflow with " + plugin);
      simpleDriverWorkflow(props, endpoint);
    }
  }

  public static void simpleDataSourceWorkflow() {
    Properties props = getDefaultProperties();
    AwsWrapperDataSource ds = getDataSource(props);
    try (Connection conn = ds.getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1")) {
      rs.next();
      LOGGER.finest("(simple datasource workflow) Result: " + rs.getInt(1));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void jndiLookup() {
    try {
      AwsWrapperDataSource ds = getDataSource(getDefaultProperties());
      final Hashtable<String, Object> env = new Hashtable<>();
      env.put(Context.INITIAL_CONTEXT_FACTORY, SimpleJndiContextFactory.class.getName());
      final InitialContext context = new InitialContext(env);
      context.bind("wrapperDataSource", ds);
      context.lookup("wrapperDataSource");
      LOGGER.finest("JNDI test success.");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void driverProfileWorkflow() {
    Properties props = getDefaultProperties();
    props.setProperty("wrapperProfileName", "D0");
    LOGGER.finest("Starting driver profile workflow...");
    final String endpoint = TestEnvironment.getCurrent()
        .getInfo()
        .getDatabaseInfo()
        .getInstances()
        .get(0)
        .getHost();

    simpleDriverWorkflow(props, endpoint);
  }

  public static void logQueryWorkflow() {
    Properties props = getDefaultProperties();
    props.setProperty("wrapperPlugins", "logQuery");
    props.setProperty("enhancedLogQueryEnabled", "true");
    final String endpoint = TestEnvironment.getCurrent()
        .getInfo()
        .getDatabaseInfo()
        .getInstances()
        .get(0)
        .getHost();

    LOGGER.finest("Starting log query workflow...");
    simpleDriverWorkflow(props, endpoint);
  }

  public static void internalPools() {
    Properties props = getDefaultProperties();
    props.setProperty("wrapperPlugins", "readWriteSplitting");
    HikariPooledConnectionProvider provider =
        new HikariPooledConnectionProvider((hostSpec, poolProps) -> new HikariConfig());
    ConnectionProviderManager.setConnectionProvider(provider);

    final String endpoint = TestEnvironment.getCurrent()
        .getInfo()
        .getDatabaseInfo()
        .getInstances()
        .get(0)
        .getHost();

    LOGGER.finest("Starting internal pools workflow...");
    simpleDriverWorkflow(props, endpoint);
  }

  public static void iamWorkflow() {
    Properties props = new Properties();
    props.setProperty("wrapperPlugins", "iam");
    props.setProperty("user", TestEnvironment.getCurrent().getInfo().getIamUsername());
    final String endpoint = TestEnvironment.getCurrent()
        .getInfo()
        .getDatabaseInfo()
        .getInstances()
        .get(0)
        .getHost();

    LOGGER.finest("Starting iam workflow....");
    simpleDriverWorkflow(props, endpoint);
  }

  public static void secretsManagerWorkflow() {
    Properties props = new Properties();
    props.setProperty("wrapperPlugins", "awsSecretsManager");
    props.setProperty("secretsManagerSecretId", "secrets-manager-id");
    props.setProperty("secretsManagerRegion", TestEnvironment.getCurrent().getInfo().getRegion());
    final String endpoint = TestEnvironment.getCurrent()
        .getInfo()
        .getDatabaseInfo()
        .getInstances()
        .get(0)
        .getHost();

    LOGGER.finest("Starting secrets manager workflow...");
    simpleDriverWorkflow(props, endpoint);
  }

  public static void customEndpoints() {
    Properties props = getDefaultProperties();
    props.setProperty("wrapperPlugins", "customEndpoint");
    final String fakeCustomEndpoint = TestEnvironment.getCurrent()
        .getInfo()
        .getDatabaseInfo()
        .getClusterEndpoint()
        .replace(".cluster-", ".cluster-custom-");

    LOGGER.finest("Starting custom endpoints workflow...");
    simpleDriverWorkflow(props, fakeCustomEndpoint);
  }

  private static AwsWrapperDataSource getDataSource(Properties props) {
    AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setTargetDataSourceClassName(DriverHelper.getDataSourceClassname());
    ds.setJdbcProtocol(DriverHelper.getDriverProtocol());
    ds.setServerName(TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getClusterEndpoint());
    ds.setDatabase(TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());
    ds.setTargetDataSourceProperties(props);
    return ds;
  }

  private static void simpleDriverWorkflow(Properties props, String endpoint) {
    String url =
        DriverHelper.getWrapperDriverProtocol()
            + endpoint
            + "/"
            + TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName()
            + DriverHelper.getDriverRequiredParameters();

    try (Connection conn = DriverManager.getConnection(url, props);
        PreparedStatement stmt = conn.prepareStatement("SELECT ?")) {
      stmt.setInt(1, 1);
      stmt.executeQuery();
      ResultSet rs = stmt.executeQuery();
      rs.next();
      LOGGER.finest("(simple driver workflow) Result: " + rs.getInt(1));
      conn.setReadOnly(true);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static Properties getDefaultProperties() {
    Properties props = new Properties();
    props.setProperty("user", TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername());
    props.setProperty("password", TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword());
    //props.setProperty("defaultRowFetchSize", "10");
    return props;
  }
}
