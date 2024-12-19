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
import software.amazon.jdbc.ConnectionProviderManager;
import software.amazon.jdbc.HikariPooledConnectionProvider;
import software.amazon.jdbc.ds.AwsWrapperDataSource;

public class Main {

  private static final Logger LOGGER = Logger.getLogger(Main.class.getName());

  public static void main(String[] args) throws SQLException {

    if (!software.amazon.jdbc.Driver.isRegistered()) {
      software.amazon.jdbc.Driver.register();
    }

    final ClusterDetails clusterDetails = new ClusterDetails();
    LOGGER.info(clusterDetails.toString());

    simpleWorkflows(clusterDetails);
    simpleDataSourceWorkflow(clusterDetails);
    jndiLookup(clusterDetails);
    driverProfileWorkflow(clusterDetails);
    logQueryWorkflow(clusterDetails);
    internalPools(clusterDetails);
    iamWorkflow(clusterDetails);
    secretsManagerWorkflow(clusterDetails);
    customEndpoints(clusterDetails);
  }

  public static void simpleWorkflows(final ClusterDetails clusterDetails) {
    Properties props = getDefaultProperties(clusterDetails);
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
      simpleDriverWorkflow(props, clusterDetails.endpoint, clusterDetails.databaseName);
    }
  }

  public static void simpleDataSourceWorkflow(final ClusterDetails clusterDetails) {
    Properties props = getDefaultProperties(clusterDetails);
    AwsWrapperDataSource ds = getDataSource(props, clusterDetails);
    try (Connection conn = ds.getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1")) {
      rs.next();
      LOGGER.finest("(simple datasource workflow) Result: " + rs.getInt(1));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void jndiLookup(final ClusterDetails clusterDetails) {
    try {
      AwsWrapperDataSource ds = getDataSource(getDefaultProperties(clusterDetails), clusterDetails);
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

  public static void driverProfileWorkflow(final ClusterDetails clusterDetails) {
    Properties props = getDefaultProperties(clusterDetails);
    props.setProperty("wrapperProfileName", "D0");
    LOGGER.finest("Starting driver profile workflow...");

    simpleDriverWorkflow(props, clusterDetails.endpoint, clusterDetails.databaseName);
  }

  public static void logQueryWorkflow(final ClusterDetails clusterDetails) {
    Properties props = getDefaultProperties(clusterDetails);
    props.setProperty("wrapperPlugins", "logQuery");
    props.setProperty("enhancedLogQueryEnabled", "true");

    LOGGER.finest("Starting log query workflow...");
    simpleDriverWorkflow(props, clusterDetails.endpoint, clusterDetails.databaseName);
  }

  public static void internalPools(final ClusterDetails clusterDetails) {
    Properties props = getDefaultProperties(clusterDetails);
    props.setProperty("wrapperPlugins", "readWriteSplitting");
    HikariPooledConnectionProvider provider =
        new HikariPooledConnectionProvider((hostSpec, poolProps) -> new HikariConfig());
    ConnectionProviderManager.setConnectionProvider(provider);

    LOGGER.finest("Starting internal pools workflow...");
    simpleDriverWorkflow(props, clusterDetails.endpoint, clusterDetails.databaseName);
  }

  public static void iamWorkflow(final ClusterDetails clusterDetails) {
    Properties props = new Properties();
    props.setProperty("wrapperPlugins", "iam");
    props.setProperty("user", clusterDetails.iamUser);

    LOGGER.finest("Starting iam workflow....");
    simpleDriverWorkflow(props, clusterDetails.endpoint, clusterDetails.databaseName);
  }

  public static void secretsManagerWorkflow(final ClusterDetails clusterDetails) {
    Properties props = new Properties();
    props.setProperty("wrapperPlugins", "awsSecretsManager");
    props.setProperty("secretsManagerSecretId", "secrets-manager-id");
    props.setProperty("secretsManagerRegion", clusterDetails.region);

    LOGGER.finest("Starting secrets manager workflow...");
    simpleDriverWorkflow(props, clusterDetails.endpoint, clusterDetails.databaseName);
  }

  public static void customEndpoints(final ClusterDetails clusterDetails) {
    Properties props = getDefaultProperties(clusterDetails);
    props.setProperty("wrapperPlugins", "customEndpoint");
    final String fakeCustomEndpoint = clusterDetails.endpoint.replace(".cluster-", ".cluster-custom-");

    LOGGER.finest("Starting custom endpoints workflow...");
    simpleDriverWorkflow(props, fakeCustomEndpoint, clusterDetails.databaseName);
  }

  private static AwsWrapperDataSource getDataSource(Properties props, final ClusterDetails clusterDetails) {
    AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setTargetDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");
    ds.setJdbcProtocol("jdbc:postgresql://");
    ds.setServerName(clusterDetails.endpoint);
    ds.setDatabase(clusterDetails.databaseName);
    ds.setTargetDataSourceProperties(props);
    return ds;
  }

  private static void simpleDriverWorkflow(Properties props, String endpoint, String databaseName) {
    String url =
        "jdbc:aws-wrapper:postgresql://"
            + endpoint
            + "/"
            + databaseName;

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

  private static Properties getDefaultProperties(final ClusterDetails clusterDetails) {
    Properties props = new Properties();
    props.setProperty("user", clusterDetails.username);
    props.setProperty("password", clusterDetails.password);
    props.setProperty("connectTimeout", "30000");
    props.setProperty("socketTimeout", "30000");
    return props;
  }
}
