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
import java.sql.Statement;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.naming.Context;
import javax.naming.InitialContext;
import software.amazon.jdbc.ConnectionProviderManager;
import software.amazon.jdbc.HikariPooledConnectionProvider;
import software.amazon.jdbc.ds.AwsWrapperDataSource;
import software.amazon.jdbc.plugin.failover.FailoverSuccessSQLException;

public class Main {
  private static final String USERNAME = "username";
  private static final String PASSWORD = "password";
  private static final String PROTOCOL = "jdbc:aws-wrapper:postgresql://";
  private static final String URL = "mydb.cluster-XYZ.us-east-2.rds.amazonaws.com";
  private static final String CUSTOM_URL = "mydb.cluster-custom-XYZ.us-east-2.rds.amazonaws.com";
  private static final String DB = "mydb";
  private static final String IAM_USER = "john_smith";
  private static final String SECRETS_MANAGER_ID = "secrets-manager-id";
  private static final String SECRETS_MANAGER_REGION = "us-east-2";
  // Plugins which do not need extra setup to pass the simple workflow.
  private static final Set<String> simpleWorkflowPlugins = Set.of(
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

  public static void main(String[] args) {
    Properties props = getDefaultProperties();
    testSimpleWorkflows(props);
    simpleDataSourceWorkflow(props);
    // failover();
    jndiLookup();
    driverProfileWorkflow(props);
    logQueryWorkflow(props);
    internalPools(props);
    iamWorkflow();
    secretsManagerWorkflow();
    customEndpoints(props);
  }

  private static void testSimpleWorkflows(Properties props) {
    for (String plugin : simpleWorkflowPlugins) {
      props.setProperty("wrapperPlugins", plugin);
      System.out.println("Testing simple workflow with " + plugin);
      simpleDriverWorkflow(props);
    }
  }

  private static Properties getDefaultProperties() {
    Properties props = new Properties();
    props.setProperty("user", USERNAME);
    props.setProperty("password", PASSWORD);
    props.setProperty("defaultRowFetchSize", "10");
    return props;
  }

  private static AwsWrapperDataSource getDataSource(Properties props) {
    AwsWrapperDataSource ds = new AwsWrapperDataSource();
    ds.setTargetDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");
    ds.setJdbcProtocol("jdbc:postgresql://");
    ds.setServerName(URL);
    ds.setDatabase(DB);
    ds.setTargetDataSourceProperties(props);
    return ds;
  }

  private static void simpleDriverWorkflow(Properties props) {
    simpleDriverWorkflow(props, URL);
  }

  private static void simpleDriverWorkflow(Properties props, String url) {
    try (Connection conn = DriverManager.getConnection(getConnString(url), props);
         PreparedStatement stmt = conn.prepareStatement("SELECT ?")) {
      stmt.setInt(1, 1);
      stmt.executeQuery();
      ResultSet rs = stmt.executeQuery();
      rs.next();
      System.out.println("(simple driver workflow) Result: " + rs.getInt(1));
      conn.setReadOnly(true);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static String getConnString(String url) {
    return PROTOCOL + url + "/" + DB;
  }

  private static void simpleDataSourceWorkflow(Properties props) {
    AwsWrapperDataSource ds = getDataSource(props);
    try (Connection conn = ds.getConnection();
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT 1")) {
      rs.next();
      System.out.println("(simple datasource workflow) Result: " + rs.getInt(1));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void failover() {
    Properties props = getDefaultProperties();
    props.setProperty("wrapperPlugins", "failover2");
    props.setProperty("failoverMode", "reader-or-writer");

    try {
      AwsWrapperDataSource ds = getDataSource(props);
      ds.getReference();
      Connection conn = ds.getConnection();

      while (true) {
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1");
        rs.next();
        System.out.println("(failover test) result: " + rs.getInt(1));
        TimeUnit.SECONDS.sleep(2);
      }
    } catch (Exception e) {
      if (e instanceof FailoverSuccessSQLException) {
        System.out.println("Failover succeeded.");
      }
      throw new RuntimeException(e);
    }
  }

  private static void jndiLookup() {
    try {
      AwsWrapperDataSource ds = getDataSource(getDefaultProperties());
      final Hashtable<String, Object> env = new Hashtable<>();
      env.put(Context.INITIAL_CONTEXT_FACTORY, SimpleJndiContextFactory.class.getName());
      final InitialContext context = new InitialContext(env);
      context.bind("wrapperDataSource", ds);
      context.lookup("wrapperDataSource");
      System.out.println("JNDI test success.");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void driverProfileWorkflow(Properties props) {
    props.setProperty("wrapperProfileName", "D0");
    System.out.println("Starting driver profile workflow...");
    simpleDriverWorkflow(props);
  }

  private static void logQueryWorkflow(Properties props) {
    props.setProperty("wrapperPlugins", "logQuery");
    props.setProperty("enhancedLogQueryEnabled", "true");

    System.out.println("Starting log query workflow...");
    simpleDriverWorkflow(props);
  }

  private static void internalPools(Properties props) {
    props.setProperty("wrapperPlugins", "readWriteSplitting");
    HikariPooledConnectionProvider provider =
        new HikariPooledConnectionProvider((hostSpec, poolProps) -> new HikariConfig());
    ConnectionProviderManager.setConnectionProvider(provider);

    System.out.println("Starting internal pools workflow...");
    simpleDriverWorkflow(props);
  }

  private static void iamWorkflow() {
    Properties props = new Properties();
    props.setProperty("wrapperPlugins", "iam");
    props.setProperty("user", IAM_USER);
    System.out.println("Starting iam workflow....");
    simpleDriverWorkflow(props);
  }

  private static void secretsManagerWorkflow() {
    Properties props = new Properties();
    props.setProperty("wrapperPlugins", "awsSecretsManager");
    props.setProperty("secretsManagerSecretId", SECRETS_MANAGER_ID);
    props.setProperty("secretsManagerRegion", SECRETS_MANAGER_REGION);
    System.out.println("Starting secrets manager workflow...");
    simpleDriverWorkflow(props);
  }

  private static void customEndpoints(Properties props) {
    props.setProperty("wrapperPlugins", "customEndpoint");
    System.out.println("Starting custom endpoints workflow...");
    simpleDriverWorkflow(props, CUSTOM_URL);
  }
}
