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

import integration.TestEnvironmentFeatures;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.DisableOnTestFeature;
import integration.container.condition.EnableOnNumOfInstances;
import integration.container.condition.EnableOnTestFeature;
import integration.container.condition.MakeSureFirstInstanceWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.logging.Logger;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.srw.SimpleReadWriteSplittingPlugin;

@TestMethodOrder(MethodOrderer.MethodName.class)
@ExtendWith(TestDriverProvider.class)
@EnableOnTestFeature({
    TestEnvironmentFeatures.FAILOVER_SUPPORTED,
    TestEnvironmentFeatures.RDS_PROXY,
})
@DisableOnTestFeature({
    TestEnvironmentFeatures.PERFORMANCE,
    TestEnvironmentFeatures.RUN_HIBERNATE_TESTS_ONLY,
    TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
    TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
    TestEnvironmentFeatures.RUN_DB_METRICS_ONLY})
@EnableOnNumOfInstances(min = 2)
@MakeSureFirstInstanceWriter
@Order(15)


public class ProxyReadWriteSplittingTest extends SimpleReadWriteSplittingTest {

  private static final Logger LOGGER = Logger.getLogger(ProxyReadWriteSplittingTest.class.getName());

  /**
   * Verifies connectivity to the read-only proxy endpoint before running tests.
   * Uses longer timeouts and retry logic to handle DNS propagation delays.
   */
  private void verifyReadOnlyEndpointConnectivity() throws SQLException {
    String readOnlyEndpoint = getReaderEndpoint();
    LOGGER.info("Verifying connectivity to read-only endpoint: " + readOnlyEndpoint);

    Properties props = new Properties();
    props.setProperty("user", TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername());
    props.setProperty("password", TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword());
    props.setProperty("connectTimeout", "60000"); // 60 seconds
    props.setProperty("socketTimeout", "30000");  // 30 seconds

    String url = ConnectionStringHelper.getWrapperUrl(readOnlyEndpoint);

    int maxRetries = 3;
    SQLException lastException = null;

    for (int attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        LOGGER.info("Connection attempt " + attempt + " to read-only endpoint");
        try (Connection conn = DriverManager.getConnection(url, props);
             Statement stmt = conn.createStatement()) {
          stmt.executeQuery("SELECT 1");
          LOGGER.info("Successfully connected to read-only endpoint");
          return;
        }
      } catch (SQLException e) {
        lastException = e;
        LOGGER.warning("Connection attempt " + attempt + " failed: " + e.getMessage());
        if (attempt < maxRetries) {
          try {
            Thread.sleep(10000); // Wait 10 seconds between retries
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new SQLException("Interrupted while waiting to retry connection", ie);
          }
        }
      }
    }

    throw new SQLException("Failed to connect to read-only endpoint after " + maxRetries + " attempts", lastException);
  }

  @Override
  protected Properties getProps() {
    // Verify read-only endpoint connectivity before running tests
    try {
      verifyReadOnlyEndpointConnectivity();
    } catch (SQLException e) {
      throw new RuntimeException("Read-only endpoint connectivity verification failed", e);
    }

    final Properties props = getDefaultPropsNoPlugins();
    PropertyDefinition.PLUGINS.set(props, "srw");
    props.setProperty(SimpleReadWriteSplittingPlugin.VERIFY_NEW_SRW_CONNECTIONS.name, "false");
    props.setProperty(SimpleReadWriteSplittingPlugin.SRW_WRITE_ENDPOINT.name, getWriterEndpoint());
    props.setProperty(SimpleReadWriteSplittingPlugin.SRW_READ_ENDPOINT.name, getReaderEndpoint());

    // Use longer timeouts for RDS Proxy connections
    props.setProperty("connectTimeout", "60000"); // 60 seconds
    props.setProperty("socketTimeout", "30000");  // 30 seconds

    return props;
  }

  @Override
  protected String getWriterEndpoint() {
    return TestEnvironment.getCurrent()
            .getInfo()
            .getRdsProxyReadWriteEndpoint();
  }

  @Override
  protected String getReaderEndpoint() {
    return TestEnvironment.getCurrent()
            .getInfo()
            .getRdsProxyReadOnlyEndpoint();
  }

  @TestTemplate
  @Disabled("Skipping as test involves disabling connectivity to the srw reader endpoint.")
  @Override
  public void test_writerFailover_setReadOnlyTrueFalse() throws SQLException {
    // Skipping as RDS Proxy is not set up with Toxiproxy.
  }

  @TestTemplate
  @Disabled("Skipping as test involves disabling connectivity to the srw reader endpoint.")
  @Override
  public void test_failoverReaderToWriter_setReadOnlyTrueFalse() {
    // Skipping as RDS Proxy is not set up with Toxiproxy.
  }


  @TestTemplate
  @Disabled("Skipping because RDS Proxy provides its own connection pooling.")
  @Override
  public void test_pooledConnectionFailover() throws SQLException, InterruptedException {
  }

  @TestTemplate
  @Disabled("Skipping because RDS Proxy provides its own connection pooling.")
  @Override
  public void test_pooledConnectionFailoverWithClusterURL() throws SQLException, InterruptedException {
  }

  @TestTemplate
  @Disabled("Skipping because RDS Proxy provides its own connection pooling.")
  @Override
  public void test_pooledConnection_failoverFailed() throws SQLException {
  }

  @TestTemplate
  @Disabled("Skipping because RDS Proxy provides its own connection pooling.")
  @Override
  public void test_pooledConnection_failoverInTransaction() throws SQLException {
  }
}

