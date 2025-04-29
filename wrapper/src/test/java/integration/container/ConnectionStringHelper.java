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

package integration.container;

import integration.DatabaseEngine;
import integration.DriverHelper;
import integration.TestEnvironmentFeatures;
import integration.TestEnvironmentInfo;
import integration.TestInstanceInfo;
import java.util.Properties;
import java.util.Set;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.StringUtils;

public class ConnectionStringHelper {

  public static String getUrl() {
    return getUrl(
        TestEnvironment.getCurrent().getCurrentDriver(),
        TestEnvironment.getCurrent()
            .getInfo()
            .getDatabaseInfo()
            .getInstances()
            .get(0)
            .getHost(),
        TestEnvironment.getCurrent()
            .getInfo()
            .getDatabaseInfo()
            .getInstances()
            .get(0)
            .getPort(),
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());
  }

  public static String getUrl(String host, int port, String databaseName) {
    return getUrl(TestEnvironment.getCurrent().getCurrentDriver(), host, port, databaseName);
  }

  public static String getUrl(
      TestDriver testDriver,
      String host,
      int port,
      String databaseName) {
    final DatabaseEngine databaseEngine = TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine();
    final String requiredParameters = DriverHelper.getDriverRequiredParameters(databaseEngine, testDriver);
    final String url = DriverHelper.getDriverProtocol(databaseEngine, testDriver)
        + host
        + ":"
        + port
        + "/"
        + databaseName
        + requiredParameters;
    return url;
  }

  public static String getWrapperUrlWithPlugins(String host, int port, String databaseName, String wrapperPlugins) {
    final String url = getWrapperUrl(TestEnvironment.getCurrent().getCurrentDriver(), host, port, databaseName);
    return url
        + (url.contains("?") ? "&" : "?")
        + "wrapperPlugins="
        + wrapperPlugins;
  }

  /**
   * Creates a JDBC url with the writer instance endpoint.
   *
   * @return a JDBC URL.
   */
  public static String getWrapperUrl() {
    return getWrapperUrl(
        TestEnvironment.getCurrent().getCurrentDriver(),
        TestEnvironment.getCurrent()
            .getInfo()
            .getDatabaseInfo()
            .getInstances()
            .get(0)
            .getHost(),
        TestEnvironment.getCurrent()
            .getInfo()
            .getDatabaseInfo()
            .getInstances()
            .get(0)
            .getPort(),
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());
  }

  public static String getWrapperUrl(TestInstanceInfo instance) {
    return getWrapperUrl(
        TestEnvironment.getCurrent().getCurrentDriver(),
        instance.getHost(),
        instance.getPort(),
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());
  }

  public static String getWrapperUrl(String host, int port, String databaseName) {
    return getWrapperUrl(TestEnvironment.getCurrent().getCurrentDriver(), host, port, databaseName);
  }

  public static String getWrapperUrl(
      TestDriver testDriver, String host, int port, String databaseName) {
    return DriverHelper.getWrapperDriverProtocol(
        TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine(), testDriver)
        + host
        + ":"
        + port
        + "/"
        + databaseName
        + DriverHelper.getDriverRequiredParameters(
        TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine(), testDriver);
  }

  public static String getWrapperReaderClusterUrl() {
    return ConnectionStringHelper.getWrapperUrl(
        TestEnvironment.getCurrent().getCurrentDriver(),
        TestEnvironment.getCurrent()
            .getInfo()
            .getDatabaseInfo()
            .getClusterReadOnlyEndpoint(),
        TestEnvironment.getCurrent()
            .getInfo()
            .getDatabaseInfo()
            .getClusterReadOnlyEndpointPort(),
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());
  }

  public static String getProxyWrapperUrl() {
    return getWrapperUrl(
        TestEnvironment.getCurrent().getCurrentDriver(),
        TestEnvironment.getCurrent()
            .getInfo()
            .getProxyDatabaseInfo()
            .getInstances()
            .get(0)
            .getHost(),
        TestEnvironment.getCurrent()
            .getInfo()
            .getProxyDatabaseInfo()
            .getInstances()
            .get(0)
            .getPort(),
        TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getDefaultDbName());
  }

  public static String getProxyUrl() {
    return getUrl(
        TestEnvironment.getCurrent().getCurrentDriver(),
        TestEnvironment.getCurrent()
            .getInfo()
            .getProxyDatabaseInfo()
            .getInstances()
            .get(0)
            .getHost(),
        TestEnvironment.getCurrent()
            .getInfo()
            .getProxyDatabaseInfo()
            .getInstances()
            .get(0)
            .getPort(),
        TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getDefaultDbName());
  }

  public static String getWrapperClusterEndpointUrl() {
    if (StringUtils.isNullOrEmpty(TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getClusterEndpoint())) {
      throw new RuntimeException("Cluster Endpoint is not available in this test environment.");
    }
    return getWrapperUrl(
        TestEnvironment.getCurrent().getCurrentDriver(),
        TestEnvironment.getCurrent()
            .getInfo()
            .getDatabaseInfo()
            .getClusterEndpoint(),
        TestEnvironment.getCurrent()
            .getInfo()
            .getDatabaseInfo()
            .getClusterEndpointPort(),
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());
  }

  public static Properties getDefaultProperties() {
    final Properties props = new Properties();
    TestEnvironmentInfo envInfo = TestEnvironment.getCurrent().getInfo();
    props.setProperty(PropertyDefinition.USER.name, envInfo.getDatabaseInfo().getUsername());
    props.setProperty(PropertyDefinition.PASSWORD.name, envInfo.getDatabaseInfo().getPassword());

    final Set<TestEnvironmentFeatures> features = envInfo.getRequest().getFeatures();
    props.setProperty(PropertyDefinition.ENABLE_TELEMETRY.name, "true");
    props.setProperty(PropertyDefinition.TELEMETRY_SUBMIT_TOPLEVEL.name, "true");
    props.setProperty(
        PropertyDefinition.TELEMETRY_TRACES_BACKEND.name,
        features.contains(TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED) ? "xray" : "none");
    props.setProperty(
        PropertyDefinition.TELEMETRY_METRICS_BACKEND.name,
        features.contains(TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED) ? "otlp" : "none");

    props.setProperty(PropertyDefinition.TCP_KEEP_ALIVE.name, "false");

    if (TestEnvironment.getCurrent().getCurrentDriver() == TestDriver.MARIADB) {
      // This property is sometimes required when using the mariadb driver against multi-az mysql version 8.4, or you
      // will get the error "RSA public key is not available client side" when connecting. The mariadb driver may not
      // fully support mysql 8.4's SSL mechanisms, which is why this property is only required for newer mysql versions.
      props.setProperty("allowPublicKeyRetrieval", "true");
    }

    return props;
  }

  public static Properties getDefaultPropertiesWithNoPlugins() {
    final Properties properties = getDefaultProperties();
    properties.setProperty(PropertyDefinition.PLUGINS.name, "");
    return properties;
  }
}
