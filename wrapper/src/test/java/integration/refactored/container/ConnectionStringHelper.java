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

package integration.refactored.container;

import integration.refactored.DatabaseEngine;
import integration.refactored.DriverHelper;
import java.util.Properties;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.StringUtils;

public class ConnectionStringHelper {

  public static String getUrl() {
    return getUrl(null);
  }

  public static String getUrl(final String wrapperPlugins) {
    return getUrl(
        TestEnvironment.getCurrent().getCurrentDriver(),
        TestEnvironment.getCurrent()
            .getInfo()
            .getDatabaseInfo()
            .getInstances()
            .get(0)
            .getEndpoint(),
        TestEnvironment.getCurrent()
            .getInfo()
            .getDatabaseInfo()
            .getInstances()
            .get(0)
            .getEndpointPort(),
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName(),
        wrapperPlugins);
  }

  public static String getUrl(String host, int port, String databaseName, String wrapperPlugins) {
    return getUrl(TestEnvironment.getCurrent().getCurrentDriver(), host, port, databaseName, wrapperPlugins);
  }

  public static String getUrl(
      TestDriver testDriver,
      String host,
      int port,
      String databaseName,
      String wrapperPlugins) {
    final DatabaseEngine databaseEngine = TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine();
    final String requiredParameters = DriverHelper.getDriverRequiredParameters(databaseEngine, testDriver);
    final String url = DriverHelper.getDriverProtocol(databaseEngine, testDriver)
        + host
        + ":"
        + port
        + "/"
        + databaseName
        + requiredParameters
        + (requiredParameters.startsWith("?") ? "&" : "?")
        + "wrapperPlugins=";
    return wrapperPlugins != null ? url + wrapperPlugins : url;
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
            .getEndpoint(),
        TestEnvironment.getCurrent()
            .getInfo()
            .getDatabaseInfo()
            .getInstances()
            .get(0)
            .getEndpointPort(),
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
            .getEndpoint(),
        TestEnvironment.getCurrent()
            .getInfo()
            .getProxyDatabaseInfo()
            .getInstances()
            .get(0)
            .getEndpointPort(),
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
    props.setProperty(
        PropertyDefinition.USER.name,
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getUsername());
    props.setProperty(
        PropertyDefinition.PASSWORD.name,
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getPassword());
    DriverHelper.setTcpKeepAlive(TestEnvironment.getCurrent().getCurrentDriver(), props, false);
    return props;
  }

  public static Properties getDefaultPropertiesWithNoPlugins() {
    final Properties properties = getDefaultProperties();
    properties.setProperty(PropertyDefinition.PLUGINS.name, "");
    return properties;
  }
}
