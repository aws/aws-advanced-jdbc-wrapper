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

import integration.refactored.DriverHelper;
import java.util.Properties;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.StringUtils;

public class ConnectionStringHelper {

  public static String getClusterUrl() {
    return getUrl(
        TestEnvironment.getCurrent().getCurrentDriver(),
        TestEnvironment.getCurrent()
            .getInfo()
            .getDatabaseInfo()
            .getClusterEndpoint(),
        TestEnvironment.getCurrent()
            .getInfo()
            .getDatabaseInfo()
            .getInstances()
            .get(0)
            .getEndpointPort(),
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());
  }

  public static String getUrl() {
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
        TestEnvironment.getCurrent().getInfo().getDatabaseInfo().getDefaultDbName());
  }

  public static String getUrl(String host, int port, String databaseName) {
    return getUrl(TestEnvironment.getCurrent().getCurrentDriver(), host, port, databaseName);
  }

  public static String getUrl(TestDriver testDriver, String host, int port, String databaseName) {
    return DriverHelper.getDriverProtocol(
        TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine(), testDriver)
        + host
        + ":"
        + port
        + "/"
        + databaseName
        + DriverHelper.getDriverRequiredParameters(
        TestEnvironment.getCurrent().getInfo().getRequest().getDatabaseEngine(), testDriver);
  }

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
        TestEnvironment.getCurrent().getInfo().getProxyDatabaseInfo().getDefaultDbName());
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
}
