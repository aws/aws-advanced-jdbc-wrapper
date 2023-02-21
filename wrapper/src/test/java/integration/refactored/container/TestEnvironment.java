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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import integration.refactored.DatabaseEngine;
import integration.refactored.TestEnvironmentFeatures;
import integration.refactored.TestEnvironmentInfo;
import integration.refactored.TestInstanceInfo;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import org.testcontainers.shaded.org.apache.commons.lang3.NotImplementedException;
import software.amazon.jdbc.util.StringUtils;

public class TestEnvironment {

  private static TestEnvironment env;

  private TestEnvironmentInfo info;
  private HashMap<String, Proxy> proxies;
  private TestDriver currentDriver;

  private TestEnvironment() {}

  public static synchronized TestEnvironment getCurrent() {
    if (env == null) {
      env = create();
    }
    return env;
  }

  private static TestEnvironment create() {
    TestEnvironment environment = new TestEnvironment();

    String infoJson = System.getenv("TEST_ENV_INFO_JSON");

    if (StringUtils.isNullOrEmpty(infoJson)) {
      throw new RuntimeException("Environment variable TEST_ENV_INFO_JSON is required.");
    }

    try {
      final ObjectMapper mapper = new ObjectMapper();
      environment.info = mapper.readValue(infoJson, TestEnvironmentInfo.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Invalid value in TEST_ENV_INFO_JSON.", e);
    }

    if (environment
        .info
        .getRequest()
        .getFeatures()
        .contains(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED)) {
      initProxies(environment);
    }

    return environment;
  }

  private static void initProxies(TestEnvironment environment) {
    environment.proxies = new HashMap<>();

    int proxyControlPort = environment.info.getProxyDatabaseInfo().getControlPort();
    for (TestInstanceInfo instance : environment.info.getProxyDatabaseInfo().getInstances()) {
      ToxiproxyClient client = new ToxiproxyClient(instance.getEndpoint(), proxyControlPort);
      List<Proxy> proxies;
      try {
        proxies = client.getProxies();
      } catch (IOException e) {
        throw new RuntimeException("Error getting proxies for " + instance.getInstanceId(), e);
      }
      if (proxies == null || proxies.isEmpty()) {
        throw new RuntimeException("Proxy for " + instance.getInstanceId() + " is not found.");
      }
      environment.proxies.put(instance.getInstanceId(), proxies.get(0));
    }

    if (!StringUtils.isNullOrEmpty(environment.info.getProxyDatabaseInfo().getClusterEndpoint())) {
      ToxiproxyClient client =
          new ToxiproxyClient(
              environment.info.getProxyDatabaseInfo().getClusterEndpoint(), proxyControlPort);
      Proxy proxy =
          environment.getProxy(
              client,
              environment.info.getDatabaseInfo().getClusterEndpoint(),
              environment.info.getDatabaseInfo().getClusterEndpointPort());
      environment.proxies.put(environment.info.getProxyDatabaseInfo().getClusterEndpoint(), proxy);
    }

    if (!StringUtils.isNullOrEmpty(
        environment.info.getProxyDatabaseInfo().getClusterReadOnlyEndpoint())) {
      ToxiproxyClient client =
          new ToxiproxyClient(
              environment.info.getProxyDatabaseInfo().getClusterReadOnlyEndpoint(),
              proxyControlPort);
      Proxy proxy =
          environment.getProxy(
              client,
              environment.info.getDatabaseInfo().getClusterReadOnlyEndpoint(),
              environment.info.getDatabaseInfo().getClusterReadOnlyEndpointPort());
      environment.proxies.put(
          environment.info.getProxyDatabaseInfo().getClusterReadOnlyEndpoint(), proxy);
    }
  }

  private Proxy getProxy(ToxiproxyClient proxyClient, String host, int port) {
    final String upstream = host + ":" + port;
    try {
      return proxyClient.getProxy(upstream);
    } catch (IOException e) {
      throw new RuntimeException("Error getting proxy for " + upstream, e);
    }
  }

  public Proxy getProxy(String instanceName) {
    Proxy proxy = this.proxies.get(instanceName);
    if (proxy == null) {
      throw new RuntimeException("Proxy for " + instanceName + " not found.");
    }
    return proxy;
  }

  public java.util.Collection<Proxy> getProxies() {
    return Collections.unmodifiableCollection(this.proxies.values());
  }

  public TestEnvironmentInfo getInfo() {
    return this.info;
  }

  public void setCurrentDriver(TestDriver testDriver) {
    this.currentDriver = testDriver;
  }

  public TestDriver getCurrentDriver() {
    return this.currentDriver;
  }

  public List<TestDriver> getAllowedTestDrivers() {
    ArrayList<TestDriver> allowedTestDrivers = new ArrayList<>();
    for (TestDriver testDriver : TestDriver.values()) {
      if (isTestDriverAllowed(testDriver)) {
        allowedTestDrivers.add(testDriver);
      }
    }
    return allowedTestDrivers;
  }

  public boolean isTestDriverAllowed(TestDriver testDriver) {

    boolean disabledByFeature;
    boolean driverCompatibleToDatabaseEngine;

    final Set<TestEnvironmentFeatures> features = this.info.getRequest().getFeatures();
    final DatabaseEngine databaseEngine = this.info.getRequest().getDatabaseEngine();

    switch (testDriver) {
      case MYSQL:
        driverCompatibleToDatabaseEngine =
            databaseEngine == DatabaseEngine.MYSQL || databaseEngine == DatabaseEngine.MARIADB;
        disabledByFeature = features.contains(TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS);
        break;
      case PG:
        driverCompatibleToDatabaseEngine = databaseEngine == DatabaseEngine.PG;
        disabledByFeature = features.contains(TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS);
        break;
      case MARIADB:
        driverCompatibleToDatabaseEngine =
            databaseEngine == DatabaseEngine.MYSQL || databaseEngine == DatabaseEngine.MARIADB;
        disabledByFeature = features.contains(TestEnvironmentFeatures.SKIP_MARIADB_DRIVER_TESTS);
        break;
      default:
        throw new NotImplementedException(testDriver.toString());
    }

    if (disabledByFeature || !driverCompatibleToDatabaseEngine) {
      // this driver is disabled
      return false;
    }
    return true;
  }
}
