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

import com.amazonaws.xray.AWSXRay;
import com.amazonaws.xray.AWSXRayRecorderBuilder;
import com.amazonaws.xray.config.DaemonConfiguration;
import com.amazonaws.xray.emitters.Emitter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import integration.DatabaseEngine;
import integration.TestEnvironmentFeatures;
import integration.TestEnvironmentInfo;
import integration.TestInstanceInfo;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.MetricReader;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import software.amazon.jdbc.Driver;
import software.amazon.jdbc.util.StringUtils;

public class TestEnvironment {

  private static final Logger LOGGER = Logger.getLogger(TestEnvironment.class.getName());

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

      // Helps to eliminate problem with proxied endpoints.
      Driver.setPrepareHostFunc((host) -> {
        if (host.endsWith(".proxied")) {
          return host.substring(0, host.length() - ".proxied".length()); // removes prefix at the end of host
        }
        return host;
      });
    }

    if (environment
        .info
        .getRequest()
        .getFeatures()
        .contains(TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED)) {
      try {
        String xrayEndpoint = String.format("%s:%d",
            environment.info.getTracesTelemetryInfo().getEndpoint(),
            environment.info.getTracesTelemetryInfo().getEndpointPort());

        DaemonConfiguration configuration = new DaemonConfiguration();
        configuration.setUDPAddress(xrayEndpoint);

        Emitter emitter = Emitter.create(configuration);
        AWSXRayRecorderBuilder builder = AWSXRayRecorderBuilder.standard().withEmitter(emitter);
        AWSXRay.setGlobalRecorder(builder.build());
      } catch (Exception ex) {
        throw new RuntimeException("Error initializing XRay.", ex);
      }
    }

    if (environment
        .info
        .getRequest()
        .getFeatures()
        .contains(TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED)) {

      try {
        String metricsEndpoint = String.format("http://%s:%d",
            environment.info.getMetricsTelemetryInfo().getEndpoint(),
            environment.info.getMetricsTelemetryInfo().getEndpointPort());

        MetricReader metricReader = PeriodicMetricReader.builder(
            OtlpGrpcMetricExporter.builder()
                .setEndpoint(metricsEndpoint)
                .build())
            .setInterval(5, TimeUnit.SECONDS) // send metrics every 5s
            .build();

        OpenTelemetrySdk.builder()
            .setMeterProvider(
                SdkMeterProvider.builder()
                    .setResource(Resource.getDefault()
                        .toBuilder()
                        .put("service.name", "AWSJDBCWrapperIntegrationTests")
                        .build())
                    .registerMetricReader(metricReader)
                    .build())
            .buildAndRegisterGlobal();

      } catch (Exception ex) {
        throw new RuntimeException("Error initializing XRay.", ex);
      }
    }

    return environment;
  }

  private static void initProxies(TestEnvironment environment) {
    environment.proxies = new HashMap<>();

    int proxyControlPort = environment.info.getProxyDatabaseInfo().getControlPort();
    for (TestInstanceInfo instance : environment.info.getProxyDatabaseInfo().getInstances()) {
      ToxiproxyClient client = new ToxiproxyClient(instance.getHost(), proxyControlPort);
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
        throw new UnsupportedOperationException(testDriver.toString());
    }

    if (disabledByFeature || !driverCompatibleToDatabaseEngine) {
      // this driver is disabled
      return false;
    }
    return true;
  }
}
