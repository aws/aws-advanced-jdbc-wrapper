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
import integration.DatabaseEngine;
import integration.TestDatabaseInfo;
import integration.TestEnvironmentFeatures;
import integration.TestEnvironmentInfo;
import integration.TestInstanceInfo;
import integration.orchestra.OrchestraEnvironmentInfo;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.MetricReader;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import software.amazon.jdbc.Driver;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.orchestra.client.NetworkControlClient;

public class TestEnvironment {

  private static TestEnvironment env;

  private TestEnvironmentInfo info;

  /**
   * Gateway destination names, populated when the environment provisioned network impairment.
   *
   * <p>Names and nothing else. A gateway selector is a filter rule the gateway holds, addressed by name, so
   * there is no client-side object to keep - which is why this is a set of strings rather than a map of
   * proxy handles.
   */
  private java.util.LinkedHashSet<String> gatewaySelectors;

  /**
   * The impairable destinations of each region, on a global database.
   *
   * <p>Kept apart from {@link #gatewaySelectors} rather than merged into it, and that separation is the whole
   * design. {@code ProxyHelper.disableAllConnectivity} iterates the flat set, so adding a global database's other
   * regions to it would quietly turn every existing "cut the network" test into "cut every region at once" -
   * including the failover tests, which would then be measuring something else entirely. The flat set stays what
   * it has always been, the region the suite connects to, and cross-region impairment is asked for by name.
   */
  private java.util.LinkedHashMap<String, java.util.LinkedHashSet<String>> gatewaySelectorsByRegion;
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

    // The environment is described by ORCHESTRA_CONTEXT, which the engine sets on this container. There was
    // a second path here while the legacy harness still existed, reading its TEST_ENV_INFO_JSON; the ~437
    // call sites below getCurrent() never saw the difference, because they read this same facade.
    environment.info = OrchestraEnvironmentInfo.read();

    if (environment
        .info
        .getRequest()
        .getFeatures()
        .contains(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED)) {
      initGatewaySelectors(environment);

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

  /**
   * Registers destination selectors with Orchestra's gateway, one per endpoint the tests can impair.
   *
   * <p>Runs container-side, which is later than the equivalent step used to happen. The retired harness
   * created a proxy per database host while it built the environment, because each proxy needed a listener
   * for the tests to connect to; a gateway selector is only a filter rule the gateway already routing
   * traffic will apply, so nothing needs it until a test wants to impair something. Here, where the
   * endpoints are already known, is the natural place to declare them.
   *
   * <p>One control endpoint serves the whole composition, so a single client registers every selector.
   *
   * <p>Names must be what the tests look up. {@code ProxyHelper.disableConnectivity} is called with an
   * instance id in the failover tests and with a cluster endpoint hostname in
   * {@code AuroraTestUtility.simulateTemporaryFailure}, so both are registered under those names.
   */
  private static void initGatewaySelectors(TestEnvironment environment) {
    environment.gatewaySelectors = new LinkedHashSet<>();

    final NetworkControlClient client = NetworkControlClient.fromEnvironment();
    final TestDatabaseInfo database = environment.info.getDatabaseInfo();

    try {
      for (TestInstanceInfo instance : database.getInstances()) {
        registerEndpoint(
            client, environment, instance.getInstanceId(), instance.getHost(), instance.getPort());
      }

      // The cluster endpoints are registered under their own hostnames because that is how
      // simulateTemporaryFailure asks for them. Selecting a destination by name rather than by listener is
      // what lets the same endpoint be impaired without any client being reconfigured.
      registerEndpoint(client, environment,
          database.getClusterEndpoint(), database.getClusterEndpoint(), database.getClusterEndpointPort());
      registerEndpoint(client, environment,
          database.getClusterReadOnlyEndpoint(), database.getClusterReadOnlyEndpoint(),
          database.getClusterReadOnlyEndpointPort());

      registerGlobalDatabaseRegions(client, environment);

    } catch (IOException e) {
      throw new RuntimeException(
          "Could not register destination selectors with Orchestra's gateway, so no test can impair "
              + "network traffic.", e);
    }
  }

  /**
   * Registers every region of a global database, so a whole region can be made unreachable.
   *
   * <p>The gap this closes. A cross-region deployment has a failure mode a single-region one does not - the
   * database is healthy and the network is not - and until now the suite could only express that as
   * configuration, by telling the driver which regions it may use. That tests whether the driver obeys an
   * instruction, not what it does when a region genuinely stops answering.
   *
   * <p>Only the selector <em>names</em> are new. One gateway already carries every packet the container sends,
   * secondary regions included, because it is the container's default route - so nothing had to be added to the
   * network path. What was missing was a name for those destinations to impair them by.
   *
   * <p>Registering a selector is inert until a test impairs it: the gateway turns selectors into firewall rules
   * only where a toxic is attached, so this adds names and changes no traffic.
   *
   * <p>The primary region's instances are registered a second time here under the same names, which is harmless -
   * registration replaces by name - and is what keeps this loop simple enough to read.
   */
  private static void registerGlobalDatabaseRegions(
      final NetworkControlClient client, final TestEnvironment environment) throws IOException {

    environment.gatewaySelectorsByRegion = new java.util.LinkedHashMap<>();

    final integration.TestGlobalDatabaseInfo global = environment.info.getGlobalDatabaseInfo();
    if (global == null) {
      return;
    }

    for (final integration.TestRegionalClusterInfo cluster : global.getRegions()) {
      final java.util.LinkedHashSet<String> names = new java.util.LinkedHashSet<>();

      for (final String instanceIdentifier : cluster.getInstanceIdentifiers()) {
        registerRegionalEndpoint(client, names, instanceIdentifier,
            cluster.getInstanceEndpoint(instanceIdentifier), cluster.getPort());
      }

      // Both cluster endpoints too, because cutting a region has to include the addresses a client would
      // reconnect through. Leaving them reachable would impair the instances and leave the region's front door
      // open, which is not a failure mode that happens.
      registerRegionalEndpoint(
          client, names, cluster.getClusterEndpoint(), cluster.getClusterEndpoint(), cluster.getPort());
      registerRegionalEndpoint(client, names,
          cluster.getClusterReadOnlyEndpoint(), cluster.getClusterReadOnlyEndpoint(), cluster.getPort());

      environment.gatewaySelectorsByRegion.put(cluster.getRegion(), names);
    }
  }

  /** Registers one destination and records its name against a region rather than in the flat set. */
  private static void registerRegionalEndpoint(
      final NetworkControlClient client,
      final java.util.Set<String> names,
      final String name,
      final String host,
      final int port) throws IOException {

    if (StringUtils.isNullOrEmpty(host) || StringUtils.isNullOrEmpty(name)) {
      return;
    }
    client.register(name, host + ":" + port);
    names.add(name);
  }

  /**
   * Registers one impairable destination under the name tests look it up by.
   *
   * <p>The name is an instance id for the failover tests and a cluster endpoint hostname for
   * {@code simulateTemporaryFailure}, which is why it is passed separately from the host: for an instance
   * the two differ.
   */
  private static void registerEndpoint(
      final NetworkControlClient client,
      final TestEnvironment environment,
      final String name,
      final String host,
      final int port) throws IOException {

    if (StringUtils.isNullOrEmpty(host)) {
      return;
    }
    client.register(name, host + ":" + port);
    environment.gatewaySelectors.add(name);
  }

  /**
   * Returns the names of every impairable destination.
   *
   * <p>What {@code ProxyHelper}'s "all connectivity" and "all latencies" operations iterate. Names rather
   * than proxy handles, because the gateway is addressed by destination name over its control API and there
   * is no client-side object to hand out.
   *
   * @return the destination names, empty when this environment provisioned no impairment
   */
  public java.util.Collection<String> getProxyNames() {
    return this.gatewaySelectors == null
        ? Collections.emptyList()
        : Collections.unmodifiableCollection(this.gatewaySelectors);
  }

  /**
   * Returns the impairable destinations of one region of a global database.
   *
   * <p>Separate from {@link #getProxyNames()} on purpose; see {@link #gatewaySelectorsByRegion}.
   *
   * @param region the AWS region
   * @return that region's destination names, empty when the region is unknown or this is not a global database
   */
  public java.util.Collection<String> getProxyNames(final String region) {
    if (this.gatewaySelectorsByRegion == null) {
      return Collections.emptyList();
    }
    final java.util.Set<String> names = this.gatewaySelectorsByRegion.get(region);
    return names == null ? Collections.emptyList() : Collections.unmodifiableCollection(names);
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

    final EnumSet<TestEnvironmentFeatures> features = this.info.getRequest().getFeatures();
    final DatabaseEngine databaseEngine = this.info.getRequest().getDatabaseEngine();

    switch (testDriver) {
      case MYSQL:
        driverCompatibleToDatabaseEngine = databaseEngine == DatabaseEngine.MYSQL;
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

    // the driver is disabled when a feature disables it or it is incompatible with the engine
    return !disabledByFeature && driverCompatibleToDatabaseEngine;
  }
}
