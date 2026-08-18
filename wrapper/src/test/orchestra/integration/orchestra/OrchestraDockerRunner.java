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

package integration.orchestra;

import integration.DatabaseEngine;
import integration.DatabaseEngineDeployment;
import integration.TargetJvm;
import integration.TestEnvironmentFeatures;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import software.amazon.orchestra.EnvComposition;
import software.amazon.orchestra.EnvCompositionBuilder;
import software.amazon.orchestra.OnError;
import software.amazon.orchestra.contract.Database;
import software.amazon.orchestra.contract.NetworkGateway;
import software.amazon.orchestra.instruments.docker.DockerNetworkInstrumentDefinition;
import software.amazon.orchestra.instruments.docker.GradleTestContainerRun;
import software.amazon.orchestra.instruments.docker.JavaTestContainerInstrumentDefinition;
import software.amazon.orchestra.instruments.network.TransparentNetworkGatewayInstrumentDefinition;

/**
 * Runs the in-container suite against a database container, with no AWS involved.
 *
 * <p>The migrated equivalent of {@code test-all-docker}, and it was the first step in retiring Toxiproxy
 * rather than merely working around it: Toxiproxy's last user was the legacy harness, and the harness could
 * not be retired until every environment kind it provided existed here. This was the first of those, and the
 * only one that needs no cloud account.
 *
 * <p>That last point is the reason to start here. An Aurora composition costs an hour a run, most of it
 * waiting for RDS, which made every mistake in this migration expensive to find. A Docker composition
 * provisions in under a minute, so the parts common to every environment - the context crossing into the
 * container, the gateway routing, the in-container build - can be exercised quickly and cheaply.
 *
 * <h2>What it declares, and what it cannot</h2>
 *
 * <p>{@code NETWORK_OUTAGES_ENABLED}, because the gateway is present and impairment works the same way here
 * as against a cluster: by destination, through Orchestra's control API.
 *
 * <p>No {@code FAILOVER_SUPPORTED}: a single container has nothing to fail over to, which is also why the
 * harness's Docker environments never claimed it. No {@code IAM}, no {@code AWS_CREDENTIALS_ENABLED} and no
 * {@code SECRETS_MANAGER}, since there is no AWS in this composition at all - declaring them would advertise
 * capabilities that would fail the moment a test used them.
 *
 * <p>Telemetry is present, and it is the one capability where this composition is <em>less</em>
 * AWS-dependent than the harness rather than equally so. The harness grants a Docker environment
 * {@code AWS_CREDENTIALS_ENABLED} for no reason other than telemetry: its collector exports to X-Ray and
 * CloudWatch, which needs credentials a Docker run would otherwise have no use for. Here the daemon runs in
 * local mode and the collector logs what it receives, so both backends run without credentials and this task
 * stays free.
 */
@Tag("orchestra")
@Timeout(value = 60, unit = TimeUnit.MINUTES)
public class OrchestraDockerRunner {

  /** The task the in-container Gradle build runs, unchanged from the harness. */
  private static final String IN_CONTAINER_TASK = "in-container";

  /**
   * What a Docker environment genuinely provides.
   *
   * <p>Deliberately short. The omissions are the point: everything AWS-shaped is absent because there is no
   * AWS here, and failover is absent because one container cannot fail over.
   */
  private static final EnumSet<TestEnvironmentFeatures> FEATURES = EnumSet.of(
      TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
      TestEnvironmentFeatures.HIKARI);

  /**
   * Reports whether this run exercises the Valkey caches, from {@code -Dorchestra-caching}.
   *
   * <p>A mode rather than part of the default composition, because that is how the harness splits it:
   * {@code test-all-docker} excludes the {@code caching} tag and {@code test-all-caching} includes it, and the
   * two are separate CI jobs over the same engine and JVM matrix. Four extra containers on every Docker run
   * would also be paid for by the runs that exclude those tests.
   *
   * <p>Docker only, and that asymmetry is the harness's: {@code test-all-caching} sets {@code test-no-aurora},
   * so the cache tests never ran against a cluster there. This repository's Aurora composition carries the
   * caches unconditionally and its runs execute those tests, which is broader rather than different - so
   * nothing here narrows it.
   *
   * @return {@code true} to provision the four caches and select their tests
   */
  static boolean caching() {
    return Boolean.parseBoolean(System.getProperty("orchestra-caching", "false"));
  }

  /**
   * Returns the features for this run: {@link #FEATURES} plus telemetry, plus the caches when asked for.
   *
   * <p>Resolved once per run and passed to both the shape and the composition, so a shape cannot declare a
   * capability the composition did not provision - which for the caches would surface in the container as an
   * empty cache list rather than as a missing instrument.
   *
   * @return the feature set to publish
   */
  private static EnumSet<TestEnvironmentFeatures> features() {
    final EnumSet<TestEnvironmentFeatures> features = EnumSet.copyOf(FEATURES);
    features.addAll(TelemetryBackends.requested());

    if (caching()) {
      features.add(TestEnvironmentFeatures.VALKEY_CACHE);
    }
    return features;
  }

  @Test
  @DisplayName("runs the in-container suite against a database container, without AWS")
  public void runTests() throws Exception {
    // Rejected rather than ignored. Every suite the Aurora runner offers enables only on AWS deployments -
    // the harness's own tasks all set test-no-docker - so a suite asked for here could not run its class.
    // Ignoring the property would provision a container, run the ordinary suite, and look like the suite had
    // passed.
    if (!TestSuite.requested().isStandard()) {
      throw new IllegalArgumentException(
          "orchestra-suite=" + TestSuite.requested() + " is not available on the Docker task: its test "
              + "class enables on AWS deployments only. Use orchestra-test-pg-aurora.");
    }

    final OrchestraDockerConfig configuration = new OrchestraDockerConfig(IN_CONTAINER_TASK);
    final EnumSet<TestEnvironmentFeatures> features = features();

    // Which database servers to run. The harness's Docker matrix is over the server, and it is the PR gate, so
    // a task that could only start PostgreSQL would quietly narrow what every pull request is checked against.
    final DatabaseEngine[] engines = ContainerEngineVariation.requested();

    final EnvCompositionBuilder builder = EnvComposition.getBuilder()
        // No SecurityGroupIpWhitelist and no AwsOrphanCleanup. Both are AWS global resources, and a
        // composition that provisions nothing in AWS has nothing to whitelist and nothing to reclaim.
        .addInstrumentDefinition(new DockerNetworkInstrumentDefinition())
        .addInstrument(NetworkGateway.class, new TransparentNetworkGatewayInstrumentDefinition())
        // The database, under the same role an Aurora cluster takes. That is what lets the in-container
        // facade read either without knowing which it got.
        //
        // A default that ContainerEngineVariation rebinds per slot. The mapping is the variation's so that the
        // default and the varied slots cannot disagree about which container an engine means.
        .addInstrument(Database.class, ContainerEngineVariation.databaseFor(engines[0]))
        .addInstrument(TestShape.class, new TestShapeInstrumentDefinition(
            engines[0],
            DatabaseEngineDeployment.DOCKER,
            // No IAM user: there is no IAM. The shape carries null rather than a name that could not
            // authenticate anywhere.
            null,
            features))
        .addInstrumentDefinition(new JavaTestContainerInstrumentDefinition())
        // The JVM axis, which applies here as much as it does to a cluster: the harness varies the JVM for
        // Docker environments too, and this is the cheapest place to exercise it - a container provisions in
        // under a minute, where a cluster costs an hour.
        // The engine axis, and the JVM axis beside it: the harness varies both for Docker environments, and
        // this is the cheapest place to exercise either.
        .addVariation(new ContainerEngineVariation(engines, features))
        .addVariation(new JvmVariation(JvmVariation.requested()));

    if (caching()) {
      // The four caches the query-cache and Spring caching tests address by index. A variation rather than four
      // bare instruments because that is where Orchestra exposes role-scoped configuration, which is what
      // caches differing only in authentication and TLS need.
      builder.addVariation(ValkeyCachesVariation.standard());
    }

    // Where the wrapper's traces and metrics go. On by default here too: the telemetry plugins are the same
    // ones in either environment, and this is the cheap place to exercise them.
    TelemetryBackends.addTo(builder, features);

    builder
        .configureExecutionPipeline(e -> e
            // CONTINUE once the JVM axis produces more than one slot, for the reason the Aurora runner gives:
            // the question a matrix answers is which combinations work, so every slot runs and the build
            // still fails at the end. A single slot should still fail fast.
            .onError(JvmVariation.requested().length > 1 ? OnError.CONTINUE : OnError.FAIL)
            .useComposition(new GradleTestContainerRun(configuration.getGradleTask())))
        .build()
        .run(configuration);
  }
}
