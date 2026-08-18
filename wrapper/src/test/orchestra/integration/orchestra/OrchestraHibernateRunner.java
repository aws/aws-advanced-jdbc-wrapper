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

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import software.amazon.orchestra.EnvComposition;
import software.amazon.orchestra.OnError;
import software.amazon.orchestra.contract.Database;
import software.amazon.orchestra.contract.NetworkGateway;
import software.amazon.orchestra.instruments.docker.DockerNetworkInstrumentDefinition;
import software.amazon.orchestra.instruments.docker.JavaTestContainerInstrumentDefinition;
import software.amazon.orchestra.instruments.docker.PostgisContainerInstrumentDefinition;
import software.amazon.orchestra.instruments.network.TransparentNetworkGatewayInstrumentDefinition;

/**
 * Runs Hibernate ORM's own test suite against a Postgis container, with the wrapper as its JDBC driver.
 *
 * <p>The migrated equivalent of {@code test-hibernate-only}, and a separate runner because it is a separate
 * composition rather than a variation on one. The harness expresses the same thing as a feature -
 * {@code RUN_HIBERNATE_TESTS_ONLY} - but that feature does not select tests: it makes the harness skip its
 * in-container suite entirely and build a different container to run somebody else's. Modelling that as a
 * suite would mean a mode that silently redefines what the runner runs.
 *
 * <h2>What is being tested</h2>
 *
 * <p>Not the wrapper's tests. An ORM's conformance suite, on the theory that the fastest way to find out
 * whether a JDBC driver is a faithful one is to run a demanding client's own tests through it. So a failure
 * here names a Hibernate test, and the question it raises is which driver behaviour that test depends on.
 *
 * <h2>Why the database is Postgis</h2>
 *
 * <p>Because Hibernate's suite maps types a plain Postgres image cannot provide: geometry, and vectors through
 * pgvector. The harness reached the same place by building its own postgis image; Orchestra's Postgis
 * instrument does it as an instrument, publishing the same {@code Database} role and the same state, so
 * nothing else in the composition changes.
 *
 * <h2>No AWS, and no shape</h2>
 *
 * <p>The harness's task sets {@code test-no-aurora} and both Multi-AZ exclusions, so this only ever ran
 * against a container - which makes this the second composition that needs no cloud account.
 *
 * <p>It also publishes no {@code TestShape}. Every other composition here does, because the wrapper's
 * in-container facade rebuilds the harness's {@code TestEnvironmentInfo} from it. Nothing in this container
 * reads a context: Hibernate's build takes its database from Gradle properties, which is what
 * {@link HibernateSuiteRun} passes.
 */
@Tag("orchestra")
// Hours, not minutes. Hibernate's suite is tens of thousands of tests run with --no-parallel, and the first
// run on a machine also pulls a JDK image, builds the clone into it, and resolves Hibernate's entire build
// graph. The harness gives its own equivalent no timeout at all and relies on the CI job's.
@Timeout(value = 8, unit = TimeUnit.HOURS)
public class OrchestraHibernateRunner {

  /**
   * The task in Hibernate's build, not ours.
   *
   * <p>Plain {@code test}, where every other runner here names {@code in-container}. That difference is the
   * whole of what this composition does differently at the end.
   */
  private static final String HIBERNATE_TASK = "test";

  @Test
  @DisplayName("runs Hibernate ORM's own test suite against Postgis, through the wrapper")
  public void runTests() throws Exception {
    final OrchestraHibernateConfig configuration = new OrchestraHibernateConfig(HIBERNATE_TASK);

    EnvComposition.getBuilder()
        // No AWS global resources: nothing here is provisioned in an account, so there is nothing to
        // whitelist and nothing to reclaim.
        .addInstrumentDefinition(new DockerNetworkInstrumentDefinition())
        // Required by the test container, which sits on the client network and reaches the database through
        // it. Hibernate's build also resolves its dependencies over this path, so the gateway is load-bearing
        // here in a way it is not in a suite that only talks to a database.
        .addInstrument(NetworkGateway.class, new TransparentNetworkGatewayInstrumentDefinition())
        // Under the same role a plain container or an Aurora cluster takes, which is what lets the rest of
        // the composition be unaware that this one carries extensions.
        .addInstrument(Database.class, new PostgisContainerInstrumentDefinition())
        .addInstrumentDefinition(new JavaTestContainerInstrumentDefinition())
        .configureExecutionPipeline(e -> e
            // One composition, so a failure should stop the run rather than be collected.
            .onError(OnError.FAIL)
            // Not GradleTestContainerRun: the command names the database, whose hostname is only known once
            // this composition is provisioned. See HibernateSuiteRun.
            .useComposition(new HibernateSuiteRun(configuration.getGradleTask())))
        .build()
        .run(configuration);
  }
}
