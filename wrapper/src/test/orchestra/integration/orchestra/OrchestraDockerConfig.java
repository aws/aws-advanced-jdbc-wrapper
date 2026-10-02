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

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import software.amazon.orchestra.instruments.docker.MySqlContainerConfiguration;
import software.amazon.orchestra.instruments.docker.PostgresContainerConfiguration;

/**
 * Configuration for a Docker-only run: a database in a container, no AWS.
 *
 * <p>The environment the harness calls {@code DOCKER} and drives with {@code test-all-docker}. It matters to
 * the Toxiproxy retirement out of proportion to its size, because it is the first environment kind that can
 * be migrated without an AWS account - so the loop is minutes rather than the hour a cluster costs.
 *
 * <h2>Why this extends the Aurora configuration</h2>
 *
 * <p>For the container plumbing, and only that: the image, the working directory, the fifteen copied paths,
 * the bound output directories, the forwarded system properties and the driver jar are identical for every
 * environment kind, and that code is verified. Extending reuses it without editing it.
 *
 * <p>The AWS-shaped methods it inherits are inert here rather than wrong. The Docker composition binds no
 * AWS instrument and registers neither the security-group whitelist nor the orphan sweep, so nothing ever
 * calls {@code getAuroraPassword} or {@code getAwsRegion}. The two that would have had an effect are
 * overridden below.
 *
 * <p>The tidier shape is a shared base class with the container plumbing and two siblings beside it. That is
 * a pure move of working code and worth doing, but it is not what makes Docker environments run, so it is
 * deliberately not bundled into the change that does.
 */
public class OrchestraDockerConfig extends OrchestraAuroraConfig
    implements PostgresContainerConfiguration, MySqlContainerConfiguration {

  /**
   * Creates the configuration for a Docker run.
   *
   * @param gradleTask the task to run inside the container, normally {@code in-container}
   */
  public OrchestraDockerConfig(final String gradleTask) {
    super(gradleTask);
  }

  /**
   * Returns no environment variables for the container.
   *
   * <p>Overridden because the inherited version resolves AWS credentials, and this is the one inherited
   * method that would actually do something unwanted: it would reach for a credential provider, and either
   * fail or waste time, in a composition that touches no AWS service at all.
   */
  @Override
  public Map<String, String> getTestContainerEnvironment() {
    return Collections.emptyMap();
  }

  /**
   * Returns one instance.
   *
   * <p>Overridden because {@link TestShapeInstrumentDefinition} reads the instance count from
   * {@code AuroraClusterConfiguration} - one value, so the shape and the database cannot disagree - and this
   * composition provisions a single container. Reporting the inherited two would make
   * {@code @EnableOnNumOfInstances} gate on a topology that does not exist, and the in-container tests
   * believe the shape.
   *
   * <p>Multi-instance Docker follows the four-cache pattern: one role per container, with a variation
   * binding them. That is the next increment, not this one.
   */
  @Override
  public int getAuroraInstanceCount() {
    return 1;
  }

  /**
   * Returns the PostgreSQL image to run.
   *
   * <p>Pinned, where the harness used {@code postgres:latest}. A moving tag means a run can change behaviour
   * because a registry tag moved rather than because the driver did, and a failure attributed to the wrong
   * cause is worse than an old image. 16 is a version Aurora PostgreSQL also offers, which keeps the Docker
   * and Aurora environments comparable.
   */
  @Override
  public String getPostgresImage() {
    return "postgres:16-alpine";
  }

  /**
   * Starts PostgreSQL with two-phase commit enabled.
   *
   * <p>{@code max_prepared_transactions} defaults to zero, which disables prepared transactions, and it is a
   * static parameter - so it has to be set on the command line rather than by an init script. Without it
   * {@code XaTestUtility.assumePreparedTransactionsSupported} skips every test that prepares a branch, and
   * this composition is the pull request gate: the XA tests would be checked on no pull request and only on
   * the scheduled Aurora runs, while still appearing in the report as part of the gate's suite.
   *
   * <p>The retired harness had the same gap. It started {@code postgres:latest} with no arguments and set
   * {@code max_prepared_transactions} only on the RDS deployments, through a parameter group; the Docker XA
   * prepare tests skipped there too. This is a difference from the harness rather than a reproduction of it,
   * which is why it is worth stating: the value matches what the Aurora path sets, so a test that prepares a
   * branch now behaves the same in both environments instead of being silently absent from one.
   *
   * <p>{@code fsync=off} is named again because a command replaces the image's own rather than adding to it,
   * and that is what Testcontainers would otherwise have set. It is the same trade the Postgis instrument
   * makes: durability is worthless in a container deleted at the end of the run, and the writes are the
   * slowest part of the suite.
   */
  @Override
  public List<String> getPostgresCommand() {
    return Arrays.asList(
        "postgres",
        "-c", "fsync=off",
        "-c", "max_prepared_transactions=100");
  }

  /**
   * Returns the database the container creates.
   *
   * <p>The same name the Aurora path uses. Nothing requires them to match - the in-container tests read the
   * database name from the context rather than assuming it - but keeping them equal means a test log reads
   * the same whichever environment produced it.
   */
  @Override
  public String getPostgresDatabaseName() {
    return "test_database";
  }

  @Override
  public String getPostgresUsername() {
    return "test_user";
  }

  @Override
  public String getPostgresPassword() {
    return "test_password";
  }

  /**
   * Returns the in-container properties, with the caching tests selected or excluded.
   *
   * <p>The tag is what separates the two Docker jobs in the harness: {@code test-all-docker} excludes
   * {@code caching} and {@code test-all-caching} includes it. Without it, a run with no caches would still
   * enumerate the cache tests and fail them on an empty cache list, and a caching run would spend its time on
   * the rest of the suite as well.
   *
   * <p>{@code putIfAbsent}, so a tag named explicitly on the command line still wins - the inherited method has
   * already forwarded any. Someone asking for a specific tag is answering a narrower question than the mode's
   * default.
   */
  @Override
  public Map<String, String> getTestContainerSystemProperties() {
    final Map<String, String> properties =
        new LinkedHashMap<>(super.getTestContainerSystemProperties());

    properties.putIfAbsent(
        OrchestraDockerRunner.caching() ? "test-include-tags" : "test-exclude-tags", "caching");

    return properties;
  }

  /**
   * Returns the MySQL image to run.
   *
   * <p>Pinned for the same reason the PostgreSQL one is, and to the version Aurora MySQL is compatible with, so
   * the Docker and Aurora environments stay comparable. The harness used a moving tag.
   */
  @Override
  public String getMySqlImage() {
    return "mysql:8.4";
  }

  /**
   * Returns the same database name, user and password as the PostgreSQL container.
   *
   * <p>Deliberately identical across engines. Nothing requires it - the in-container tests read these from the
   * context rather than assuming them - but it means a test log reads the same whichever engine produced it,
   * and an engine axis is then the only difference between two slots.
   */
  @Override
  public String getMySqlDatabaseName() {
    return "test_database";
  }

  @Override
  public String getMySqlUsername() {
    return "test_user";
  }

  @Override
  public String getMySqlPassword() {
    return "test_password";
  }

  /**
   * Grants the test user the privilege {@code XA RECOVER} needs.
   *
   * <p>The retired harness did this by copying a shell script into the container's init directory, and
   * leaving it out of this composition was not a silent loss of coverage - it was a hang.
   * {@code XaTransactionTest.test_recover_returnsPreparedBranch} prepares a branch and then calls
   * {@code XAResource.recover}, which MySQL 8 refuses without {@code XA_RECOVER_ADMIN}; the branch stays
   * prepared, holding a metadata lock on the table, and the next test's {@code DROP TABLE} waits for it.
   * MySQL's {@code lock_wait_timeout} defaults to a year, so the run does not fail - it stops, with the
   * worker blocked in a socket read and nothing in the log to say why.
   *
   * <p>SQL rather than the harness's shell script, which needed {@code MYSQL_ROOT_PASSWORD} to log in as
   * root. The image pipes {@code .sql} files through a client that is already root, so the privilege that
   * has to be granted is the only thing left to say.
   */
  @Override
  public Map<String, String> getMySqlInitScripts() {
    return Collections.singletonMap(
        "01-grant-xa-recover.sql",
        "GRANT XA_RECOVER_ADMIN ON *.* TO '" + getMySqlUsername() + "'@'%';\nFLUSH PRIVILEGES;\n");
  }
}
