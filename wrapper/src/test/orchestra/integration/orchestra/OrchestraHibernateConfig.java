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

import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import software.amazon.orchestra.instruments.docker.PostgisContainerConfiguration;

/**
 * Configuration for the Hibernate ORM run: a Postgis database, and a container holding Hibernate's own
 * checkout.
 *
 * <p>The migrated equivalent of the harness's {@code test-hibernate-only}, and the one mode where "run the
 * integration tests" is the wrong description of what happens. Nothing in this repository's test suite runs.
 * The container clones Hibernate at a pinned tag, puts the wrapper on its driver path, and runs
 * <em>Hibernate's</em> test suite against a database reached through the wrapper - so what is under test is
 * whether an ORM's own conformance suite passes when its JDBC driver is this one.
 *
 * <p>That is why it is a separate composition rather than a suite selection. Every other mode varies which of
 * our tests run; this one replaces the workload. Two things follow, and both are reproduced below: the
 * database has to be Postgis rather than plain Postgres, because Hibernate's suite maps geometry and vector
 * types, and the credentials are fixed to {@code hibernate_orm_test} because Hibernate's own build
 * configuration names them.
 *
 * <h2>Why it extends the Docker configuration</h2>
 *
 * <p>For the parts that do not change: the JVM image, the gateway, and the absence of AWS. What it replaces is
 * everything about the container's contents - the build steps, the copies, the binds and the working
 * directory - because none of the wrapper's own suite is present here. The inherited Postgres settings are
 * inert: this composition binds the Postgis instrument to the {@code Database} role and never provisions a
 * plain Postgres container.
 */
public class OrchestraHibernateConfig extends OrchestraDockerConfig implements PostgisContainerConfiguration {

  /**
   * The Hibernate tag to clone, matching the harness's {@code HIBERNATE_VERSION}.
   *
   * <p>Pinned rather than tracking a branch, and the override files below are pinned with it: they patch four
   * of Hibernate's own sources and its build scripts, so they are only known to apply to this tag. Following
   * {@code main} would turn an unrelated upstream edit into a failure in this repository.
   */
  private static final String HIBERNATE_VERSION = "7.3.0";

  /** Where the clone lives, and therefore where its Gradle build has to run. */
  static final String HIBERNATE_DIRECTORY = "/app/hibernate-orm";

  /** Where Hibernate's build looks for the JDBC drivers it tests against. */
  private static final String DRIVERS_DIRECTORY = HIBERNATE_DIRECTORY + "/drivers";

  /** Hibernate's own sources, which the files below patch. */
  private static final String CORE_MAIN = HIBERNATE_DIRECTORY + "/hibernate-core/src/main/java/org/hibernate";

  /** Hibernate's own tests, likewise. */
  private static final String CORE_TEST =
      HIBERNATE_DIRECTORY + "/hibernate-core/src/test/java/org/hibernate/orm/test";

  /** Where the host's copies of those overrides live. */
  private static final Path FILES = MODULE.resolve("src/test/resources/hibernate_files");

  /**
   * The database, user and password, all three the same value.
   *
   * <p>Not a choice. Hibernate's build reads them from the {@code pg_amazon_ci} profile in
   * {@code local.databases.gradle}, which is one of the files copied in below, and that profile names
   * {@code hibernate_orm_test}. The harness forces the same three values for the same reason.
   */
  private static final String HIBERNATE_DATABASE = "hibernate_orm_test";

  /**
   * Creates the configuration for a Hibernate run.
   *
   * @param gradleTask the task to run in Hibernate's build, normally {@code test}
   */
  public OrchestraHibernateConfig(final String gradleTask) {
    super(gradleTask);
  }

  /**
   * Returns the build steps that turn a JDK image into one holding Hibernate's checkout.
   *
   * <p>At image-build time rather than after the container starts, which is not a preference. The container
   * sits on the client network with its egress routed through the gateway, so a clone performed later would
   * depend on the network path the composition exists to control, and it would be repeated on every run
   * rather than cached in a layer. The harness clones in its Dockerfile for the same reason.
   *
   * <p>{@code bash} because {@code collect_test_results.sh} is a bash script and the image is Alpine, whose
   * default shell is not. {@code git} because nothing in a JDK image can clone.
   *
   * <p>The harness follows its clone with two {@code rm -f} steps that delete shaded bundle jars from the
   * driver directories. Those are unnecessary here: it copied all of {@code build/libs}, which accumulates
   * bundles, while this copies the one jar the build just produced - the same reason
   * {@code OrchestraAuroraConfig} names a single jar.
   */
  @Override
  public List<String> getTestContainerBuildSteps() {
    return List.of(
        "RUN apk add --no-cache --upgrade bash git",
        "RUN git clone --depth 1 --branch " + HIBERNATE_VERSION
            + " https://github.com/hibernate/hibernate-orm.git " + HIBERNATE_DIRECTORY);
  }

  /**
   * Returns everything copied into the container: the driver, Hibernate's patches, and the results script.
   *
   * <p>None of the wrapper's own suite. No compiled test classes, no {@code build.gradle.kts}, no Orchestra
   * client jars - the code running here is Hibernate's, and it reads its configuration from Gradle properties
   * rather than from a composition context.
   *
   * <p>The seven overrides are the harness's, and each exists because the pinned Hibernate tag will not
   * otherwise build or pass on this setup:
   *
   * <ul>
   *   <li>{@code gradle.properties} lowers the minimum JDK from 25 to 17, so the suite can run on the JVMs
   *       this repository supports.
   *   <li>{@code settings.gradle} pins Derby to 10.16.1.1, the last release that runs on Java 17.
   *   <li>{@code local.databases.gradle} supplies the {@code pg_amazon_ci} profile, which is what points
   *       Hibernate at the wrapper as its driver.
   *   <li>the four Java files patch Hibernate sources and tests around interval and struct-array handling.
   * </ul>
   *
   * <p>Copied over the clone rather than applied as a patch: a patch that no longer applies fails obscurely
   * mid-build, where a copy either lands or reports a missing source before anything starts.
   */
  @Override
  public Map<Path, String> getTestContainerCopies() {
    final Map<Path, String> copies = new LinkedHashMap<>();

    // The wrapper itself, as the driver Hibernate's suite connects through. The whole point of the run.
    copies.put(driverJar(), DRIVERS_DIRECTORY + "/aws-advanced-jdbc-wrapper.jar");

    copies.put(FILES.resolve("gradle.properties"), HIBERNATE_DIRECTORY + "/gradle.properties");
    copies.put(FILES.resolve("settings.gradle"), HIBERNATE_DIRECTORY + "/settings.gradle");
    copies.put(FILES.resolve("local.databases.gradle"),
        HIBERNATE_DIRECTORY + "/local-build-plugins/src/main/groovy/local.databases.gradle");

    copies.put(FILES.resolve("PostgreSQLCastingIntervalSecondJdbcType.java"),
        CORE_MAIN + "/dialect/type/PostgreSQLCastingIntervalSecondJdbcType.java");
    copies.put(FILES.resolve("DataSourceTest.java"),
        CORE_TEST + "/datasource/DataSourceTest.java");
    copies.put(FILES.resolve("StructEmbeddableArrayTest.java"),
        CORE_TEST + "/mapping/embeddable/StructEmbeddableArrayTest.java");
    copies.put(FILES.resolve("PostgresIntervalSecondTest.java"),
        CORE_TEST + "/type/PostgresIntervalSecondTest.java");

    // Outside the clone, because it collects from all of it. See HibernateSuiteRun.
    copies.put(FILES.resolve("collect_test_results.sh"), "/app/collect_test_results.sh");

    return copies;
  }

  /**
   * Returns the one directory that has to outlive the container.
   *
   * <p>Where {@code collect_test_results.sh} writes its archives, which is the only output of this run. Its
   * paths are absolute - {@code /app/build/test-results} inside the container - so this bind is what makes the
   * results reachable on the host rather than discarded with the container.
   *
   * <p>The host side is a directory of its own rather than {@code build/test-results}, which every other
   * composition here binds. Sharing it cost a validated run: this task was started while an Aurora run was
   * midway through its suite, both bound the same host directory, and a Gradle {@code Test} task deletes
   * {@code build/test-results} before it runs - so the Aurora container's results directory disappeared from
   * under it and its build died with {@code FileNotFoundException:
   * /app/build/test-results/in-container/binary/output.bin.idx} after the test itself had passed. A separate
   * directory means this run's archives can never be what breaks another run's reporting.
   *
   * <p>It does not make concurrent runs safe, and nothing here can: the deletion above is applied to every
   * {@code Test} task in this build, so starting any of these tasks clears that directory whatever this one
   * binds. One Orchestra run at a time per checkout.
   *
   * <p>The inherited binds are deliberately not kept. Two are for a suite that does not run here, and
   * {@code /app/gradle} would bind this repository's Gradle wrapper directory next to a checkout that ships
   * its own.
   */
  @Override
  public Map<Path, String> getTestContainerBinds() {
    return Collections.singletonMap(
        outputDirectory("build/test-results-hibernate"), "/app/build/test-results");
  }

  /**
   * Returns Hibernate's checkout as the working directory.
   *
   * <p>So {@code ./gradlew} resolves to Hibernate's wrapper rather than to anything of ours, which is also
   * what lets {@link HibernateSuiteRun} run the suite without naming a directory.
   */
  @Override
  public String getTestContainerWorkingDirectory() {
    return HIBERNATE_DIRECTORY;
  }

  /**
   * Returns no forwarded system properties.
   *
   * <p>The inherited ones select tests in <em>our</em> suite - tag filters, shard coordinates,
   * {@code test-classes} - and Hibernate's build has never heard of them. {@link HibernateSuiteRun} composes
   * the properties this run needs, because the important ones name a database that only exists once the
   * composition is provisioned.
   */
  @Override
  public Map<String, String> getTestContainerSystemProperties() {
    return Collections.emptyMap();
  }

  @Override
  public String getPostgisDatabaseName() {
    return HIBERNATE_DATABASE;
  }

  @Override
  public String getPostgisUsername() {
    return HIBERNATE_DATABASE;
  }

  @Override
  public String getPostgisPassword() {
    return HIBERNATE_DATABASE;
  }
}
