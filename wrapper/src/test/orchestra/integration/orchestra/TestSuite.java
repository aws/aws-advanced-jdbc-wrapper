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

import integration.DatabaseEngineDeployment;
import integration.TestEnvironmentFeatures;
import integration.TestTags;
import java.util.EnumSet;
import java.util.Locale;

/**
 * Which suite a run exercises, from {@code -Dorchestra-suite}.
 *
 * <p>The harness's four {@code *_ONLY} flags plus {@code PERFORMANCE}, stated as one choice rather than five
 * independent switches. They are mutually exclusive in fact - each is a dedicated CI workflow, each selects
 * exactly one test class, and every one of them appears in the others' {@code @DisableOnTestFeature} lists -
 * so five booleans would mostly describe combinations that can only produce an empty run.
 *
 * <h2>These narrow rather than add</h2>
 *
 * <p>Each feature here selects its own class through {@code @EnableOnTestFeature} and simultaneously switches
 * off the ordinary suite: about twenty-five classes list these features in {@code @DisableOnTestFeature}, so
 * declaring one turns a 250-test run into a one-class run. That is the harness's behaviour and the reason
 * these are a mode: adding {@code RUN_DB_METRICS_ONLY} to the default feature set would silently delete the
 * migrated suite's coverage.
 *
 * <p>{@code PERFORMANCE} is the same shape with an inverted switch. The harness publishes it unless
 * {@code test-no-performance} is set, and every one of its non-performance tasks sets it - so its effective
 * default is off, which is what {@link #STANDARD} reproduces. Reading the harness's default as "on" would
 * disable the whole ordinary suite on every migrated run.
 *
 * <h2>What each suite carries</h2>
 *
 * <p>Beyond the feature, three things the harness's task bodies encode and the conditions rely on:
 *
 * <ul>
 *   <li>Features to <em>drop</em>, mirroring the {@code test-no-*} properties each task sets. These are not
 *       cosmetic: {@code RUN_DB_METRICS_ONLY} is paired with {@code test-no-failover}, and dropping
 *       {@code IAM} also skips creating the database user, which a one-class run has no use for. Equally,
 *       what each suite <em>keeps</em> is deliberate - {@code PerformanceTest} requires
 *       {@code NETWORK_OUTAGES_ENABLED} and {@code FAILOVER_SUPPORTED} as well as {@code PERFORMANCE}, so a
 *       performance suite that dropped either would run nothing at all.
 *   <li>Tag filters, because the performance suites are separated from each other by JUnit tags rather than
 *       by features: both publish {@code PERFORMANCE}, and {@code advanced} is what tells
 *       {@code AdvancedPerformanceTest} apart from {@code PerformanceTest}.
 *   <li>The environment the selected class needs - which deployments it enables on, and how many instances
 *       its {@code @EnableOnNumOfInstances} demands. Stated here so a run that cannot produce a single test
 *       fails before provisioning rather than after an hour of it.
 * </ul>
 *
 * <p>Hibernate is deliberately absent. {@code RUN_HIBERNATE_TESTS_ONLY} is not a suite selection at all: it
 * makes the harness skip the in-container task entirely and instead build a container that clones
 * hibernate-orm and runs <em>upstream Hibernate's</em> test suite against the wrapper. That is a second
 * harness rather than a mode, and its two in-repo classes are unreachable today for exactly that reason - the
 * flag that enables them is the flag that stops the suite they live in from running.
 */
public enum TestSuite {

  /**
   * The ordinary suite: everything not gated behind one of these features.
   *
   * <p>Publishes none of them, which is what keeps the ~250-test run intact.
   *
   * <p>Excludes the {@code gdb} tag, which is the whole of how the global database tests stay out of ordinary
   * runs. They gate on {@code GLOBAL_DATABASE} as well, so they would skip rather than fail - but skipping ~20
   * classes on every run reports as coverage that ran, and a global database run selects them by the same tag
   * from the other side.
   */
  STANDARD(
      "standard",
      EnumSet.noneOf(TestEnvironmentFeatures.class),
      EnumSet.noneOf(TestEnvironmentFeatures.class),
      null,
      TestTags.GDB,
      1,
      EnumSet.of(
          DatabaseEngineDeployment.AURORA,
          DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER,
          DatabaseEngineDeployment.RDS_MULTI_AZ_INSTANCE,
          DatabaseEngineDeployment.AURORA_GLOBAL),
      false),

  /**
   * {@code PerformanceTest} - failover timing against a real cluster.
   *
   * <p>Excludes the {@code advanced} and {@code rw-splitting} tags, as {@code test-aurora-pg-performance}
   * does. The second exclusion is not redundant even though
   * {@code ReadWriteSplittingPerformanceTest} can never run - it carries {@code @EnableOnTestFeature} and
   * {@code @DisableOnTestFeature} for the same {@code PERFORMANCE} feature, so both conditions fire - and
   * the tag exclusion is what made that harmless rather than a mystery.
   *
   * <p>Three instances minimum, matching the harness excluding the one- and two-node environments.
   */
  PERFORMANCE(
      "performance",
      EnumSet.of(TestEnvironmentFeatures.PERFORMANCE),
      EnumSet.of(
          TestEnvironmentFeatures.IAM,
          TestEnvironmentFeatures.HIKARI,
          TestEnvironmentFeatures.SECRETS_MANAGER),
      null,
      "advanced,rw-splitting",
      3,
      EnumSet.of(DatabaseEngineDeployment.AURORA),
      false),

  /**
   * {@code AdvancedPerformanceTest} - the same feature, selected by the {@code advanced} tag.
   *
   * <p>A separate suite rather than a flag on the previous one, because that is the only thing separating
   * them: {@code test-aurora-pg-advanced-performance} differs from {@code test-aurora-pg-performance} by
   * including the tag the other excludes.
   */
  ADVANCED_PERFORMANCE(
      "advanced-performance",
      EnumSet.of(TestEnvironmentFeatures.PERFORMANCE),
      EnumSet.of(
          TestEnvironmentFeatures.IAM,
          TestEnvironmentFeatures.HIKARI,
          TestEnvironmentFeatures.SECRETS_MANAGER),
      "advanced",
      null,
      3,
      EnumSet.of(DatabaseEngineDeployment.AURORA),
      false),

  /**
   * {@code AutoscalingTests} - a cluster growing and shrinking under a connection pool.
   *
   * <p>Five instances, which its {@code @EnableOnNumOfInstances(min = 5)} requires, and Aurora only. The
   * test adds a sixth instance itself through the RDS API and deletes it again, so the composition needs no
   * extra instrument - but the credentials it runs with need {@code rds:CreateDBInstance} and
   * {@code rds:DeleteDBInstance}, which is worth knowing before a run reports a permissions error twenty
   * minutes in.
   *
   * <p>Keeps every feature the standard suite has. The harness's {@code test-autoscaling-only} drops
   * nothing either, and the test builds its own Hikari provider rather than reading the feature.
   */
  AUTOSCALING(
      "autoscaling",
      EnumSet.of(TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY),
      EnumSet.noneOf(TestEnvironmentFeatures.class),
      null,
      null,
      5,
      EnumSet.of(DatabaseEngineDeployment.AURORA),
      false),

  /**
   * {@code KmsEncryptionIntegrationTest} - client-side encryption over a KMS key.
   *
   * <p>The one suite needing something the composition cannot create: a customer master key. The workflow
   * that drives it looks up {@code alias/jdbc-encryption-key} and creates it if absent, deliberately keeping
   * it between runs, so the key is an input here rather than a provisioned resource - see
   * {@code OrchestraAuroraConfig.getTestContainerEnvironment}.
   *
   * <p>No cluster of its own kind is needed. Every cluster and instance the harness creates is already
   * {@code storageEncrypted}, independent of this flag, and so is every one Orchestra creates.
   */
  ENCRYPTION(
      "encryption",
      EnumSet.of(TestEnvironmentFeatures.RUN_ENCRYPTION_TESTS_ONLY),
      EnumSet.of(
          TestEnvironmentFeatures.FAILOVER_SUPPORTED,
          TestEnvironmentFeatures.SECRETS_MANAGER,
          TestEnvironmentFeatures.HIKARI),
      null,
      null,
      2,
      EnumSet.of(DatabaseEngineDeployment.AURORA),
      true),

  /**
   * {@code DatabasePerformanceMetricTest} - how long a failover takes, measured ten times.
   *
   * <p>The only suite the harness prunes the environment matrix for rather than merely filtering: it drops
   * single-instance environments, anything below two instances, and every deployment other than Aurora and
   * Multi-AZ cluster. Those three constraints are reproduced here.
   *
   * <p>Drops {@code FAILOVER_SUPPORTED} even though the test failovers repeatedly, because that is what
   * {@code test-metrics-*} does and the test does not read the feature - it calls the RDS API directly. The
   * feature's absence is what keeps the ordinary failover classes off.
   */
  METRICS(
      "metrics",
      EnumSet.of(TestEnvironmentFeatures.RUN_DB_METRICS_ONLY),
      EnumSet.of(
          TestEnvironmentFeatures.FAILOVER_SUPPORTED,
          TestEnvironmentFeatures.SECRETS_MANAGER,
          TestEnvironmentFeatures.HIKARI),
      null,
      null,
      2,
      EnumSet.of(
          DatabaseEngineDeployment.AURORA,
          DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER),
      false);

  private final String token;
  private final EnumSet<TestEnvironmentFeatures> added;
  private final EnumSet<TestEnvironmentFeatures> removed;
  private final String includeTags;
  private final String excludeTags;
  private final int minimumInstances;
  private final EnumSet<DatabaseEngineDeployment> deployments;
  private final boolean needsKmsKey;

  TestSuite(
      final String token,
      final EnumSet<TestEnvironmentFeatures> added,
      final EnumSet<TestEnvironmentFeatures> removed,
      final String includeTags,
      final String excludeTags,
      final int minimumInstances,
      final EnumSet<DatabaseEngineDeployment> deployments,
      final boolean needsKmsKey) {

    this.token = token;
    this.added = added;
    this.removed = removed;
    this.includeTags = includeTags;
    this.excludeTags = excludeTags;
    this.minimumInstances = minimumInstances;
    this.deployments = deployments;
    this.needsKmsKey = needsKmsKey;
  }

  /**
   * Returns the suite this run asked for.
   *
   * @return the suite, {@link #STANDARD} when unset
   * @throws IllegalArgumentException if the value is not a suite this task runs
   */
  public static TestSuite requested() {
    final String requested = System.getProperty("orchestra-suite", STANDARD.token).trim();

    for (final TestSuite suite : values()) {
      if (suite.token.equalsIgnoreCase(requested)) {
        return suite;
      }
    }

    final StringBuilder supported = new StringBuilder();
    for (final TestSuite suite : values()) {
      supported.append(supported.length() == 0 ? "" : ", ").append(suite.token);
    }
    throw new IllegalArgumentException(
        "orchestra-suite=" + requested + " is not a suite this task runs. Supported: " + supported + ".");
  }

  /** Returns the property value that selects this suite. */
  public String token() {
    return this.token;
  }

  /**
   * Applies this suite to the standard feature set.
   *
   * <p>Removals after additions, so a suite that both drops and needs a feature is a contradiction this
   * would expose rather than resolve by ordering. None do today.
   *
   * @param features the feature set to adjust in place
   */
  public void applyTo(final EnumSet<TestEnvironmentFeatures> features) {
    features.addAll(this.added);
    features.removeAll(this.removed);
  }

  /** Returns the JUnit tags to include, or {@code null} for all. */
  public String includeTags() {
    return this.includeTags;
  }

  /** Returns the JUnit tags to exclude, or {@code null} for none. */
  public String excludeTags() {
    return this.excludeTags;
  }

  /** Returns the smallest cluster the selected class enables on. */
  public int minimumInstances() {
    return this.minimumInstances;
  }

  /** Reports whether this suite can run against a deployment. */
  public boolean supports(final DatabaseEngineDeployment deployment) {
    return this.deployments.contains(deployment);
  }

  /** Returns the deployments this suite runs against, for an error message. */
  public String supportedDeployments() {
    final StringBuilder names = new StringBuilder();
    for (final DatabaseEngineDeployment deployment : this.deployments) {
      names.append(names.length() == 0 ? "" : ", ").append(deployment.name());
    }
    return names.toString();
  }

  /** Reports whether this suite needs a KMS key passed into the container. */
  public boolean needsKmsKey() {
    return this.needsKmsKey;
  }

  /** Reports whether this run is the ordinary suite. */
  public boolean isStandard() {
    return STANDARD.equals(this);
  }

  /** Returns the token, so log lines and error messages name what a run asked for. */
  @Override
  public String toString() {
    return this.token.toLowerCase(Locale.ROOT);
  }
}
