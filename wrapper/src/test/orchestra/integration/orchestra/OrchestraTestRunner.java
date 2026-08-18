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
import integration.container.TestDriver;
import java.util.EnumSet;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import software.amazon.orchestra.EnvComposition;
import software.amazon.orchestra.EnvCompositionBuilder;
import software.amazon.orchestra.OnError;
import software.amazon.orchestra.contract.BlueGreenDeployment;
import software.amazon.orchestra.contract.Database;
import software.amazon.orchestra.contract.GlobalDatabase;
import software.amazon.orchestra.contract.NetworkGateway;
import software.amazon.orchestra.instruments.aws.AuroraGlobalClusterInstrumentDefinition;
import software.amazon.orchestra.instruments.aws.AwsOrphanCleanup;
import software.amazon.orchestra.instruments.aws.BlueGreenDeploymentInstrumentDefinition;
import software.amazon.orchestra.instruments.aws.MultiRegionSecurityGroupIpWhitelist;
import software.amazon.orchestra.instruments.aws.RdsEngine;
import software.amazon.orchestra.instruments.aws.RegionalDatabaseViewInstrumentDefinition;
import software.amazon.orchestra.instruments.aws.SecurityGroupIpWhitelist;
import software.amazon.orchestra.instruments.docker.DockerNetworkInstrumentDefinition;
import software.amazon.orchestra.instruments.docker.GradleTestContainerRun;
import software.amazon.orchestra.instruments.docker.JavaTestContainerInstrumentDefinition;
import software.amazon.orchestra.instruments.network.TransparentNetworkGatewayInstrumentDefinition;

/**
 * Runs the in-container integration suite against an Orchestra-provisioned environment.
 *
 * <p>The replacement for {@code integration.host.TestRunner}, and the reason Orchestra exists. Where that
 * class was driven by {@code TestEnvironmentProvider}'s seven nested loops and a {@code TestEnvironment} that
 * provisioned everything itself, this declares what the environment contains and lets the engine build it,
 * order it, run the suite, and tear it down.
 *
 * <p>It was additive while it was being proven: the old runner stayed and worked, so every environment kind
 * could be validated against a real account before anything was deleted. It has since been, and the harness
 * is gone - which is why the properties below no longer have {@code test-no-*} equivalents to agree with.
 *
 * <h2>Scope</h2>
 *
 * <p>Started as one slice - Aurora PostgreSQL, two instances, one JVM, nothing optional - because that is
 * the smallest composition exercising the whole chain: AWS provisioning, the security group rule, the
 * context crossing into the container, and a real Gradle suite reading it. Everything the harness varies was
 * then added as an axis or a mode rather than as another runner.
 *
 * <p>What a default run is today: Aurora PostgreSQL, two instances, Java 21, all three drivers where the
 * engine allows them, the routing gateway with impairment working, and both telemetry backends. The axes are
 * {@code -Dorchestra-engines}, {@code -Dorchestra-drivers}, {@code -Dorchestra-instances} and
 * {@code -Dorchestra-jvms}; the modes are {@code -Dorchestra-deployment}, {@code -Dorchestra-bluegreen} and
 * {@code -Dorchestra-telemetry}. The two translations this originally deferred are done: the proxy tests run
 * through the gateway rather than through Toxiproxy's per-endpoint listeners, and the blue/green tests run
 * against a real deployment.
 *
 * <p>What the harness still has and this does not: the {@code *_ONLY} suites - Hibernate, autoscaling,
 * encryption and the database metrics runs - and {@code PERFORMANCE}. Each is a narrowing of the suite plus
 * a feature flag rather than an environment capability, which is why they come last.
 *
 * <h2>The gateway is here even though nothing impairs traffic</h2>
 *
 * <p>{@code JavaTestContainerInstrumentDefinition} requires it: the container sits on an internal Docker
 * network and its route out — to the Aurora endpoint, over the internet — goes through the gateway. Without
 * it the suite could not reach the database at all.
 */
@Tag("orchestra")
// A guard against a hung run, not a budget for a legitimate one - and 90 minutes was the latter by mistake. A
// three-instance performance run spends 25 minutes provisioning and then an hour in PerformanceTest's repeats,
// and this killed it at 90 with a bare InterruptedException from the exec awaiting the container, which reads
// as an infrastructure failure rather than as a timeout. Blue/green makes the gap wider still: the deployment
// instrument alone is configured with a four-hour provisioning budget, so anything shorter than that here
// would cancel runs the composition considers healthy.
@Timeout(value = 8, unit = TimeUnit.HOURS)
public class OrchestraTestRunner {

  /** The task the in-container Gradle build runs, unchanged from the harness. */
  private static final String IN_CONTAINER_TASK = "in-container";

  /**
   * The database user the IAM tests authenticate as.
   *
   * <p>The harness's default when {@code IAM_USER} is unset, kept identical so the in-container
   * expectations are unchanged. Declared once here and handed to both the instrument that creates the user
   * and the shape that publishes its name, so the two cannot disagree - and if they did, the symptom would
   * be an IAM token minted for a user that does not exist.
   */
  private static final String IAM_USERNAME = "jane_doe";

  /**
   * The features every slot of this matrix provides.
   *
   * <p>Hoisted to a constant because two places need exactly the same set: the default {@code TestShape}
   * and the shape {@link EngineVariation} rebuilds per engine. Two copies would be a way for a slot to
   * silently advertise different capabilities from its siblings.
   *
   * <p>The one omission still carries meaning - no {@code BLUE_GREEN_DEPLOYMENT}, which most of the suite
   * disables on - while {@code NETWORK_OUTAGES_ENABLED} is what lets the failover suite run at all. The
   * telemetry features are added by {@link TelemetryBackends#requested()} rather than listed here, because
   * they are the one capability a run switches off rather than on.
   *
   * <p>{@code HIKARI} and {@code SECRETS_MANAGER} need nothing provisioned, which is why they can simply be
   * declared. In the harness both are plain flags - {@code config.noHikari ? null : HIKARI} - because Hikari
   * is a library already on the in-container classpath, and the Secrets Manager test creates and
   * force-deletes its own secret in {@code @BeforeAll}, needing only the region and credentials this
   * composition already supplies. Withholding them was costing the run {@code HikariTests}, the
   * Hikari-gated read/write splitting tests, and {@code AwsSecretsManager2IntegrationTest} for no reason.
   */
  private static final EnumSet<TestEnvironmentFeatures> FEATURES = EnumSet.of(
      TestEnvironmentFeatures.FAILOVER_SUPPORTED,
      TestEnvironmentFeatures.IAM,
      TestEnvironmentFeatures.AWS_CREDENTIALS_ENABLED,
      TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
      TestEnvironmentFeatures.HIKARI,
      TestEnvironmentFeatures.SECRETS_MANAGER,
      TestEnvironmentFeatures.VALKEY_CACHE);

  /**
   * Reports whether this run exercises a blue/green deployment, from {@code -Dorchestra-bluegreen}.
   *
   * <p>A mode rather than an addition to the matrix, because {@code BLUE_GREEN_DEPLOYMENT} is a feature most
   * of the suite <em>disables</em> on: {@code FailoverTest}, the read/write splitting classes and the XA
   * classes all carry {@code @DisableOnTestFeature(BLUE_GREEN_DEPLOYMENT)}. Declaring it in the default set
   * would therefore switch most of the suite off rather than add to it, which is also how the harness treats
   * it - {@code withBlueGreenFeature} builds separate environments.
   *
   * <p>It is also much slower: a deployment can take a couple of hours to provision, so it should not be
   * paid for by every run.
   *
   * @return {@code true} to build a blue/green deployment over the cluster
   */
  private static boolean blueGreen() {
    return Boolean.parseBoolean(System.getProperty("orchestra-bluegreen", "false"));
  }



  /**
   * Returns the deployment kind to provision, from {@code -Dorchestra-deployment}.
   *
   * <p>A mode on this task rather than a second runner, because everything else about the composition is
   * identical: the gateway, the container, the caches, the matrix axes, the IAM user, the feature set. Only
   * the database instrument and what the shape reports differ, so a second runner would be this file copied
   * with two lines changed - and the copy would drift.
   *
   * <p>It matters more than Docker did. An enumeration of the suite's class-level conditions puts
   * {@code RDS_MULTI_AZ_CLUSTER} at 151 of 156 test methods against Docker's 85, because the large families -
   * read/write splitting, failover, Hikari - list Aurora and Multi-AZ and exclude Docker entirely. CI also
   * exercised it deliberately through {@code test-all-multi-az}, which is why the harness could not be
   * retired until this existed.
   *
   * <p>{@code multi-az-instance} is the fourth deployment the harness provides and a different deployment
   * from the cluster rather than a smaller one: one instance with an invisible standby, no reader endpoint,
   * and failover by rebooting the instance. Fewer classes enable on it - the read/write splitting and EFM2
   * families - but it is the only non-Aurora deployment the harness runs blue/green against, which is why
   * retiring the harness needs it too.
   *
   * @return the deployment to build, defaulting to Aurora
   */
  private static DatabaseEngineDeployment deployment() {
    final String requested = System.getProperty("orchestra-deployment");
    if (requested == null || requested.trim().isEmpty()) {
      return DatabaseEngineDeployment.AURORA;
    }

    final String normalised = requested.trim().toUpperCase(Locale.ROOT).replace('-', '_');
    switch (normalised) {
      case "AURORA":
        return DatabaseEngineDeployment.AURORA;
      case "MULTI_AZ_CLUSTER":
      case "RDS_MULTI_AZ_CLUSTER":
        return DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER;
      case "MULTI_AZ_INSTANCE":
      case "RDS_MULTI_AZ_INSTANCE":
        return DatabaseEngineDeployment.RDS_MULTI_AZ_INSTANCE;
      case "AURORA_GLOBAL":
      case "GLOBAL":
      case "GDB":
        return DatabaseEngineDeployment.AURORA_GLOBAL;
      default:
        throw new IllegalArgumentException(
            "orchestra-deployment=" + requested + " is not a deployment this task can provision. Supported: "
                + "aurora, multi-az-cluster, multi-az-instance, aurora-global.");
    }
  }

  /**
   * Returns the features for this run.
   *
   * <p>{@link #FEATURES} plus {@code BLUE_GREEN_DEPLOYMENT} in blue/green mode, plus whatever
   * {@link TelemetryBackends#requested()} resolves to, plus a
   * {@code SKIP_*_DRIVER_TESTS} for every driver this run is not exercising. Nothing else is removed: the
   * blue/green tests disable only on {@code PERFORMANCE} and the {@code *_ONLY} flags, none of which this
   * composition declares, and they need {@code IAM} because one of them authenticates that way.
   *
   * @return the feature set to publish
   */
  private static EnumSet<TestEnvironmentFeatures> features() {
    final EnumSet<TestEnvironmentFeatures> features = EnumSet.copyOf(FEATURES);
    if (blueGreen()) {
      features.add(TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT);
    }
    if (DatabaseEngineDeployment.AURORA_GLOBAL.equals(deployment())) {
      features.add(TestEnvironmentFeatures.GLOBAL_DATABASE);
    }
    features.addAll(TelemetryBackends.requested());

    // Last, because a suite both adds its own feature and drops the ones its task disables. Applying it
    // before the driver skips below would work equally well; applying it before telemetry would not, since
    // no suite touches telemetry and a reader would have to check that to know.
    TestSuite.requested().applyTo(features);

    // Expressed as skips rather than as a selection, because that is the only vocabulary the in-container
    // filter has: TestEnvironment.isTestDriverAllowed asks whether a SKIP_ feature is present, so "run the
    // MariaDB driver" has to be said as "skip the other two".
    final EnumSet<TestDriver> selected = drivers();
    for (final TestDriver driver : TestDriver.values()) {
      if (!selected.contains(driver)) {
        features.add(skipFeatureFor(driver));
      }
    }
    return features;
  }

  /**
   * Returns the drivers to exercise, from {@code -Dorchestra-drivers}.
   *
   * <p>A separate axis from the engine, and the distinction is the whole point of this property. The engine
   * is what gets provisioned; the driver is which JDBC implementation the in-container suite connects with,
   * and the two are not in step because the MariaDB driver is used <em>against a MySQL engine</em>. CI relies
   * on that: {@code test-bgd-mysql-aurora-mariadb-driver} and
   * {@code test-bgd-mysql-rds-instance-mariadb-driver} both provision MySQL and both disable the MariaDB
   * engine, pinning the driver to MariaDB and nothing else. Two of the scheduled blue/green jobs are those,
   * so the harness could not be retired while this path could only say which engine to build.
   *
   * <p>Defaults to every driver, which is what the harness does when no {@code test-no-*-driver} is passed
   * and what this task did implicitly before the property existed: with a MySQL engine and no skips the
   * container enumerates the MySQL and MariaDB drivers and runs each test twice. That was invisible only
   * because the engine axis defaults to Postgres, where the PG driver is the sole compatible one.
   *
   * @return at least one driver
   */
  private static EnumSet<TestDriver> drivers() {
    final String requested = System.getProperty("orchestra-drivers");
    if (requested == null || requested.trim().isEmpty()) {
      return EnumSet.allOf(TestDriver.class);
    }

    final EnumSet<TestDriver> drivers = EnumSet.noneOf(TestDriver.class);
    for (final String name : requested.split(",")) {
      drivers.add(TestDriver.valueOf(name.trim().toUpperCase(Locale.ROOT)));
    }

    if (drivers.isEmpty()) {
      throw new IllegalArgumentException(
          "orchestra-drivers was given but named no driver. Omit it to run every driver the engine "
              + "supports.");
    }
    return drivers;
  }

  /**
   * Returns the feature that switches a driver off.
   *
   * <p>One per driver, and the enum this maps from is the in-container {@code TestDriver} rather than the
   * engine, so a mistake here would show up as a test silently not running rather than as an error.
   *
   * @param driver the driver to disable
   * @return the feature that disables it
   */
  private static TestEnvironmentFeatures skipFeatureFor(final TestDriver driver) {
    switch (driver) {
      case PG:
        return TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS;
      case MYSQL:
        return TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS;
      case MARIADB:
        return TestEnvironmentFeatures.SKIP_MARIADB_DRIVER_TESTS;
      default:
        throw new IllegalArgumentException(
            "No skip feature is known for driver " + driver + ". Add the pairing rather than letting a "
                + "driver be unswitchable.");
    }
  }

  /**
   * Reports whether a driver can be used against an engine.
   *
   * <p>The same compatibility {@code TestEnvironment.isTestDriverAllowed} enforces in the container,
   * repeated here so a run that could produce no tests at all fails before it provisions anything. The
   * asymmetry is real rather than an oversight: the MariaDB driver speaks to a MySQL engine as well as to a
   * MariaDB one, while the MySQL and PostgreSQL drivers each speak only to their own.
   *
   * @param driver the driver
   * @param engine the wrapper's engine
   * @return {@code true} if the container would enumerate that pairing
   */
  private static boolean driverSuits(final TestDriver driver, final DatabaseEngine engine) {
    switch (driver) {
      case PG:
        return DatabaseEngine.PG.equals(engine);
      case MYSQL:
        return DatabaseEngine.MYSQL.equals(engine);
      case MARIADB:
        return DatabaseEngine.MYSQL.equals(engine) || DatabaseEngine.MARIADB.equals(engine);
      default:
        return false;
    }
  }

  /**
   * Rejects the one deployment-and-mode combination that has no meaning here.
   *
   * <p>Blue/green over an RDS Multi-AZ <em>cluster</em>, which the harness does not do either: its matrix
   * pairs the blue/green feature with Aurora and Multi-AZ instance only. Nothing about it would work if
   * asked for. The Multi-AZ cluster instrument takes its parameter group from configuration and does not
   * pick one up from the composition, so the group this branch adds would never be attached and replication
   * would never be enabled; and the family that group is created with is Aurora's, which the cluster's own
   * engine name does not match.
   *
   * <p>Rejected here rather than left to fail during provisioning, because a run that got that far would
   * spend an hour on three instances first and then report an error about a parameter group family.
   *
   * @param deployment the deployment this run asked for
   */
  private static void rejectUnsupportedMode(final DatabaseEngineDeployment deployment) {
    if (blueGreen() && DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER.equals(deployment)) {
      throw new IllegalArgumentException(
          "orchestra-bluegreen=true is not supported with orchestra-deployment=multi-az-cluster, and the "
              + "harness does not run that combination either: blue/green applies to aurora and "
              + "multi-az-instance.");
    }

    if (DatabaseEngineDeployment.AURORA_GLOBAL.equals(deployment)) {
      if (blueGreen()) {
        throw new IllegalArgumentException(
            "orchestra-bluegreen=true is not supported with orchestra-deployment=aurora-global. A blue/green "
                + "deployment is created from a regional cluster, and a global database's clusters are not "
                + "eligible while they are members of it.");
      }
      if (engines().length > 1) {
        throw new IllegalArgumentException(
            "orchestra-deployment=aurora-global provisions one global database, and orchestra-engines named "
                + engines().length + " engines. An engine matrix rebinds the database role, which on a global "
                + "run is a view of the topology rather than a cluster of its own - so the second slot would "
                + "provision an unrelated database. Run one engine at a time.");
      }
    }

    rejectEnginesWithNoDriver();
    rejectSuiteThatCannotRun(deployment);
  }

  /**
   * Rejects a suite whose selected class could not run in the environment this run would build.
   *
   * <p>Every one of these would otherwise provision a cluster, wait an hour, and report success having run
   * nothing: the {@code *_ONLY} features select a single class through {@code @EnableOnTestFeature} and
   * switch the rest of the suite off, so "no tests ran" is indistinguishable from "the suite passed" unless
   * the mismatch is caught here.
   *
   * <p>Three ways it can happen. The class enables on deployments this run is not building - only
   * {@code DatabasePerformanceMetricTest} accepts anything other than Aurora. The cluster is smaller than
   * its {@code @EnableOnNumOfInstances} demands, which is what the harness expresses by excluding instance
   * counts in each task; every requested count has to qualify rather than just one, since a matrix slot that
   * runs nothing is the same waste on a smaller scale. Or the run is a blue/green one, which is a different
   * mode entirely: the blue/green classes disable on all of these features, and these classes say nothing
   * about a deployment being switched over.
   *
   * @param deployment the deployment this run asked for
   */
  private static void rejectSuiteThatCannotRun(final DatabaseEngineDeployment deployment) {
    final TestSuite suite = TestSuite.requested();
    if (suite.isStandard()) {
      return;
    }

    if (blueGreen()) {
      throw new IllegalArgumentException(
          "orchestra-suite=" + suite + " cannot be combined with orchestra-bluegreen=true. The blue/green "
              + "tests disable on every suite feature, and the suite's own class is not a blue/green test, "
              + "so the run would provision a deployment and execute nothing.");
    }

    if (!suite.supports(deployment)) {
      throw new IllegalArgumentException(
          "orchestra-suite=" + suite + " does not run against orchestra-deployment=" + deployment.name()
              + ". Its test class enables on: " + suite.supportedDeployments() + ".");
    }

    for (final int count : instanceCounts()) {
      if (count < suite.minimumInstances()) {
        throw new IllegalArgumentException(
            "orchestra-suite=" + suite + " needs at least " + suite.minimumInstances()
                + " instances and orchestra-instances asks for " + count + ". Its test class carries "
                + "@EnableOnNumOfInstances(min = " + suite.minimumInstances() + "), so that slot would "
                + "provision a cluster and run nothing.");
      }
    }

    // A missing key is the worst version of this failure rather than a lesser one. The test's precondition
    // is assumeTrue, so without a key it reports as skipped and the run reports as passed - which is what
    // the harness does today, since it sets KMS_KEY_ID while the test reads AWS_KMS_KEY_ARN.
    if (suite.needsKmsKey() && OrchestraAuroraConfig.kmsKey() == null) {
      throw new IllegalArgumentException(
          "orchestra-suite=" + suite + " needs a KMS key, and neither the KMS_KEY_ID environment variable "
              + "nor -Dorchestra-kms-key is set. The key is an input rather than something this composition "
              + "creates: the workflow driving this suite reuses alias/jdbc-encryption-key. Without it the "
              + "test would skip its own precondition and the run would report success having encrypted "
              + "nothing.");
    }
  }

  /**
   * Rejects an engine-and-driver combination that would run nothing.
   *
   * <p>Checked because the failure it prevents is silent. A driver is switched off by publishing a
   * {@code SKIP_} feature, and a slot whose engine has no compatible driver left simply enumerates zero
   * invocations: the run provisions a cluster, starts the container, reports success and tests nothing.
   * {@code -Dorchestra-engines=postgres -Dorchestra-drivers=mariadb} is the obvious way to ask for that, and
   * a mixed engine axis with a pinned driver is the subtle one - the skip set is published per shape, so
   * pinning MariaDB across {@code postgres,mysql} silences the Postgres slot alone.
   */
  private static void rejectEnginesWithNoDriver() {
    final EnumSet<TestDriver> selected = drivers();

    for (final RdsEngine engine : engines()) {
      final DatabaseEngine wrapperEngine = EngineVariation.wrapperEngineFor(engine);

      boolean anySuits = false;
      for (final TestDriver driver : selected) {
        if (driverSuits(driver, wrapperEngine)) {
          anySuits = true;
          break;
        }
      }

      if (!anySuits) {
        throw new IllegalArgumentException(
            "No driver in orchestra-drivers=" + selected + " can be used against the " + wrapperEngine
                + " engine, so that slot would provision a database and run no tests. The MariaDB driver "
                + "works against MySQL and MariaDB engines; the MySQL and PostgreSQL drivers only against "
                + "their own.");
      }
    }
  }

  /**
   * Reports whether this deployment's topology is read through the {@code rds_tools} extension.
   *
   * <p>The Multi-AZ deployments, and only those. Aurora answers the same question with
   * {@code aurora_replica_status()}, which is built in, so creating an extension there would be work with
   * nothing reading it.
   *
   * @param deployment the deployment this run provisions
   * @return {@code true} when the extension has to be created before the suite runs
   */
  private static boolean rdsToolsNeeded(final DatabaseEngineDeployment deployment) {
    return DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER.equals(deployment)
        || DatabaseEngineDeployment.RDS_MULTI_AZ_INSTANCE.equals(deployment);
  }

  @Test
  @DisplayName("provisions an Aurora PostgreSQL cluster and runs the in-container suite against it")
  public void runTests() throws Exception {
    rejectUnsupportedMode(deployment());

    final boolean globalDatabase = DatabaseEngineDeployment.AURORA_GLOBAL.equals(deployment());

    // The global configuration is the Aurora one plus regions, so a global run inherits every other setting
    // rather than keeping its own copy. It is a different type because the multi-region whitelist and the
    // orphan sweep ask the configuration whether this run spans regions.
    final OrchestraAuroraConfig configuration = globalDatabase
        ? new OrchestraGlobalDatabaseConfig(IN_CONTAINER_TASK)
        : new OrchestraAuroraConfig(IN_CONTAINER_TASK);

    // Resolved once and shared, which is the point of FEATURES being a constant: the default TestShape and
    // the shape EngineVariation rebuilds per engine must advertise the same capabilities, and calling
    // features() at each site would hand them two objects whose equality depended on the property being
    // read the same way twice.
    final EnumSet<TestEnvironmentFeatures> features = features();

    final EnvCompositionBuilder builder = EnvComposition.getBuilder()
        // The runner's IP in the security group, and a sweep of anything an earlier run abandoned. Both
        // are per-run rather than per-cluster, which is what the harness's authorizeIP/deAuthorizeIP pair
        // did by hand once per environment.
        //
        // The multi-region whitelist on a global run, because a security group is regional: a secondary
        // cluster whose own region does not allow this machine provisions healthily, bills for the whole run,
        // and times out every connection with nothing pointing at the cause.
        .addGlobalResource(globalDatabase
            ? new MultiRegionSecurityGroupIpWhitelist()
            : new SecurityGroupIpWhitelist())
        .addGlobalResource(new AwsOrphanCleanup())
        .addInstrumentDefinition(new DockerNetworkInstrumentDefinition())
        .addInstrument(NetworkGateway.class, new TransparentNetworkGatewayInstrumentDefinition())
        // A default that EngineVariation rebinds per slot, matching the deployment either way. The mapping
        // is EngineVariation's so that the default and the varied slots cannot disagree about which
        // instrument a deployment means.
        //
        // On a global run this is a view rather than the thing itself: the topology is created under the
        // GlobalDatabase role below, and this publishes one region of it as an ordinary cluster. The primary
        // region, deliberately - a secondary holds no writer, and the health check that runs before every
        // test class asserts there is one. The GDB tests reach the secondaries through the published
        // topology, which they have to anyway in order to set failoverHomeRegion and the per-region host
        // templates.
        .addInstrument(Database.class, globalDatabase
            ? new RegionalDatabaseViewInstrumentDefinition()
            : EngineVariation.databaseFor(deployment(), engines()[0]))
        // The database side of IAM. Credentials in the container let a test mint a token; this is what
        // makes the token usable, by creating the user it authenticates as and granting it rds_iam.
        //
        // Bound after the database on purpose in a global run: the user is created on the writer, which is in
        // the primary region, and that is the region the database role publishes.
        .addInstrumentDefinition(new IamDatabaseUserInstrumentDefinition(IAM_USERNAME))
        // What the harness calls the TestEnvironmentRequest. Orchestra publishes what a composition
        // contains, not what kind of run it is, and ~150 in-container call sites plus the condition
        // annotations ask for the latter. See TestShape for why this is temporary.
        //
        // A default that EngineVariation rebinds in every slot. Declared anyway so the composition is
        // complete without the variation, which is what makes the variation removable.
        .addInstrument(TestShape.class, new TestShapeInstrumentDefinition(
            EngineVariation.wrapperEngineFor(engines()[0]), deployment(), IAM_USERNAME, features))
        .addInstrumentDefinition(new JavaTestContainerInstrumentDefinition())
        .addVariation(new InstanceCountVariation(instanceCounts()))
        // The JVM axis, and a pure configuration override rather than a rebinding: the shape derives the
        // JVM it publishes from the same image this overrides. See TargetJvmImages.
        .addVariation(new JvmVariation(jvms()))
        // The four caches the query-cache and Spring caching tests address by index. Not an axis - it
        // returns one composition - but a variation is where Orchestra exposes role-scoped configuration,
        // which is what four caches differing only in authentication and TLS need.
        .addVariation(ValkeyCachesVariation.standard());

    if (globalDatabase) {
      // The topology itself: one global cluster, the primary regional cluster, and a cluster in each secondary
      // region. Bound to a role of its own rather than to Database, because it is not something a workload
      // connects to - it is how a workload learns which regions exist and how to address them.
      builder.addInstrument(
          GlobalDatabase.class, new AuroraGlobalClusterInstrumentDefinition(engines()[0]));

    } else {
      // The engine axis. Absent from a global run, which is why the engine matrix is rejected there: this
      // variation rebinds the Database role, and on a global run that role is a view of a topology bound
      // elsewhere - so rebinding it per engine would replace the view with a plain cluster instrument and
      // provision a second, unrelated database.
      builder.addVariation(new EngineVariation(deployment(), IAM_USERNAME, features, engines()));

      // The parameter group the database is created with, and a default EngineVariation rebinds per slot
      // for the same reason it rebinds the database: a group belongs to one engine family. Unconditional
      // outside a global run, because PostgreSQL needs max_prepared_transactions for the XA tests to run
      // rather than skip, and MySQL needs require_secure_transport off to be connectable at all.
      //
      // Absent on a global run, and that is a known gap rather than a decision that a global database needs
      // no parameters: a parameter group is regional, so a global database needs one per region, created with
      // that region's client. Until then a global run takes the engine defaults, which means its PostgreSQL
      // XA tests would skip and the MariaDB driver cannot reach a MySQL global database.
      builder.addInstrument(
          EngineVariation.parameterGroupRole(deployment()),
          EngineVariation.parameterGroupFor(deployment(), engines()[0]));
    }

    if (rdsToolsNeeded(deployment())) {
      // A property of the deployment, not of the engine: Multi-AZ publishes its topology through
      // rds_tools.show_topology(), where Aurora has aurora_replica_status() and needs nothing created. The
      // instrument itself skips a MySQL slot, so an engine matrix needs no condition here.
      builder.addInstrumentDefinition(new RdsToolsExtensionInstrumentDefinition(IAM_USERNAME));
    }

    if (blueGreen()) {
      builder
          // Bound to a role token rather than added as a bare definition, for two reasons. The container
          // needs a name it can look the deployment up by, and a bare definition is keyed by its own class,
          // which lives in orchestra-instruments and is not on the container's classpath. It also gives the
          // test container something to order itself after: a deployment takes tens of minutes to create,
          // and the container's context snapshot is taken when it is built.
          .addInstrument(
              BlueGreenDeployment.class, new BlueGreenDeploymentInstrumentDefinition(Database.class));
    }

    // Where the wrapper's traces and metrics go, one backend per feature the shape declares. On by default,
    // as in the harness; see TelemetryBackends.
    TelemetryBackends.addTo(builder, features);

    builder
        .configureExecutionPipeline(e -> e
            // CONTINUE once there is more than one slot. With a matrix, stopping at the first failing
            // composition would hide whether the others are fine, and the question a matrix exists to
            // answer is which combinations work - so every slot runs and the build still fails at the end.
            .onError(compositionCount() > 1 ? OnError.CONTINUE : OnError.FAIL)
            .useComposition(new GradleTestContainerRun(configuration.getGradleTask())))
        .build()
        .run(configuration);
  }

  /**
   * Returns the engines to run, from {@code -Dorchestra-engines}.
   *
   * <p>Defaults to PostgreSQL alone. The default is a single engine rather than everything because this is
   * an hour-long task per slot against real AWS resources, and a developer running it unqualified means
   * "the usual one", not "all of them".
   *
   * @return at least one engine
   */
  private static RdsEngine[] engines() {
    final String requested = System.getProperty("orchestra-engines");
    if (requested == null || requested.trim().isEmpty()) {
      return new RdsEngine[] {RdsEngine.POSTGRES};
    }

    final String[] names = requested.split(",");
    final RdsEngine[] engines = new RdsEngine[names.length];
    for (int i = 0; i < names.length; i++) {
      engines[i] = RdsEngine.valueOf(names[i].trim().toUpperCase(Locale.ROOT));
    }
    return engines;
  }

  /**
   * Returns the JVMs to run the suite on, from {@code -Dorchestra-jvms}.
   *
   * <p>The harness's {@code TargetJvm} loop, which it drives by exclusion with {@code test-no-openjdk8},
   * {@code test-no-openjdk11}, {@code test-no-openjdk17}, {@code test-no-openjdk21},
   * {@code test-no-openjdk24} and {@code test-no-graalvm}. Named by the enum, so
   * {@code -Dorchestra-jvms=OPENJDK17,OPENJDK21} runs two slots.
   *
   * <p>Defaults to Java 21 alone, which is what this task ran on before the axis existed. A default of every
   * JVM would multiply an hour-long AWS run by six, and the JVM is the axis least likely to be what a
   * developer is investigating.
   *
   * @return at least one JVM
   */
  private static TargetJvm[] jvms() {
    return JvmVariation.requested();
  }

  /**
   * Returns the cluster sizes to run, from {@code -Dorchestra-instances}.
   *
   * <p>Defaults to two, the smallest size that makes a failover test meaningful.
   *
   * @return at least one count
   */
  private static int[] instanceCounts() {
    final String requested = System.getProperty("orchestra-instances");
    if (requested == null || requested.trim().isEmpty()) {
      return new int[] {2};
    }

    final String[] values = requested.split(",");
    final int[] counts = new int[values.length];
    for (int i = 0; i < values.length; i++) {
      counts[i] = Integer.parseInt(values[i].trim());
    }
    return counts;
  }

  /** How many slots the matrix expands to, which decides whether a failure stops the run. */
  private static int compositionCount() {
    return engines().length * instanceCounts().length * jvms().length;
  }

}
