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

import integration.TargetJvm;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.orchestra.EnvConfiguration;
import software.amazon.orchestra.instruments.aws.AuroraClusterConfiguration;
import software.amazon.orchestra.instruments.aws.AwsClients;
import software.amazon.orchestra.instruments.aws.BlueGreenDeploymentConfiguration;
import software.amazon.orchestra.instruments.aws.RdsMultiAzClusterConfiguration;
import software.amazon.orchestra.instruments.aws.RdsMultiAzInstanceConfiguration;
import software.amazon.orchestra.instruments.aws.SecurityGroupConfiguration;
import software.amazon.orchestra.instruments.docker.JavaTestContainerConfiguration;
import software.amazon.orchestra.instruments.docker.OtlpCollectorConfiguration;
import software.amazon.orchestra.instruments.docker.ValkeyContainerConfiguration;
import software.amazon.orchestra.instruments.docker.XRayDaemonConfiguration;
import software.amazon.orchestra.instruments.network.NetworkGatewayConfiguration;

/**
 * The single configuration object Orchestra reads for a migrated integration test run.
 *
 * <p>One object implementing every declared instrument's interface, which is Orchestra's model: each
 * instrument casts this to its own interface and reads only its own slice. It replaces the parts of
 * {@code TestEnvironmentConfiguration} that describe <em>what to provision</em>. The parts that decide
 * <em>which combinations to run</em> — the {@code test-no-*} filters — become variations instead, because
 * that is the difference the migration is about: the harness selects a matrix by subtracting from nested
 * loops, and Orchestra states it by naming what to include.
 *
 * <h2>What the container gets, and why it is listed here</h2>
 *
 * <p>{@link #getTestContainerCopies()} and {@link #getTestContainerBinds()} reproduce
 * {@code ContainerHelper.createTestContainer}'s fifteen hardcoded paths. They are configuration now rather
 * than framework code, which is what lets Orchestra host this suite without knowing anything about the JDBC
 * wrapper's layout.
 *
 * <p>Copies and binds are not interchangeable. Copies are snapshots, so a rebuild on the host cannot change
 * what is executing in a long run; binds are how reports get back out. The harness had exactly this split
 * and it is preserved deliberately.
 */
public class OrchestraAuroraConfig implements EnvConfiguration,
    AuroraClusterConfiguration,
    SecurityGroupConfiguration,
    JavaTestContainerConfiguration,
    NetworkGatewayConfiguration,
    // The Multi-AZ cluster's settings, which are separate from Aurora's on purpose: RDS Multi-AZ takes
    // storage and IOPS and fixes the instance count at three, where Aurora takes a count and neither.
    // Sharing one interface would mean half the values being meaningless in each direction.
    RdsMultiAzClusterConfiguration,
    // The Multi-AZ instance's settings, separate again for a reason the interface documents: both are
    // implemented on this one object, Java resolves a getter by signature rather than by declaring
    // interface, and the two deployments' storage defaults differ (gp3 here, io1 for the cluster). A shared
    // getter name would hand one deployment the other's storage type and surface as an IOPS ratio error.
    RdsMultiAzInstanceConfiguration,
    // Also with no methods implemented. The defaults are the ones the validation example used: a four-hour
    // provisioning budget, a fifteen-minute switchover budget, and no waiting for deletion, which can take
    // two hours and would be added to every blue/green run for no benefit to the test that just finished.
    BlueGreenDeploymentConfiguration,
    // Declared with no methods implemented: every value has a default, and the four caches differ only in
    // authentication and TLS, which ValkeyCachesVariation supplies as role-scoped overrides. The interface
    // still has to appear here, because the instrument casts this object to it.
    ValkeyContainerConfiguration,
    // The two telemetry backends, both entirely on their defaults, and the collector's default config is a
    // deliberate difference from the harness rather than an unfinished one. The harness mounts an
    // otel-config.yaml whose exporters are awsxray and awsemf, so its collector forwards every segment and
    // metric into the account's X-Ray and CloudWatch. Nothing in the suite asserts on what arrives there -
    // the tests only exercise the wrapper's exporters - so the default pipeline, which accepts telemetry and
    // logs it, buys the same in-container coverage without paying to ingest it. It also needs no credentials
    // rendered to the collector, which this instrument does not do.
    XRayDaemonConfiguration,
    OtlpCollectorConfiguration {

  /**
   * Where the wrapper module sits relative to the host JVM's working directory.
   *
   * <p>Package-private rather than private, along with {@link #outputDirectory} and {@link #driverJar}, so
   * {@link OrchestraHibernateConfig} can resolve its own paths the same way. It copies an entirely different
   * set of files into an entirely different container, so it replaces those maps rather than adding to them -
   * but "where the module is" and "which jar the build produced" are the same questions either way, and
   * answering them twice is how the two would drift.
   */
  static final Path MODULE = Path.of(".");

  /**
   * The host's AWS credentials directory, bound into the container when the run asks for it.
   *
   * <p>From {@code user.home} rather than a hardcoded path, so this works on whichever platform the host is.
   * The directory and not the credentials file itself: a single-file bind is resolved once, so it breaks when
   * the file is replaced rather than written in place — which is what a credential refresh does — and the
   * container would keep reading the file that is no longer there.
   */
  private static final Path AWS_DIRECTORY = Path.of(System.getProperty("user.home"), ".aws");

  /**
   * Where {@link #AWS_DIRECTORY} appears inside the container.
   *
   * <p>Deliberately not {@code /root/.aws}, which is where the SDK would look on its own. Mounting it out of
   * the way means nothing in the host's AWS directory is picked up implicitly — only what
   * {@link #getTestContainerEnvironment()} names explicitly. That distinction is load-bearing rather than
   * tidiness; see {@link #CONTAINER_AWS_CONFIG_FILE}.
   */
  private static final String CONTAINER_AWS_DIRECTORY = "/orchestra/aws";

  /**
   * The credentials file the container reads, which is the host's, live.
   */
  private static final String CONTAINER_AWS_CREDENTIALS_FILE = CONTAINER_AWS_DIRECTORY + "/credentials";

  /**
   * A config file path that intentionally does not exist.
   *
   * <p>The host's {@code ~/.aws/config} must not be used, and pointing somewhere empty is the only way to say
   * so. A developer's {@code [profile default]} there carries {@code credential_process} — a command that
   * fetches fresh credentials — and the SDK prefers it over the static keys in {@code credentials}. Inside a
   * Linux container that command is a Windows executable that is not present, so the whole profile fails with
   * {@code Failed to refresh process-based credentials} and every AWS-touching test fails with
   * {@code Unable to load credentials from any of the providers in the chain}. The credentials are sitting
   * right there in the bound file; the SDK never looks at them.
   *
   * <p>Leaving {@code AWS_CONFIG_FILE} unset does not avoid this. The SDK falls back to
   * {@code $HOME/.aws/config}, so anything bound at the default location gets read anyway — which is why the
   * bind is mounted elsewhere.
   *
   * <p>An absent file at an explicit path is not an error to the SDK; it is treated as an empty profile file.
   * Nothing is lost by skipping the real one: the only setting the suite needs from it is the region, and that
   * is passed as {@code AWS_REGION}.
   */
  private static final String CONTAINER_AWS_CONFIG_FILE = CONTAINER_AWS_DIRECTORY + "/config.unused";

  /**
   * The property that turns the credentials-file bind on.
   *
   * <p>Named like the other {@code orchestra-*} run properties so it is forwarded and documented the same
   * way, rather than being a second mechanism to remember.
   */
  private static final String BIND_CREDENTIALS_PROPERTY = "orchestra-aws-credentials-bind";

  /**
   * The master password for the provisioned cluster.
   *
   * <p>Generated per run rather than read from {@code DB_PASSWORD}. Nothing outside this process needs it:
   * the in-container tests read it from Orchestra's context, which is the whole point of the migration.
   * A backslash is avoided along with the characters RDS rejects, since the password also travels through
   * a JSON context document.
   */
  private static final String PASSWORD = generatePassword();

  private final String gradleTask;

  /**
   * Creates the configuration for a run.
   *
   * @param gradleTask the task to run inside the container, normally {@code in-container}
   */
  public OrchestraAuroraConfig(final String gradleTask) {
    this.gradleTask = gradleTask;
  }

  /**
   * Returns the Gradle task the run executes in the container.
   *
   * @return a task name from the in-container build script
   */
  public String getGradleTask() {
    return this.gradleTask;
  }

  // ---------------------------------------------------------------- Aurora

  @Override
  public String getAuroraPassword() {
    return PASSWORD;
  }

  /**
   * Returns the database name the cluster is created with.
   *
   * <p>Matches the harness's default so the in-container tests' expectations are unchanged.
   */
  @Override
  public String getAuroraDatabaseName() {
    return "test_database";
  }

  @Override
  public String getAuroraUsername() {
    return "test_user";
  }

  // ---------------------------------------------------------------- RDS Multi-AZ

  /**
   * Returns the master password for a Multi-AZ cluster.
   *
   * <p>The same per-run generated value Aurora uses, and required rather than defaulted for the same reason:
   * a shared known password on a database reachable from the runner's IP is not something to ship a default
   * for. Only one of the two clusters is ever provisioned in a run, so sharing the value costs nothing.
   */
  @Override
  public String getMultiAzPassword() {
    return PASSWORD;
  }

  /** Matches the Aurora database name, so the in-container expectations do not vary by deployment. */
  @Override
  public String getMultiAzDatabaseName() {
    return "test_database";
  }

  @Override
  public String getMultiAzUsername() {
    return "test_user";
  }

  // ------------------------------------------------------- RDS Multi-AZ instance

  /**
   * Returns the master password for a Multi-AZ instance.
   *
   * <p>The same per-run value again. One deployment is provisioned per run, so the three credentials below
   * duplicate the values above rather than diverging from them - which is the point: the in-container tests
   * connect with one set of credentials whatever provisioned the database.
   */
  @Override
  public String getMultiAzInstancePassword() {
    return PASSWORD;
  }

  /** Matches the other deployments' database name, so the in-container expectations do not vary. */
  @Override
  public String getMultiAzInstanceDatabaseName() {
    return "test_database";
  }

  @Override
  public String getMultiAzInstanceUsername() {
    return "test_user";
  }

  /**
   * Returns two instances.
   *
   * <p>The instance-count axis the harness varies over {@code [1, 2, 3, 5]}. Two is the smallest count that
   * makes a failover test meaningful, so it is the right default for the first migrated slice; a variation
   * overrides it for the wider matrix.
   */
  @Override
  public int getAuroraInstanceCount() {
    return 2;
  }

  // ---------------------------------------------------------------- test container

  /**
   * Returns the JVM image the suite runs in.
   *
   * <p>The target-JVM axis, which the harness expresses as {@code TargetJvm} plus five
   * {@code test-no-openjdk*} flags and {@code test-no-graalvm}. {@link JvmVariation} overrides this method
   * and nothing else, and {@code TestShapeInstrumentDefinition} derives the JVM it publishes from whatever
   * this returns, so the container and the shape cannot disagree about which JVM the suite is on.
   *
   * <p>Resolved through {@link TargetJvmImages} rather than written out, which also corrects it: this used to
   * return {@code amazoncorretto:21-alpine} while claiming to match the harness, and the harness picks
   * {@code amazoncorretto:21-alpine-full} for {@code OPENJDK21}. The claim is now enforced by construction.
   */
  @Override
  public String getTestContainerImage() {
    return TargetJvmImages.imageFor(TargetJvm.OPENJDK21);
  }

  @Override
  public String getTestContainerWorkingDirectory() {
    return "/app";
  }

  /**
   * Returns everything copied into the container, mirroring the harness.
   *
   * <p>Ordered, so the mapping reads in the same order as the code it replaces. The Orchestra jars are the
   * one addition: {@code orchestra-contract} and {@code orchestra-client-java} go into {@code /app/libs}
   * because the in-container facade reads its environment through {@code OrchestraClient}, and the
   * in-container build already puts {@code libs/*.jar} on the test classpath.
   */
  @Override
  public Map<Path, String> getTestContainerCopies() {
    final Map<Path, String> copies = new LinkedHashMap<>();

    copies.put(MODULE.resolve("build/classes/java/test"), "/app/test");
    copies.put(MODULE.resolve("../gradlew"), "/app/gradlew");
    copies.put(driverJar(), "/app/libs/aws-advanced-jdbc-wrapper.jar");
    copies.put(MODULE.resolve("src/test/build.gradle.kts"), "/app/build.gradle.kts");

    // The client and the contract, so in-container code can call OrchestraClient.
    //
    // Named individually and placed directly in /app/libs, not as a directory. The in-container build puts
    // fileTree("./libs") { include("*.jar") } on the test classpath, and that pattern does not recurse - a
    // libs/orchestra/ subdirectory produced NoClassDefFoundError: OrchestraClient at the first call site.
    //
    // Only these two. orchestra-core and orchestra-instruments provision environments, which is no business
    // of code running inside one, and they target Java 17 while this classpath is Java 8.
    copies.put(MODULE.resolve("lib/orchestra/orchestra-contract-0.1.0-SNAPSHOT.jar"),
        "/app/libs/orchestra-contract.jar");
    copies.put(MODULE.resolve("lib/orchestra/orchestra-client-java-0.1.0-SNAPSHOT.jar"),
        "/app/libs/orchestra-client-java.jar");

    copies.put(MODULE.resolve("src/test/resources/rds-ca-2019-root.pem"),
        "/app/test/resources/rds-ca-2019-root.pem");
    copies.put(MODULE.resolve("src/test/resources/rds-ca-rsa2048-g1.pem"),
        "/app/test/resources/rds-ca-rsa2048-g1.pem");
    copies.put(MODULE.resolve("src/test/resources/logging-test.properties"),
        "/app/test/resources/logging-test.properties");
    copies.put(MODULE.resolve("src/test/resources/simplelogger.properties"),
        "/app/test/resources/simplelogger.properties");
    copies.put(MODULE.resolve("src/test/resources/junit-platform.properties"),
        "/app/test/resources/junit-platform.properties");
    copies.put(MODULE.resolve("src/test/resources/certs/ca.crt"),
        "/app/test/resources/certs/ca.crt");

    return copies;
  }

  /**
   * Returns the directories bound so results survive the container.
   *
   * <p>The same three the harness binds, plus the Gradle directory it shares. Without these a suite runs
   * correctly and publishes nothing, which reads as a green build with no tests.
   *
   * <p>Plus one that goes the other way, when {@code orchestra-aws-credentials-bind} asks for it: the host's
   * AWS credentials directory. A bind rather than a copy for the same reason the others are copies — a copy
   * is a snapshot, and this is the one input that changes while a run is in progress.
   */
  @Override
  public Map<Path, String> getTestContainerBinds() {
    final Map<Path, String> binds = new LinkedHashMap<>();

    // Created here rather than assumed to exist, because a bind whose host path is missing does not fail -
    // it fails later, differently each time, in a way that points nowhere near the cause.
    //
    // Clearing build/test-results before a run is normal practice here: stale JUnit XML from an earlier run
    // otherwise gets counted as this run's results, which has already produced one wrong conclusion. But it
    // deletes a bind source. What follows depends on the container runtime and is not a missing-directory
    // error either way: one run silently published no XML at all, and the next died on "Failed to create
    // parent directory '/app/build/test-results/in-container'" - reported as the whole composition failing,
    // after twelve minutes of provisioning, with no test having run.
    binds.put(outputDirectory("build/reports/tests"), "/app/build/reports/tests");
    binds.put(outputDirectory("build/test-results"), "/app/build/test-results");
    binds.put(outputDirectory("build/jacoco"), "/app/build/jacoco");
    binds.put(MODULE.resolve("../gradle"), "/app/gradle");

    // The host's AWS credentials directory, bound rather than copied, and the only bind here that exists to
    // get something *in* rather than out.
    //
    // This is what lets a long run survive its own credentials. A developer's session token lasts one hour;
    // Multi-AZ provisioning alone spends a quarter of that before a test runs, and the suite needs more than
    // the rest. Copied credentials are a snapshot, so the container's token died mid-suite and 28 tests
    // failed on "The security token included in the request is expired" while the cluster was perfectly
    // healthy. A bind means the file the container reads is the file that gets refreshed, so a refresh
    // reaches a run already in progress.
    //
    // The refreshing itself is somebody else's job - on a developer machine a scheduled task rewrites the
    // file every half hour. This only makes that reach the container.
    //
    // Opt-in, not automatic: CI has no ~/.aws and does not need one, because the credentials it issues
    // outlast a run. See usesHostCredentialsFile.
    //
    // The directory rather than the credentials file: a bind of a single file breaks when the file is
    // replaced rather than rewritten in place, which is exactly what credential-refresh tools do, and the
    // container would go on reading the old inode. Binding the directory also brings config alongside
    // credentials, which the SDK reads together.
    if (usesHostCredentialsFile()) {
      binds.put(AWS_DIRECTORY, CONTAINER_AWS_DIRECTORY);
    }
    return binds;
  }

  /**
   * Returns a bind source under the module, creating it if it is not there.
   *
   * <p>Only for the directories the container writes into. The AWS credentials directory is deliberately not
   * handled this way: creating it would produce an empty one and hide the real problem, so it is checked and
   * reported instead.
   *
   * @param relative the path relative to the wrapper module
   * @return the path, which exists by the time it is returned
   */
  static Path outputDirectory(final String relative) {
    final Path path = MODULE.resolve(relative);
    try {
      Files.createDirectories(path);
    } catch (final IOException e) {
      throw new IllegalStateException(
          "Could not create " + path.toAbsolutePath() + ", which the test container needs to publish results "
              + "into.", e);
    }
    return path;
  }

  /**
   * Returns the AWS credentials and region the in-container suite needs.
   *
   * <p>The harness did the same thing at {@code TestEnvironment}'s {@code AWS_CREDENTIALS_ENABLED} branch,
   * and it is not optional for this slice. Two-thirds of the suite calls the RDS API from inside the
   * container: the failover tests trigger a failover through it, and {@code AuroraTestUtility} builds a
   * client during setup, so without credentials the failures are not confined to the IAM tests — a run with
   * this method removed failed 103 of 111 tests, nearly all of them on
   * {@code Unable to load credentials from any of the providers in the chain}.
   *
   * <p>There are two ways to hand them over, chosen by {@code orchestra-aws-credentials-bind}. By default the
   * credentials are resolved once and copied, which is right for CI: credentials there last six hours,
   * comfortably longer than a run, and there is no credentials file to point at. With the property set the
   * container is instead pointed at the host's credentials file, which is what a developer's one-hour token
   * needs. {@link #usesHostCredentialsFile()} has the reasoning.
   *
   * <p>Either way nothing secret reaches Orchestra's context document, which describes what was provisioned
   * and is written to logs and reports; a session token is neither part of that description nor something to
   * persist. With the bind, nothing secret passes through this method at all.
   *
   * <h2>Why the bind exists at all</h2>
   *
   * <p>Because copying was the only option once, and it cost a Multi-AZ run. Environment variables are set
   * when the container is created and never again, so a copied token is frozen at that moment. A developer's
   * session token lasts one hour, Multi-AZ provisioning spends a quarter of it before a test runs, and the
   * suite needs more than what is left: the token expired mid-suite and 28 tests failed on
   * {@code The security token included in the request is expired} against a perfectly healthy cluster.
   *
   * <p>With the property set, the container is pointed at the credentials file instead, bound from the host by
   * {@link #getTestContainerBinds()}. The file the container reads is the file that gets refreshed, so a
   * refresh reaches a run already in progress and the run is no longer bounded by the token it started with.
   * Nothing here does the refreshing; on a developer machine a scheduled task rewrites the file every half
   * hour.
   *
   * <p>The two are never combined. The default provider chain checks environment variables before the
   * profile, so setting the static keys alongside the pointer would pin the container to the snapshot and
   * make the bind useless.
   *
   * <p>The bind only pays off if the in-container SDK re-reads the file, and that turns on how each client is
   * built rather than on anything set here. A {@code DefaultCredentialsProvider} handed to a client
   * explicitly picks up a rewritten credentials file — measured against the SDK version the in-container
   * build uses, resolving the new key after the file's modification time changed. A client builder left
   * without any provider does not: it gets one pinned to a static profile snapshot
   * (aws/aws-sdk-java-v2#5073). The integration tests supply one explicitly for that reason, so a client
   * built during setup still works after the host refreshes.
   */
  @Override
  public Map<String, String> getTestContainerEnvironment() {
    final Map<String, String> environment = new LinkedHashMap<>();

    // The region is set either way. It is not in the credentials file, an IAM auth token is signed for a
    // region, and the SDK will not guess one inside a container with no instance metadata.
    environment.put("AWS_REGION", getAwsRegion());

    // Before the branch below, so both apply whichever way credentials reach the container. Neither is a
    // credential, and neither has anything to do with which of those two paths a run takes.
    addKmsKey(environment);
    addRepeatTimes(environment);

    if (usesHostCredentialsFile()) {
      // Both paths are named explicitly, and the config one points at nothing on purpose - see
      // CONTAINER_AWS_CONFIG_FILE. Unsetting it is not equivalent: the SDK would fall back to a default
      // location and read the host's config after all.
      environment.put("AWS_SHARED_CREDENTIALS_FILE", CONTAINER_AWS_CREDENTIALS_FILE);
      environment.put("AWS_CONFIG_FILE", CONTAINER_AWS_CONFIG_FILE);
      environment.put("AWS_PROFILE", profileName());
      // No static keys alongside them. The default chain checks environment variables before the profile, so
      // setting both would pin the container to a snapshot and make the bind pointless.
      return environment;
    }

    // The default: resolve once and copy, which is what the harness did. Right for CI, where credentials
    // arrive as environment variables, last longer than a run, and leave nothing on disk to bind.
    //
    // Resolution goes through AwsClients.credentials so the container authenticates as exactly the identity
    // that provisioned the cluster. Resolving independently here could point the suite at a different
    // account than the one it is testing against, and that failure reads as a broken test.
    final AwsCredentials credentials = AwsClients.credentials(this).resolveCredentials();
    environment.put("AWS_ACCESS_KEY_ID", credentials.accessKeyId());
    environment.put("AWS_SECRET_ACCESS_KEY", credentials.secretAccessKey());
    if (credentials instanceof AwsSessionCredentials) {
      environment.put("AWS_SESSION_TOKEN", ((AwsSessionCredentials) credentials).sessionToken());
    }
    return environment;
  }

  /**
   * Returns whether the container should read the host's credentials file instead of copied keys.
   *
   * <p>Opt-in through {@code orchestra-aws-credentials-bind}, and off by default, because the two
   * environments this runs in have genuinely different needs rather than one being a degraded version of the
   * other. CI issues credentials that last six hours, comfortably longer than a run, so a copy is correct
   * there and there is no {@code ~/.aws} to bind anyway. A developer's session token lasts one hour, less
   * than a Multi-AZ run, and is refreshed on a timer — so the file is the only thing that stays current, and
   * binding it is the only way that reaches the container.
   *
   * <p>Deciding this by inspection was the first attempt and it is worse than a flag. Guessing from whether
   * the file exists and whether the host has credential environment variables set gets the common cases
   * right and then picks silently in the ambiguous ones — a stale {@code ~/.aws/credentials} next to fresh
   * environment keys would have the container authenticate as something else entirely, with nothing in the
   * output to say a choice had been made. A flag makes the choice visible in the command that started the
   * run.
   *
   * @return whether to bind the host's credentials directory
   * @throws IllegalStateException if the bind is requested but the host has no credentials file, since
   *     binding a missing directory does not fail — Docker creates it empty and the container resolves no
   *     credentials at all, which surfaces much later as every AWS-touching test failing
   */
  private boolean usesHostCredentialsFile() {
    if (!Boolean.parseBoolean(System.getProperty(BIND_CREDENTIALS_PROPERTY, "false"))) {
      return false;
    }

    final Path file = AWS_DIRECTORY.resolve("credentials");
    if (!Files.isRegularFile(file)) {
      throw new IllegalStateException(
          BIND_CREDENTIALS_PROPERTY + " is set, but " + file.toAbsolutePath() + " does not exist. Binding it "
              + "anyway would give the container an empty directory and fail every test that calls AWS. "
              + "Either sign in so the file exists, or drop the property and let the resolved keys be "
              + "copied instead.");
    }
    requireStaticKeys(file);
    return true;
  }

  /**
   * Checks, before anything is provisioned, that the bound file will actually authenticate the container.
   *
   * <p>Because the alternative is finding out an hour later. The container reads this file and nothing else —
   * no {@code credential_process}, no SSO, no assumed role, since none of those can run in there — so the
   * named profile has to carry keys directly. When it does not, every AWS-touching test fails with
   * {@code Unable to load credentials from any of the providers in the chain}, long after a cluster has been
   * built and while the file the run was pointed at sits there looking perfectly fine.
   *
   * <p>Only key <em>names</em> are looked for. Nothing read here is kept, logged, or included in the message.
   */
  private void requireStaticKeys(final Path file) {
    final String profile = profileName();
    boolean inProfile = false;
    try {
      for (final String rawLine : Files.readAllLines(file)) {
        final String line = rawLine.trim();
        if (line.startsWith("[")) {
          // Section headers in the credentials file are bare names, unlike config's "profile <name>" form.
          inProfile = line.equals("[" + profile + "]");
        } else if (inProfile && line.startsWith("aws_access_key_id")) {
          return;
        }
      }
    } catch (final IOException e) {
      throw new IllegalStateException(
          "Could not read " + file.toAbsolutePath() + " to check that profile " + profile + " has credentials "
              + "the container can use.", e);
    }

    throw new IllegalStateException(
        BIND_CREDENTIALS_PROPERTY + " is set, but profile " + profile + " in " + file.toAbsolutePath()
            + " has no aws_access_key_id. The container can only use keys written into this file - it cannot "
            + "run credential_process, SSO, or an assumed-role flow. Refresh the profile so it holds keys, or "
            + "drop the property and let the host's resolved credentials be copied instead.");
  }

  /**
   * Returns the profile the container should read, matching the one the host provisions with.
   *
   * <p>{@code default} unless {@link #getAwsProfile()} says otherwise, which is what keeps the container
   * authenticating as the identity that created the cluster. Reading a different profile would let the suite
   * talk to a different account than the one it is testing against, and that failure would look like a
   * broken test rather than a configuration mistake.
   */
  private String profileName() {
    final String profile = getAwsProfile();
    return profile == null || profile.trim().isEmpty() ? "default" : profile.trim();
  }

  /**
   * Returns the system properties forwarded to the in-container Gradle.
   *
   * <p>Tag filters and shard coordinates, read from the host JVM's own properties so the existing CI
   * invocations keep working unchanged. The harness forwarded exactly these.
   *
   * <p>The suite's own tags go in first and an explicitly passed {@code test-include-tags} or
   * {@code test-exclude-tags} replaces them. That order is the useful one: the performance suites are
   * separated from each other by tags rather than by features, so the suite has to supply them or the two
   * would run each other's tests - while someone naming a tag on the command line is answering a narrower
   * question than the suite's default and should win.
   */
  @Override
  public Map<String, String> getTestContainerSystemProperties() {
    final Map<String, String> properties = new LinkedHashMap<>();

    final TestSuite suite = TestSuite.requested();
    if (suite.includeTags() != null) {
      properties.put("test-include-tags", suite.includeTags());
    }
    if (suite.excludeTags() != null) {
      properties.put("test-exclude-tags", suite.excludeTags());
    }

    forward(properties, "test-include-tags");
    forward(properties, "test-exclude-tags");
    forward(properties, "test-shard-index");
    forward(properties, "test-shard-count");
    // Explicit class selection. Forwarded for the reason the in-container build introduced it: a full run
    // takes longer than a temporary session token lives, so verifying one behaviour has to be able to run
    // just the classes that exercise it. Without this the property stopped at the host JVM and the
    // container ran everything regardless.
    forward(properties, "test-classes");
    return properties;
  }

  /**
   * Adds the KMS key the encryption suite needs, if this run is that suite.
   *
   * <p>An input rather than something the composition creates, because the key outlives the run by design:
   * the workflow driving this suite looks up {@code alias/jdbc-encryption-key}, creates it only if absent,
   * and deliberately never deletes it - a KMS key cannot be deleted promptly, so a per-run key would leave a
   * trail of pending deletions.
   *
   * <p>Published under both names, which looks like belt and braces and is not. The harness passes
   * {@code KMS_KEY_ID}, from the host variable of the same name that its workflow sets, while
   * {@code KmsEncryptionIntegrationTest} reads {@code AWS_KMS_KEY_ARN}. That mismatch is not benign: the
   * test's precondition is {@code assumeTrue}, so a missing key makes it report as skipped rather than
   * failed, and as wired in the harness today the encryption suite passes having encrypted nothing. Setting
   * both names is what makes the test actually run here, and keeping the harness's name means anything else
   * reading it is unaffected.
   *
   * <p>KMS accepts a key id or an ARN wherever it takes a key, so the value the workflow supplies works
   * under either name.
   *
   * <p>Silent when no key is configured, because the runner has already rejected that combination before
   * anything is provisioned. Throwing here as well would put the same check in two places and report it from
   * the later one.
   */
  private static void addKmsKey(final Map<String, String> environment) {
    if (!TestSuite.requested().needsKmsKey()) {
      return;
    }

    final String key = kmsKey();
    if (key == null) {
      return;
    }
    environment.put("KMS_KEY_ID", key);
    environment.put("AWS_KMS_KEY_ARN", key);
  }

  /**
   * Passes through how many times the performance suites repeat each measurement, when a run asks.
   *
   * <p>{@code REPEAT_TIMES}, which {@code PerformanceTest}, {@code AdvancedPerformanceTest} and
   * {@code ReadWriteSplittingPerformanceTest} read from the environment, defaulting to 5, 5 and 10. The
   * harness never sets it, so those defaults are what CI measures with - and they are the right defaults for a
   * measurement, since a single sample of a failure-detection time is not a number anyone should act on.
   *
   * <p>Worth being able to override anyway, because the two purposes differ. Measuring wants repetition;
   * checking that a suite is selected, provisioned and runs wants one pass. The measurement matrix is
   * multiplied by this value - {@code AdvancedPerformanceTest} alone builds six parameter sets per repeat, and
   * each measurement contains a deliberate sleep - so a validation run at the default spends hours confirming
   * something the first pass already showed.
   *
   * <p>Unset means unset: nothing is put in the environment, so the tests see exactly what they see under the
   * harness. Read from the host's own {@code REPEAT_TIMES} first so a CI job that exports it keeps working,
   * then from {@code -Dorchestra-repeat-times}.
   */
  private static void addRepeatTimes(final Map<String, String> environment) {
    final String fromEnvironment = System.getenv("REPEAT_TIMES");
    if (fromEnvironment != null && !fromEnvironment.isBlank()) {
      environment.put("REPEAT_TIMES", fromEnvironment.trim());
      return;
    }

    final String fromProperty = System.getProperty("orchestra-repeat-times");
    if (fromProperty != null && !fromProperty.isBlank()) {
      environment.put("REPEAT_TIMES", fromProperty.trim());
    }
  }

  /**
   * Returns the KMS key for the encryption suite, or {@code null} if none was supplied.
   *
   * <p>The host's {@code KMS_KEY_ID} variable first, which is what CI sets and therefore what makes the
   * migrated task a drop-in for {@code test-kms-encryption}, then {@code -Dorchestra-kms-key} for a local run
   * that would rather pass it as a property than export it.
   *
   * @return the key id or ARN, or {@code null}
   */
  static String kmsKey() {
    final String fromEnvironment = System.getenv("KMS_KEY_ID");
    if (fromEnvironment != null && !fromEnvironment.isBlank()) {
      return fromEnvironment.trim();
    }

    final String fromProperty = System.getProperty("orchestra-kms-key");
    if (fromProperty != null && !fromProperty.isBlank()) {
      return fromProperty.trim();
    }
    return null;
  }

  /**
   * Returns the one driver jar the container should get, named by the build.
   *
   * <p>A single file rather than the {@code build/libs} directory, which is what this replaced. That
   * directory accumulates: a real checkout had six jars in it - three driver versions plus a sources jar, a
   * javadoc jar and a shaded federated-auth bundle - and copying the lot put all of them on the
   * in-container test classpath, because the build there globs {@code libs/*.jar}.
   *
   * <p>The failure that produced was not a classpath warning. The bundle carries a {@code module-info}
   * requiring {@code commons.math3}, which is absent, so the test JVM died during boot layer
   * initialisation - before any test ran, with no JUnit XML, reported only as "Gradle Test Executor 1
   * finished with non-zero exit value 1". Worse, it is intermittent: it depends on what happens to be
   * left in {@code build/libs}, so the same code passes or fails according to what was built earlier.
   *
   * <p>The path comes from {@code orchestra-driver-jar}, which the Gradle task sets from the {@code jar}
   * task's own output. The build knows which jar it just produced; deriving it here by pattern-matching
   * filenames would be guessing at exactly the ambiguity that caused the problem.
   *
   * @return the driver jar to copy
   * @throws IllegalStateException if the property is unset or points at nothing, since a container without
   *     the driver fails much later and far less clearly
   */
  static Path driverJar() {
    final String configured = System.getProperty("orchestra-driver-jar");
    if (configured == null || configured.trim().isEmpty()) {
      throw new IllegalStateException(
          "The system property orchestra-driver-jar is not set, so the driver jar to give the container is "
              + "unknown. The orchestra-test task sets it from the jar task's output; running this test "
              + "outside that task needs it supplied.");
    }

    final Path jar = Path.of(configured);
    if (!Files.isRegularFile(jar)) {
      throw new IllegalStateException(
          "orchestra-driver-jar points at " + jar.toAbsolutePath() + ", which is not a file. The driver "
              + "has probably not been built.");
    }
    return jar;
  }

  private static void forward(final Map<String, String> properties, final String name) {
    final String value = System.getProperty(name);
    if (value != null && !value.isBlank()) {
      properties.put(name, value);
    }
  }

  private static String generatePassword() {
    final byte[] bytes = new byte[24];
    new SecureRandom().nextBytes(bytes);
    // RDS rejects '/', '@', '"' and space in a master password; a backslash is legal but has to be escaped
    // everywhere it is written, so it is avoided too.
    return "Or" + Base64.getEncoder().withoutPadding().encodeToString(bytes)
        .replace('/', 'x')
        .replace('+', 'y')
        .replace('\\', 'z');
  }
}
