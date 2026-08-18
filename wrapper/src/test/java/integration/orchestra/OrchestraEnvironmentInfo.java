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
import integration.DatabaseInstances;
import integration.TargetJvm;
import integration.TestDatabaseInfo;
import integration.TestEnvironmentFeatures;
import integration.TestEnvironmentInfo;
import integration.TestEnvironmentRequest;
import integration.TestGlobalDatabaseInfo;
import integration.TestInstanceInfo;
import integration.TestProxyDatabaseInfo;
import integration.TestRegionalClusterInfo;
import integration.TestTelemetryInfo;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import software.amazon.orchestra.client.OrchestraClient;
import software.amazon.orchestra.contract.BlueGreenDeployment;
import software.amazon.orchestra.contract.CacheState;
import software.amazon.orchestra.contract.Database;
import software.amazon.orchestra.contract.DatabaseClusterState;
import software.amazon.orchestra.contract.GlobalDatabase;
import software.amazon.orchestra.contract.GlobalDatabaseState;
import software.amazon.orchestra.contract.InstrumentContext;
import software.amazon.orchestra.contract.NetworkGateway;
import software.amazon.orchestra.contract.TelemetryBackendState;

/**
 * Builds the harness's {@link TestEnvironmentInfo} from Orchestra's composition context.
 *
 * <p>The adapter the migration turns on. {@code TestEnvironment.getCurrent()} has roughly 437 call sites, so
 * converting them to call {@code OrchestraClient} directly would not be a migration but a rewrite with no
 * safe intermediate state. Instead the facade those call sites already use is rebuilt on top of the client:
 * the in-container code genuinely reads its environment through Orchestra, and not one of the 437 changes.
 *
 * <p>Java 8, like the rest of the {@code test} source set, because these classes run inside the container
 * against every JVM the wrapper supports. {@code orchestra-contract} and {@code orchestra-client-java} are
 * Java 8 for exactly this reason.
 *
 * <h2>Where the fields come from</h2>
 *
 * <p>Endpoints, credentials and topology come from {@link DatabaseClusterState}, which Aurora and RDS
 * Multi-AZ both publish. The engine, deployment kind, instance count and feature set come from
 * {@link TestShape}, because Orchestra publishes what a composition contains rather than what kind of run it
 * is.
 *
 * <p>Several fields the harness carries have no source here and are left null, which is a decision rather
 * than an omission. {@code awsAccessKeyId} and friends are absent because the container resolves credentials
 * through the SDK's own chain, from a credentials file bound in from the host. Publishing keys here would be
 * a second, weaker copy, and a frozen one: a session token lasts an hour, less than a full run, so anything
 * that copies it turns a healthy cluster into "the security token included in the request is expired". Left
 * null, the call sites that check them fall through to the default provider, which re-reads the file.
 * {@code randomBase}, {@code clusterParameterGroupName} and {@code dbParameterGroupName} are absent because
 * they exist so the harness can clean up after itself, and Orchestra's teardown and orphan sweep own that
 * now — a test reading them would be reaching for a responsibility it no longer has.
 */
public final class OrchestraEnvironmentInfo {

  private OrchestraEnvironmentInfo() {
  }

  /**
   * Reads the context and builds the environment description the suite expects.
   *
   * @return the environment info
   * @throws RuntimeException if the context is missing or does not describe a database
   */
  public static TestEnvironmentInfo read() {
    final OrchestraClient orchestra = OrchestraClient.fromEnvironment();
    final InstrumentContext shape = orchestra.getContext(TestShape.class);

    // A cluster's rendered fields are a superset of a single database's, with host and port carrying the
    // writer endpoint, so one typed view reads either. What differs is the instance list: a container
    // publishes none, and the tests take index 0 to be the writer - hence the synthesised entry below.
    final DatabaseClusterState cluster = orchestra.getContext(Database.class, DatabaseClusterState.class);
    final boolean single = DatabaseEngineDeployment.DOCKER.name().equals(shape.getString("deployment"));

    final TestEnvironmentInfo info = new TestEnvironmentInfo();
    info.setRequest(request(shape, cluster));
    info.setRegion(single ? null : region(shape));
    // The RDS engine name, not the driver's. The two differ and this field is read by control-plane calls:
    // AuroraTestUtility.createInstance passes it straight to CreateDBInstance, which rejects "postgresql"
    // with "Invalid DB engine" - which is how this was found, by the autoscaling suite failing to add the
    // instance it exists to add. A container has no RDS name and makes no such call, so it keeps the
    // driver's.
    info.setDatabaseEngine(single ? cluster.engine() : rdsEngineName(orchestra));
    info.setDatabaseEngineVersion(single ? null : rdsEngineVersion(orchestra));
    info.setDatabaseInfo(single ? containerDatabaseInfo(cluster) : databaseInfo(cluster));
    info.setRdsDbName(single ? null : clusterIdentifier(orchestra));
    info.setIamUsername(shape.getString("iamUsername"));

    if (info.getRequest().getFeatures().contains(TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED)) {
      info.setProxyDatabaseInfo(
          single ? containerProxyDatabaseInfo(cluster) : proxyDatabaseInfo(cluster));
    }

    if (info.getRequest().getFeatures().contains(TestEnvironmentFeatures.VALKEY_CACHE)) {
      setValkeyInfo(info, orchestra);
    }

    if (info.getRequest().getFeatures().contains(TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT)) {
      info.setBlueGreenDeploymentId(blueGreenDeploymentId(orchestra));
    }

    if (info.getRequest().getFeatures().contains(TestEnvironmentFeatures.GLOBAL_DATABASE)) {
      info.setGlobalDatabaseInfo(globalDatabaseInfo(orchestra));
    }

    if (info.getRequest().getFeatures().contains(TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED)) {
      info.setTracesTelemetryInfo(telemetryInfo(orchestra, TelemetryRoles.Traces.class, "traces"));
    }

    if (info.getRequest().getFeatures().contains(TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED)) {
      info.setMetricsTelemetryInfo(telemetryInfo(orchestra, TelemetryRoles.Metrics.class, "metrics"));
    }

    return info;
  }

  /**
   * Reads one telemetry backend as the container must address it.
   *
   * <p>Container-internal by construction rather than by choice made here: what a telemetry state renders
   * into a workload context is the network alias and the container port, not the published host port. That
   * is the only view that works, because the exporter runs inside the test container and reaches the backend
   * across the Docker network - {@code localhost:32771} would resolve to the test container itself.
   *
   * <p>Split into host and port rather than using the state's ready-made {@code endpoint()}, because the
   * harness stores the two separately and composes the string itself, differently per backend:
   * {@code host:port} for the X-Ray daemon and {@code http://host:port} for OTLP. Those are the same two
   * forms {@code endpoint()} produces, so the composed values agree; passing the endpoint through as a
   * hostname would produce {@code http://http://...} at the metrics exporter.
   *
   * @param role which backend to read
   * @param purpose what it carries, for the error message
   * @throws RuntimeException if the composition declared the feature but published no usable backend
   */
  private static TestTelemetryInfo telemetryInfo(
      final OrchestraClient orchestra, final Class<?> role, final String purpose) {

    final TelemetryBackendState backend = orchestra.getContext(role, TelemetryBackendState.class);

    if (backend.host() == null || backend.host().trim().isEmpty() || backend.port() <= 0) {
      throw new RuntimeException(
          "The composition declares " + purpose + " telemetry but its backend published no reachable "
              + "endpoint, so every connection would be configured to export somewhere that does not "
              + "resolve.");
    }
    return new TestTelemetryInfo(backend.host(), backend.port());
  }

  /**
   * Describes a single database container the way the tests expect a topology to look.
   *
   * <p>The one instance is synthesised rather than read, because a container publishes no instance list and
   * a great many tests take {@code getInstances().get(0)} to be the writer - {@code getWrapperUrl()} with no
   * arguments resolves to exactly that. Without an entry those tests fail on an empty list, which reads as a
   * missing topology rather than a single-node one.
   *
   * <p>Its identifier is the container's hostname. Nothing in a Docker environment has an AWS instance id,
   * and the hostname is the only name that both identifies the container and can be connected to, which is
   * what {@code ProxyHelper} needs when a test impairs it by name.
   *
   * <p>No cluster endpoints. A single container has no writer or reader endpoint distinct from itself, and
   * leaving them null is what makes {@code @EnableOnDatabaseEngineDeployment} conditions and the
   * cluster-endpoint tests skip rather than connect somewhere meaningless.
   */
  private static TestDatabaseInfo containerDatabaseInfo(final DatabaseClusterState database) {
    final TestDatabaseInfo info = new TestDatabaseInfo();

    info.setUsername(database.username());
    info.setPassword(database.password());
    info.setDefaultDbName(database.databaseName());
    info.getInstances().add(
        new TestInstanceInfo(database.writerHost(), database.writerHost(), database.port()));

    return info;
  }

  /**
   * The proxied view of a single container, which under the gateway is the same view.
   *
   * <p>Same reasoning as {@link #proxyDatabaseInfo}: the gateway routes rather than proxies, so there is no
   * second address to publish and impairment is applied to the endpoint the test is already using.
   */
  private static TestProxyDatabaseInfo containerProxyDatabaseInfo(final DatabaseClusterState database) {
    final TestProxyDatabaseInfo proxy = new TestProxyDatabaseInfo();

    proxy.setUsername(database.username());
    proxy.setPassword(database.password());
    proxy.setDefaultDbName(database.databaseName());
    proxy.getInstances().add(
        new TestInstanceInfo(database.writerHost(), database.writerHost(), database.port()));

    return proxy;
  }

  /**
   * Reads the blue/green deployment's identifier.
   *
   * <p>The only thing {@code BlueGreenDeploymentTests} needs beyond what a normal run publishes: it names the
   * deployment in the RDS calls that trigger the switchover and that list the deployment's endpoints. The
   * region, IAM user and credentials it also uses are already published.
   *
   * <p>Read as a raw field rather than through a typed view, because {@code orchestra-contract} has no
   * {@code BlueGreenDeploymentState} class - the host state names that type but the counterpart was never
   * added - so there is nothing to project onto. One string does not justify adding one.
   *
   * @throws RuntimeException if the deployment published no identifier, which would otherwise surface as an
   *     RDS call against a null deployment and read as an AWS problem
   */
  private static String blueGreenDeploymentId(final OrchestraClient orchestra) {
    final InstrumentContext deployment = orchestra.getContext(BlueGreenDeployment.class);
    final String identifier = deployment.getString("deploymentIdentifier");

    if (identifier == null || identifier.trim().isEmpty()) {
      throw new RuntimeException(
          "The composition published a blue/green deployment with no identifier, so the switchover tests "
              + "have nothing to act on.");
    }
    return identifier;
  }

  /**
   * Assembles the cache list the query-cache and Spring caching tests index into.
   *
   * <p>Positional, and that is the contract rather than an implementation detail: those tests select index 0
   * for authenticated plaintext, 1 for anonymous plaintext, 2 for authenticated TLS and 3 for anonymous TLS,
   * and they guard on {@code size()} to decide whether a capability is present. So the roles are read in the
   * order {@link CacheRoles} documents, and reordering them here would silently point tests at the wrong
   * cache - a TLS test would pass over plaintext.
   *
   * <p>The credentials are taken from the authenticated cache rather than passed separately, because the
   * harness exposes one username and password for all caches while Orchestra publishes them per cache. Index
   * 0 is the authoritative one; the anonymous caches publish none, and the TLS authenticated cache publishes
   * the same pair.
   */
  private static void setValkeyInfo(final TestEnvironmentInfo info, final OrchestraClient orchestra) {
    final TestDatabaseInfo caches = new TestDatabaseInfo();

    // In CacheRoles' documented order. Anything else would break the index contract above.
    final CacheState authenticated = addCache(caches, orchestra, CacheRoles.AuthCache.class);
    addCache(caches, orchestra, CacheRoles.NoAuthCache.class);
    addCache(caches, orchestra, CacheRoles.TlsAuthCache.class);
    addCache(caches, orchestra, CacheRoles.TlsNoAuthCache.class);

    info.setValkeyServerInfo(caches);
    info.setValkeyServerUsername(authenticated.username());
    info.setValkeyServerPassword(authenticated.password());
  }

  /**
   * Appends one cache to the list, named for its role.
   *
   * <p>The instance id is the role's simple name because nothing looks these up by id - the tests index the
   * list - and a name like {@code TlsAuthCache} makes a log line legible in a way a generated identifier
   * would not.
   */
  private static CacheState addCache(
      final TestDatabaseInfo caches, final OrchestraClient orchestra, final Class<?> role) {

    final CacheState cache = orchestra.getContext(role, CacheState.class);
    caches.getInstances().add(new TestInstanceInfo(role.getSimpleName(), cache.host(), cache.port()));
    return cache;
  }

  /**
   * Returns the proxied view of the database, which under Orchestra is the same view.
   *
   * <p>The one part of the migration that is a translation rather than a mapping. Toxiproxy listens on a
   * port per endpoint, so the harness published a <em>second</em> set of hostnames -
   * {@code host.proxied:PROXY_PORT} - and every impairable test connected there instead of to the real
   * database. Orchestra's gateway is transparent: it sits in the network path and filters what it
   * forwards, so a client connects to the real endpoint and there is no second address to publish.
   *
   * <p>So this mirrors {@link #databaseInfo}, and that is not a placeholder standing in for something
   * better - it is what "proxied endpoint" means once the proxy is a router. Tests calling
   * {@code getProxyDatabaseInfo()} get working endpoints, and the impairment they ask for is applied
   * through the gateway's control API rather than by connecting somewhere else.
   *
   * <p>The control port is the gateway's. Unlike Toxiproxy, which ran one container per database host,
   * there is a single control endpoint for the whole composition, which is why {@code initProxies} needs
   * its own branch for this path rather than deriving a control address per instance.
   */
  private static TestProxyDatabaseInfo proxyDatabaseInfo(final DatabaseClusterState cluster) {
    final TestProxyDatabaseInfo proxy = new TestProxyDatabaseInfo();

    proxy.setUsername(cluster.username());
    proxy.setPassword(cluster.password());
    proxy.setDefaultDbName(cluster.databaseName());
    proxy.setClusterEndpoint(cluster.writerHost(), cluster.port());
    proxy.setClusterReadOnlyEndpoint(cluster.readerHost(), cluster.port());
    proxy.setInstanceEndpointSuffix(instanceEndpointSuffix(cluster), cluster.port());
    proxy.setControlPort(gatewayControlPort());

    for (final DatabaseClusterState.Instance instance : writerFirst(cluster)) {
      proxy.getInstances().add(
          new TestInstanceInfo(instance.identifier(), instance.host(), instance.port()));
    }

    return proxy;
  }

  /**
   * Returns the cluster's instances with the writer first.
   *
   * <p>Ordered rather than passed through, because a surprising number of tests take index 0 to be the
   * writer without saying so. {@code RemoteQueryCachePluginTests} connects with
   * {@code ConnectionStringHelper.getWrapperUrl()} - which resolves to the first instance - and then runs
   * {@code DROP TABLE}; pointed at a reader it fails with "cannot execute DROP TABLE in a read-only
   * transaction", which reads like a caching bug rather than a topology one. That is exactly how it
   * presented.
   *
   * <p>The harness never guaranteed this either: its list is whatever {@code describeDBInstances} returned,
   * and {@code @MakeSureFirstInstanceWriter} reorders it only for the classes carrying that annotation. The
   * classes above pass there by luck of API ordering. Since Orchestra publishes the writer's hostname
   * explicitly, the order can be made deterministic here instead of left to chance.
   *
   * <p>Identified by the flag each instance publishes, not by comparing hostnames against
   * {@code writerHost()}. Those never match on Aurora: the writer endpoint is the cluster endpoint
   * ({@code name.cluster-xxx.rds.amazonaws.com}) while an instance publishes its own
   * ({@code name-1.xxx.rds.amazonaws.com}). An earlier attempt compared them, matched nothing, and left the
   * order untouched - the tests failed identically and looked unfixed.
   *
   * <p>The flag is a snapshot of who was writer at provisioning time, which is the right basis here:
   * ordering happens once when the environment is read, and a test that cares about the live role after a
   * failover re-reads it from the database anyway.
   *
   * <p>A cluster reporting no writer is returned unchanged rather than reordered arbitrarily.
   */
  private static List<DatabaseClusterState.Instance> writerFirst(final DatabaseClusterState cluster) {
    final List<DatabaseClusterState.Instance> ordered = new ArrayList<DatabaseClusterState.Instance>();
    DatabaseClusterState.Instance writer = null;

    for (final DatabaseClusterState.Instance instance : cluster.instances()) {
      if (writer == null && instance.writer()) {
        writer = instance;
      } else {
        ordered.add(instance);
      }
    }

    if (writer == null) {
      return cluster.instances();
    }
    ordered.add(0, writer);
    return ordered;
  }

  /**
   * Returns the gateway's control API address as {@code host:port}.
   *
   * <p>Read from the composition rather than from {@code ORCHESTRA_GATEWAY_URL}, so the gateway is found
   * by its role exactly as the database is. The environment variable carries the same value, but relying
   * on it would mean two mechanisms had to agree.
   *
   * @return the authority part of the gateway's internal control URL
   * @throws RuntimeException if the composition published no gateway
   */
  public static String gatewayControlAuthority() {
    final InstrumentContext gateway =
        OrchestraClient.fromEnvironment().getContext(NetworkGateway.class);
    final String url = gateway.getString("controlUrl");

    if (url == null || url.trim().isEmpty()) {
      throw new RuntimeException(
          "The composition published no gateway control URL, so network impairment cannot be driven. A "
              + "composition declaring NETWORK_OUTAGES_ENABLED must include a network gateway.");
    }

    // http://alias:8474 -> alias:8474. Deliberately not java.net.URI: Orchestra produces this value in
    // exactly this shape, and a parse failure would be reported as a malformed URI rather than as the
    // real problem, which would be a gateway that published something unexpected.
    final int scheme = url.indexOf("://");
    final String authority = scheme < 0 ? url : url.substring(scheme + 3);
    return authority.endsWith("/") ? authority.substring(0, authority.length() - 1) : authority;
  }

  /** Returns the port half of {@link #gatewayControlAuthority()}. */
  private static int gatewayControlPort() {
    final String authority = gatewayControlAuthority();
    final int colon = authority.lastIndexOf(':');
    if (colon < 0) {
      throw new RuntimeException(
          "The gateway control URL '" + authority + "' has no port, so its API cannot be reached.");
    }
    return Integer.parseInt(authority.substring(colon + 1));
  }

  /**
   * Returns the engine name the RDS API expects, for example {@code aurora-postgresql}.
   *
   * <p>Read as a raw field for the same reason {@link #clusterIdentifier} is: the typed cluster view carries
   * what a test needs in order to <em>connect</em>, and this is the opposite - it is only ever passed to the
   * control plane. The database instrument publishes it beside the driver's name precisely because neither
   * can be derived from the other.
   *
   * <p>Not optional on an AWS deployment. Absent, {@code CreateDBInstance} is called with the driver's name
   * and refused as "Invalid DB engine", which names nothing that appears in this repository.
   */
  private static String rdsEngineName(final OrchestraClient orchestra) {
    final String engineName = orchestra.getContext(Database.class).getString("engineName");
    if (engineName == null || engineName.trim().isEmpty()) {
      throw new RuntimeException(
          "The database in this composition published no engineName, so any RDS call naming an engine - "
              + "adding an instance to the cluster, in particular - would be refused as \"Invalid DB "
              + "engine\".");
    }
    return engineName;
  }

  /**
   * Returns the engine version RDS reports, or {@code null} if the deployment published none.
   *
   * <p>Optional where the name is not, because the two are used differently: the version is passed to
   * {@code CreateDBInstance}, which infers the cluster's version when it is absent, and is otherwise read for
   * reporting by the database metrics suite. A null is therefore a slightly less informative report rather
   * than a failed call.
   */
  private static String rdsEngineVersion(final OrchestraClient orchestra) {
    final String version = orchestra.getContext(Database.class).getString("engineVersion");
    return version == null || version.trim().isEmpty() ? null : version;
  }

  /**
   * Returns the cluster identifier as AWS knows it.
   *
   * <p>Read from the raw context rather than from {@link DatabaseClusterState}, because the typed cluster
   * view carries what a test needs in order to <em>connect</em> and an AWS identifier is not that. The
   * instrument publishes it under {@code clusterIdentifier} for exactly this kind of caller.
   *
   * <p>Not optional. It is the name every RDS control-plane call in {@code AuroraTestUtility} passes -
   * failover, writer checks, the cluster health check {@code TestDriverProvider} runs before each test - so
   * a null here surfaces later as "Cluster null is not healthy", which is what it did before this was read.
   */
  private static String clusterIdentifier(final OrchestraClient orchestra) {
    final String identifier = orchestra.getContext(Database.class).getString("clusterIdentifier");
    if (identifier == null || identifier.trim().isEmpty()) {
      throw new RuntimeException(
          "The database in this composition published no clusterIdentifier, so no RDS control-plane call "
              + "can name it - failover, writer checks and the cluster health check all need it.");
    }
    return identifier;
  }

  /**
   * Reads the multi-region topology of a global database.
   *
   * <p>Through the typed view rather than raw fields, unlike the other reads here, because this one is a nested
   * structure: a list of regions each with endpoints, a host suffix and its own instance list. Parsing that from
   * the field map at every call site is exactly the kind of thing a published contract exists to avoid.
   *
   * <p>The cluster the suite connects to is <em>not</em> read from here. That is whatever the composition bound
   * to the database role - normally a secondary region for these tests - and it arrives through the same path as
   * any other environment, which is what keeps the ordinary helpers working on a global database.
   *
   * @throws RuntimeException if the composition declared the feature but published no global database, since
   *     every GDB test would then fail on a null topology rather than on anything it was testing
   */
  private static TestGlobalDatabaseInfo globalDatabaseInfo(final OrchestraClient orchestra) {
    final GlobalDatabaseState published =
        orchestra.getContext(GlobalDatabase.class, GlobalDatabaseState.class);

    if (published.regions() == null || published.regions().size() < 2) {
      throw new RuntimeException(
          "The composition declares GLOBAL_DATABASE but published " + published
              + ", which spans fewer than two regions. Every cross-region test would have nowhere to go.");
    }

    final TestGlobalDatabaseInfo info = new TestGlobalDatabaseInfo();
    info.setGlobalClusterIdentifier(published.globalClusterIdentifier());
    info.setPrimaryRegion(published.primaryRegion());
    // Nullable, and not validated here. An older engine version publishes no global endpoint, and the tests that
    // need one skip on its absence - failing the whole environment read would take out every other GDB test too.
    info.setGlobalEndpoint(published.globalEndpoint());

    final List<TestRegionalClusterInfo> regions = new ArrayList<>();
    for (final GlobalDatabaseState.RegionalCluster cluster : published.regions()) {
      final TestRegionalClusterInfo regional = new TestRegionalClusterInfo();
      regional.setRegion(cluster.region());
      regional.setClusterIdentifier(cluster.clusterIdentifier());
      regional.setClusterEndpoint(cluster.writerHost());
      regional.setClusterReadOnlyEndpoint(cluster.readerHost());
      regional.setPort(cluster.port());
      regional.setInstanceEndpointSuffix(cluster.instanceHostSuffix());
      regional.setInstanceIdentifiers(new ArrayList<>(cluster.instanceIdentifiers()));
      regions.add(regional);
    }
    info.setRegions(regions);
    return info;
  }

  /**
   * Returns the region the environment lives in.
   *
   * <p>From the context first, then the container's environment. IAM token generation needs a real region and
   * a null one fails much later inside the signer, so the fallback exists to make the failure be about the
   * region rather than about a signature.
   */
  private static String region(final InstrumentContext shape) {
    final String fromContext = shape.getString("region");
    if (fromContext != null && !fromContext.trim().isEmpty()) {
      return fromContext;
    }

    final String fromEnvironment = System.getenv("AWS_REGION");
    if (fromEnvironment != null && !fromEnvironment.trim().isEmpty()) {
      return fromEnvironment;
    }
    throw new RuntimeException(
        "No AWS region in the composition context or in AWS_REGION, so anything needing a signed AWS "
            + "request - IAM authentication in particular - cannot work.");
  }

  /** Rebuilds the request the harness's conditions and ~150 call sites read. */
  private static TestEnvironmentRequest request(
      final InstrumentContext shape, final DatabaseClusterState cluster) {

    final int instanceCount = shape.getInt("instanceCount");

    return new TestEnvironmentRequest(
        DatabaseEngine.valueOf(shape.getString("engine")),
        instanceCount > 1 ? DatabaseInstances.MULTI_INSTANCE : DatabaseInstances.SINGLE_INSTANCE,
        instanceCount,
        DatabaseEngineDeployment.valueOf(shape.getString("deployment")),
        TargetJvm.valueOf(shape.getString("targetJvm")),
        features(shape).toArray(new TestEnvironmentFeatures[0]));
  }

  /**
   * Reads the feature set, ignoring names this build does not know.
   *
   * <p>Tolerant on purpose. The context is written by a host that compiles separately from this classpath, so
   * a host that has learned a new feature must not break a container that has not — the alternative is that
   * adding a feature name breaks every older in-container build at once.
   */
  private static EnumSet<TestEnvironmentFeatures> features(final InstrumentContext shape) {
    final EnumSet<TestEnvironmentFeatures> features = EnumSet.noneOf(TestEnvironmentFeatures.class);

    // Straight from the field map: the context exposes typed getters for scalars only, and a list is read
    // the same way DatabaseClusterState reads its instances.
    final Object rendered = shape.fields().get("features");
    if (!(rendered instanceof List)) {
      return features;
    }

    for (final Object name : (List<?>) rendered) {
      if (name == null) {
        continue;
      }
      try {
        features.add(TestEnvironmentFeatures.valueOf(name.toString()));
      } catch (final IllegalArgumentException unknownToThisBuild) {
        // Skipped deliberately; see above.
      }
    }
    return features;
  }

  /** Maps the cluster's endpoints, credentials and topology onto the harness's shape. */
  private static TestDatabaseInfo databaseInfo(final DatabaseClusterState cluster) {
    final TestDatabaseInfo database = new TestDatabaseInfo();

    database.setUsername(cluster.username());
    database.setPassword(cluster.password());
    database.setDefaultDbName(cluster.databaseName());
    database.setClusterEndpoint(cluster.writerHost(), cluster.port());
    database.setClusterReadOnlyEndpoint(cluster.readerHost(), cluster.port());
    database.setInstanceEndpointSuffix(instanceEndpointSuffix(cluster), cluster.port());

    for (final DatabaseClusterState.Instance instance : writerFirst(cluster)) {
      database.getInstances().add(
          new TestInstanceInfo(instance.identifier(), instance.host(), instance.port()));
    }

    return database;
  }

  /**
   * Derives the instance endpoint suffix the harness uses to build instance hostnames.
   *
   * <p>Taken from an instance's own endpoint rather than from the cluster endpoint, because the two differ:
   * a cluster endpoint contains {@code .cluster-} and an instance endpoint does not, so stripping the
   * instance id from an instance endpoint is the only way to get a suffix that produces valid instance
   * hostnames.
   *
   * @return the suffix, or {@code null} when the cluster reported no instance endpoints
   */
  private static String instanceEndpointSuffix(final DatabaseClusterState cluster) {
    for (final DatabaseClusterState.Instance instance : cluster.instances()) {
      final String host = instance.host();
      if (host == null || host.isEmpty()) {
        continue;
      }
      final int dot = host.indexOf('.');
      if (dot > 0 && dot + 1 < host.length()) {
        return host.substring(dot + 1);
      }
    }
    return null;
  }
}
