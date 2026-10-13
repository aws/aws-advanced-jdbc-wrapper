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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import software.amazon.orchestra.Composition;
import software.amazon.orchestra.EnvConfiguration;
import software.amazon.orchestra.Instrument;
import software.amazon.orchestra.InstrumentDefinition;
import software.amazon.orchestra.RenderableState;
import software.amazon.orchestra.SimpleInstrument;
import software.amazon.orchestra.WorkloadContextTarget;
import software.amazon.orchestra.instruments.aws.AuroraClusterConfiguration;
import software.amazon.orchestra.instruments.aws.AwsConfiguration;
import software.amazon.orchestra.instruments.docker.JavaTestContainerConfiguration;

/**
 * Publishes what kind of environment this run is, for in-container code that asks.
 *
 * <p>The first consumer-defined instrument in this repository, and a useful demonstration that the extension
 * point works: it provisions nothing, holds no resource, and exists purely to put information into the
 * context. Orchestra's {@code InstrumentDefinition} accommodates that without special-casing, because a
 * "resource" is whatever the definition says it is.
 *
 * <p>What it carries is the harness's {@code TestEnvironmentRequest}: engine, deployment, instance count,
 * target JVM, and the {@code TestEnvironmentFeatures} set. See {@link TestShape} for why that is needed and
 * why it is temporary.
 *
 * <p>Deliberately not derived from the composition. The engine and instance count could be read from the
 * database's own published state, and doing so would look tidier — but the deployment kind and the feature
 * set cannot, and splitting the request across two sources would make it unclear which one a test is really
 * answering to. One instrument, one answer.
 */
public class TestShapeInstrumentDefinition implements InstrumentDefinition {

  /**
   * How many instances an RDS Multi-AZ cluster has.
   *
   * <p>Three, always. RDS offers no choice, which is why the harness warns and corrects any other request
   * ("3 instances will be used as a default") rather than honouring it.
   */
  private static final int MULTI_AZ_CLUSTER_INSTANCES = 3;

  /**
   * How many instances an RDS Multi-AZ instance deployment has.
   *
   * <p>One. {@code multiAZ=true} does provision a standby, but it has no endpoint and does not appear as an
   * instance, so one is what a client can address and one is what the topology contains. The harness says
   * the same thing by skipping every slot where this deployment is paired with any other count.
   */
  private static final int MULTI_AZ_INSTANCE_INSTANCES = 1;

  private final DatabaseEngine engine;
  private final DatabaseEngineDeployment deployment;
  private final String iamUsername;
  private final Set<TestEnvironmentFeatures> features;

  /**
   * Creates a definition describing one environment shape.
   *
   * <p>Note what is absent: the instance count and the target JVM. Both used to be passed here and are now
   * read from configuration, because they are the values on this list that another instrument also acts on. A
   * variation overriding {@code getAuroraInstanceCount} would change how many instances the cluster gets
   * while this shape went on reporting the number it was constructed with - and the in-container tests
   * believe the shape, so {@code @EnableOnNumOfInstances} would gate on one number while the cluster had
   * another. Reading both from one place makes that impossible rather than merely unlikely.
   *
   * <p>The target JVM is the same story with a longer fuse. It is decided by the container image, so a JVM
   * axis overrides that image; a shape holding its own copy would go on publishing the JVM it was constructed
   * with, and every {@code @EnableOnTargetJvm} condition would gate on a JVM the suite is not running on.
   * That does not fail - it runs or skips the wrong tests and reports success.
   *
   * @param engine the database engine under test
   * @param deployment the deployment kind
   * @param iamUsername the database user the IAM tests authenticate as, or {@code null} without IAM
   * @param features the features this environment supports
   */
  public TestShapeInstrumentDefinition(
      final DatabaseEngine engine,
      final DatabaseEngineDeployment deployment,
      final String iamUsername,
      final Set<TestEnvironmentFeatures> features) {

    this.engine = engine;
    this.deployment = deployment;
    this.iamUsername = iamUsername;
    this.features = features;
  }

  @Override
  public List<Class<?>> getRequiredConfigurationInterfaces() {
    // AwsConfiguration for the region, a property of where the run provisions rather than of what kind of
    // run it is, and without which IAM authentication in the container cannot sign a request.
    // AuroraClusterConfiguration for the instance count, so the shape and the cluster cannot disagree.
    // JavaTestContainerConfiguration for the image, so the shape and the container cannot disagree about
    // which JVM the suite is on.
    return java.util.Arrays.asList(
        AwsConfiguration.class, AuroraClusterConfiguration.class, JavaTestContainerConfiguration.class);
  }

  @Override
  public Instrument build(final EnvConfiguration configuration, final Composition composition) {
    return new SimpleInstrument(this, new TestShapeState(
        this.engine.name(),
        this.deployment.name(),
        instanceCount(configuration),
        targetJvm(configuration).name(),
        ((AwsConfiguration) configuration).getAwsRegion(),
        this.iamUsername,
        featureNames()));
  }

  /**
   * Returns the JVM the suite will run on, derived from the image the container is built from.
   *
   * <p>Derived rather than declared, so the two cannot drift: the image is what actually decides the JVM, and
   * {@link TargetJvmImages} is the single table both this and {@link JvmVariation} read.
   */
  private static TargetJvm targetJvm(final EnvConfiguration configuration) {
    return TargetJvmImages.jvmFor(
        ((JavaTestContainerConfiguration) configuration).getTestContainerImage());
  }

  /**
   * Returns how many instances the database has, which depends on what kind of database it is.
   *
   * <p>An RDS Multi-AZ cluster is always three instances: RDS fixes it, there is no knob, and
   * {@code RdsMultiAzClusterConfiguration} accordingly has no instance count for a variation to override.
   * Reporting three is therefore not a hardcoded guess but the only value it can have - the shape and the
   * cluster cannot disagree because AWS will not let them.
   *
   * <p>Aurora does have a knob, so the count is read from configuration for the reason it was moved there:
   * both the cluster instrument and this shape ask the same method, so an instance-count variation moves
   * them together.
   *
   * <p>A single Docker container reports whatever its configuration says, which
   * {@code OrchestraDockerConfig} overrides to one.
   */
  private int instanceCount(final EnvConfiguration configuration) {
    if (DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER.equals(this.deployment)) {
      return MULTI_AZ_CLUSTER_INSTANCES;
    }
    if (DatabaseEngineDeployment.RDS_MULTI_AZ_INSTANCE.equals(this.deployment)) {
      // Same reasoning as the cluster's three: fixed by the deployment, so an instance-count variation
      // cannot make the shape and the database disagree. It also keeps the request this publishes
      // SINGLE_INSTANCE, which is what @EnableOnNumOfInstances and the harness's own matrix expect.
      return MULTI_AZ_INSTANCE_INSTANCES;
    }
    return ((AuroraClusterConfiguration) configuration).getAuroraInstanceCount();
  }

  @Override
  public void destroy(final EnvConfiguration configuration, final Composition composition) {
    // Nothing to tear down. There was never a resource.
  }

  private List<String> featureNames() {
    final List<String> names = new ArrayList<>(this.features.size());
    for (final TestEnvironmentFeatures feature : this.features) {
      names.add(feature.name());
    }
    return names;
  }

  /**
   * What the shape publishes to workloads.
   *
   * <p>Enum <em>names</em> rather than ordinals, because the context is a wire format read by a separately
   * compiled classpath: an ordinal would silently change meaning the day someone reorders an enum, and the
   * failure would be a test running against the wrong deployment rather than an error.
   *
   * @param engine the engine name, matching {@code DatabaseEngine}
   * @param deployment the deployment name, matching {@code DatabaseEngineDeployment}
   * @param instanceCount how many instances the database has
   * @param targetJvm the JVM name, matching {@code TargetJvm}
   * @param region the AWS region the environment was provisioned in
   * @param iamUsername the database user the IAM tests authenticate as, or {@code null} without IAM
   * @param features the feature names, matching {@code TestEnvironmentFeatures}
   */
  public record TestShapeState(
      String engine,
      String deployment,
      int instanceCount,
      String targetJvm,
      String region,
      String iamUsername,
      List<String> features) implements RenderableState {

    /** The name the in-container facade knows this state by. */
    public static final String STATE_TYPE = "integration.orchestra.TestShapeState";

    @Override
    public String stateTypeName() {
      return STATE_TYPE;
    }

    @Override
    public Map<String, Object> render(final Class<?> target) {
      if (!WorkloadContextTarget.class.equals(target)) {
        return null;
      }

      final Map<String, Object> fields = new LinkedHashMap<>();
      fields.put("engine", this.engine);
      fields.put("deployment", this.deployment);
      fields.put("instanceCount", this.instanceCount);
      fields.put("targetJvm", this.targetJvm);
      fields.put("region", this.region);
      fields.put("iamUsername", this.iamUsername);
      fields.put("features", this.features);
      return fields;
    }
  }
}
