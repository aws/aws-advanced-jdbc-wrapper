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
import integration.TestEnvironmentFeatures;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import software.amazon.orchestra.Composition;
import software.amazon.orchestra.InstrumentDefinition;
import software.amazon.orchestra.Variation;
import software.amazon.orchestra.contract.ClusterParameterGroup;
import software.amazon.orchestra.contract.Database;
import software.amazon.orchestra.contract.DbParameterGroup;
import software.amazon.orchestra.instruments.aws.AuroraClusterInstrumentDefinition;
import software.amazon.orchestra.instruments.aws.ClusterParameterGroupInstrumentDefinition;
import software.amazon.orchestra.instruments.aws.DbParameterGroupInstrumentDefinition;
import software.amazon.orchestra.instruments.aws.RdsEngine;
import software.amazon.orchestra.instruments.aws.RdsMultiAzClusterInstrumentDefinition;
import software.amazon.orchestra.instruments.aws.RdsMultiAzInstanceInstrumentDefinition;

/**
 * The engine axis: one composition per database engine.
 *
 * <p>Replaces the outermost of {@code TestEnvironmentProvider}'s loops, and the reason it is a
 * consumer-defined variation rather than Orchestra's {@code InstrumentChoiceVariation} is that two
 * instruments have to move together. Choosing the engine means binding a different cluster
 * <em>and</em> a different {@link TestShape}, because the in-container tests read the engine from the shape
 * in order to pick a driver and to evaluate {@code @EnableOnDatabaseEngine}.
 *
 * <p>Two separate choice variations could not do it. Variations chain, each seeing the previous one's
 * output, so an engine axis over clusters and another over shapes would produce the cross-product -
 * including the two nonsense slots pairing a MySQL cluster with a PostgreSQL shape. Tests in those slots
 * would load the wrong driver and fail in a way that looks like a driver bug. Binding both roles in one
 * slot makes the mismatch unrepresentable instead of merely wrong.
 *
 * <p>{@code RdsEngine} and {@code DatabaseEngine} are separate enums on purpose and are paired here: the
 * first is Orchestra's idea of an engine, carrying the three names AWS uses, and the second is the
 * wrapper's, naming a driver. This class is where those two vocabularies meet, and keeping the pairing in
 * one table is what stops them drifting.
 */
public class EngineVariation implements Variation {

  /** What one slot of this axis binds. */
  private static final class Choice {
    private final RdsEngine cluster;
    private final DatabaseEngine driver;

    Choice(final RdsEngine cluster, final DatabaseEngine driver) {
      this.cluster = cluster;
      this.driver = driver;
    }
  }

  private final Map<String, Choice> choices = new LinkedHashMap<>();
  private final DatabaseEngineDeployment deployment;

  private final String iamUsername;
  private final Set<TestEnvironmentFeatures> features;

  /**
   * Creates the axis over the given engines, holding everything else about the shape constant.
   *
   * <p>The non-engine shape parameters are taken here because this variation rebuilds the shape, so it
   * needs everything the shape needs. They are the parts of the environment description the engine does not
   * change.
   *
   * @param deployment the deployment kind, the same for every engine on this axis
   * @param iamUsername the database user the IAM tests authenticate as, or {@code null} without IAM
   * @param features the features every slot on this axis provides
   * @param engines the engines to run, in order; at least one
   */
  public EngineVariation(
      final DatabaseEngineDeployment deployment,
      final String iamUsername,
      final Set<TestEnvironmentFeatures> features,
      final RdsEngine... engines) {

    if (engines == null || engines.length == 0) {
      throw new IllegalArgumentException(
          "An EngineVariation needs at least one engine, otherwise it would produce no compositions.");
    }

    this.deployment = deployment;

    this.iamUsername = iamUsername;
    this.features = features;

    for (final RdsEngine engine : engines) {
      this.choices.put(label(engine), new Choice(engine, wrapperEngineFor(engine)));
    }
  }

  @Override
  public List<Composition> process(final List<Composition> compositions) {
    final List<Composition> expanded = new ArrayList<>(compositions.size() * this.choices.size());

    for (final Composition composition : compositions) {
      for (final Map.Entry<String, Choice> chosen : this.choices.entrySet()) {
        final Choice choice = chosen.getValue();

        expanded.add(composition
            // All three roles, in the same slot. A fresh definition instance per slot, because definitions
            // hold per-composition state and two slots must not share it.
            .withInstrument(Database.class, databaseFor(this.deployment, choice.cluster))
            .withInstrument(
                parameterGroupRole(this.deployment),
                parameterGroupFor(this.deployment, choice.cluster))
            .withInstrument(TestShape.class, new TestShapeInstrumentDefinition(
                choice.driver, this.deployment, this.iamUsername, this.features))
            .withDisplayName(append(composition.getDisplayName(), chosen.getKey())));
      }
    }
    return expanded;
  }

  /**
   * Returns the database instrument for a deployment.
   *
   * <p>Closes a hazard this class previously had: it took the deployment as a constructor argument, passed it
   * to the shape, and then bound an Aurora cluster regardless. Constructing it with
   * {@code RDS_MULTI_AZ_CLUSTER} would therefore have produced slots whose shape said Multi-AZ while the
   * database was Aurora - the same shape-versus-reality disagreement the instance-count change removed, and
   * the one this class's own documentation claims to make unrepresentable.
   *
   * <p>An unsupported deployment fails here rather than silently binding the wrong thing, because that is the
   * failure mode being removed.
   *
   * <p>Static, and called by {@link OrchestraTestRunner} for its default binding as well as by this axis for
   * each slot. Those two produced the same mapping written twice, which is one edit away from a run whose
   * unvaried composition provisions a different deployment from its varied ones.
   *
   * @param deployment the deployment to provision
   * @param engine the engine family
   * @return the instrument that provisions it
   */
  static InstrumentDefinition databaseFor(
      final DatabaseEngineDeployment deployment, final RdsEngine engine) {

    switch (deployment) {
      case AURORA:
        return new AuroraClusterInstrumentDefinition(engine);
      case RDS_MULTI_AZ_CLUSTER:
        return new RdsMultiAzClusterInstrumentDefinition(engine);
      case RDS_MULTI_AZ_INSTANCE:
        return new RdsMultiAzInstanceInstrumentDefinition(engine);
      default:
        throw new IllegalArgumentException(
            "EngineVariation has no database instrument for " + deployment + ". Add the mapping rather "
                + "than letting a slot's shape and database disagree about the deployment.");
    }
  }

  /**
   * Returns the parameter group role a deployment's database instrument reads.
   *
   * <p>Two kinds of resource for one purpose. RDS accepts only the cluster form in {@code CreateDBCluster}
   * and only the DB form in {@code CreateDBInstance}, so a standalone Multi-AZ instance cannot use the
   * cluster group even though the parameters inside it are identical. The role differs with it, because the
   * role is how the database instrument orders itself after the group and then finds it.
   *
   * @param deployment the deployment being provisioned
   * @return the role to bind the group to
   */
  static Class<?> parameterGroupRole(final DatabaseEngineDeployment deployment) {
    return DatabaseEngineDeployment.RDS_MULTI_AZ_INSTANCE.equals(deployment)
        ? DbParameterGroup.class
        : ClusterParameterGroup.class;
  }

  /**
   * Returns the parameter group instrument for one slot of this axis.
   *
   * <p>Bound in every slot rather than once for the composition, and unconditionally rather than only when
   * something needs it. Both of those were previously otherwise, and each was wrong in its own way.
   *
   * <p>Per slot, because a parameter group belongs to an engine's family - {@code aurora-mysql8.0} is not
   * {@code aurora-postgresql16} - so one group shared across an engine matrix would be attached to a cluster
   * that rejects it. The runner used to reject such a matrix outright to avoid that, which meant an engine
   * axis and a MySQL engine were mutually exclusive.
   *
   * <p>Unconditionally, because the previous condition - blue/green, or MySQL - silently cost coverage. The
   * retired harness created a group for every engine and deployment it provisioned, and the PostgreSQL one
   * carried {@code max_prepared_transactions}. Without it PostgreSQL leaves two-phase commit disabled, and
   * the XA tests do not fail: {@code XaTestUtility.assumePreparedTransactionsSupported} skips them, so a run
   * that tested less reported exactly the same as one that tested more.
   *
   * @param deployment the deployment being provisioned
   * @param engine the engine of this slot
   * @return the instrument that provisions the group
   */
  static InstrumentDefinition parameterGroupFor(
      final DatabaseEngineDeployment deployment, final RdsEngine engine) {

    switch (deployment) {
      case AURORA:
        return new ClusterParameterGroupInstrumentDefinition(engine, suiteParameters(engine));
      case RDS_MULTI_AZ_CLUSTER:
        // A cluster group, like Aurora's, but in the plain engine's family: this deployment creates its
        // cluster with engine "postgres", and a group in the aurora-postgresql family is refused there.
        return ClusterParameterGroupInstrumentDefinition.forRdsMultiAzCluster(
            engine, suiteParameters(engine));
      case RDS_MULTI_AZ_INSTANCE:
        return new DbParameterGroupInstrumentDefinition(engine, suiteParameters(engine));
      default:
        throw new IllegalArgumentException(
            "EngineVariation has no parameter group for " + deployment + ". Add the mapping rather than "
                + "attaching a group whose family the deployment's create call will reject.");
    }
  }

  /**
   * Returns the parameters this suite needs beyond the ones Orchestra sets for the engine.
   *
   * <p>Orchestra sets what a deployment needs - the replication switches a blue/green source cannot do
   * without, and {@code require_secure_transport=OFF} so the MariaDB driver can connect at all. This is the
   * other kind: a setting that is a property of what the suite tests rather than of the environment.
   *
   * <p>{@code max_prepared_transactions} is the whole list. PostgreSQL defaults it to zero, which makes
   * {@code XAResource.prepare} fail with "prepared transactions are disabled"; the value matches the
   * harness's. MySQL/InnoDB supports XA out of the box and needs nothing here.
   *
   * @param engine the engine of the slot
   * @return the parameters by name, empty when the engine needs none
   */
  static Map<String, String> suiteParameters(final RdsEngine engine) {
    return RdsEngine.POSTGRES.equals(engine)
        ? Collections.singletonMap("max_prepared_transactions", "100")
        : Collections.emptyMap();
  }

  /**
   * Maps Orchestra's engine to the wrapper's name for the same engine.
   *
   * <p>Explicit rather than derived from the enum name. The two vocabularies genuinely differ - Aurora's
   * {@code aurora-postgresql} is the wrapper's {@code PG} - and a name-based guess would work until the day
   * it silently picked the wrong one.
   *
   * <p>Named for the engine rather than the driver, which is what it always returned: {@code DatabaseEngine}
   * is the wrapper's <em>engine</em> dimension, and the driver is a separate enum
   * ({@code integration.container.TestDriver}) that the in-container framework varies per test. Calling this
   * {@code driverFor} invited exactly the confusion that matters here - the MariaDB driver runs against a
   * MySQL engine, so the two dimensions are not in step.
   *
   * <p>Package-private so {@link OrchestraTestRunner} can check a requested driver against the engines on
   * this axis before provisioning anything.
   *
   * @param engine Orchestra's engine
   * @return the wrapper's engine
   */
  static DatabaseEngine wrapperEngineFor(final RdsEngine engine) {
    switch (engine) {
      case POSTGRES:
        return DatabaseEngine.PG;
      case MYSQL:
        return DatabaseEngine.MYSQL;
      default:
        throw new IllegalArgumentException(
            "No wrapper DatabaseEngine is mapped to " + engine + ". Add the pairing to EngineVariation "
                + "rather than letting the shape and the cluster disagree about the engine.");
    }
  }

  private static String label(final RdsEngine engine) {
    return engine.name().toLowerCase(Locale.ROOT);
  }

  private static String append(final String current, final String label) {
    return current == null || current.isEmpty() || "default".equals(current)
        ? label
        : current + "-" + label;
  }
}
