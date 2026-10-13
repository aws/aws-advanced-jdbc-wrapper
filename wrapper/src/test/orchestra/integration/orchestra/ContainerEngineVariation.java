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
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import software.amazon.orchestra.Composition;
import software.amazon.orchestra.InstrumentDefinition;
import software.amazon.orchestra.Variation;
import software.amazon.orchestra.contract.Database;
import software.amazon.orchestra.instruments.docker.MySqlContainerInstrumentDefinition;
import software.amazon.orchestra.instruments.docker.PostgresContainerInstrumentDefinition;

/**
 * Runs the Docker suite against each engine asked for, from {@code -Dorchestra-engines}.
 *
 * <p>The container counterpart of {@link EngineVariation}, and it exists for the same reason: the harness's
 * {@code test-all-docker} is a matrix over the database <em>server</em>, and the PR gate is that matrix. A
 * migrated task that could only start PostgreSQL would narrow what every pull request is checked against, while
 * looking like a like-for-like replacement.
 *
 * <p>Two engines, not three. The harness's Docker matrix also runs a MariaDB server, and that is deliberately
 * not reproduced: MariaDB as an engine is out of scope for this migration. The MariaDB <em>driver</em> is not -
 * it is an axis of its own ({@code -Dorchestra-drivers}) and it runs against a MySQL server, which is how CI
 * pins it.
 *
 * <p>Each slot rebinds two things together, because they have to agree: the {@code Database} role, so the right
 * container starts, and {@code TestShape}, so the in-container conditions see the engine that is actually
 * running. Rebinding one without the other is the failure this class is shaped to prevent - a PostgreSQL
 * container that the suite believes is MySQL fails as a connection error, not as a configuration mistake.
 */
public class ContainerEngineVariation implements Variation {

  private final DatabaseEngine[] engines;
  private final EnumSet<TestEnvironmentFeatures> features;

  /**
   * Creates the variation.
   *
   * @param engines the engines to run, at least one
   * @param features the feature set every slot publishes, which the engine does not change
   */
  public ContainerEngineVariation(
      final DatabaseEngine[] engines, final EnumSet<TestEnvironmentFeatures> features) {

    if (engines == null || engines.length == 0) {
      throw new IllegalArgumentException(
          "At least one engine is required. A variation with no values produces no compositions, which "
              + "reports as a run that passed without testing anything.");
    }
    this.engines = engines.clone();
    this.features = features;
  }

  /**
   * Returns the engines requested, defaulting to PostgreSQL alone.
   *
   * <p>Shared with the runner rather than parsed twice, like {@code JvmVariation.requested()}.
   *
   * <p>Accepts {@code pg} and {@code postgres} for the same engine. The Aurora task names engines the way RDS
   * does ({@code postgres}) while the in-container suite names them the way the driver does ({@code pg}), and a
   * developer moving between the two tasks should not have to remember which spelling belongs to which.
   *
   * @return at least one engine
   * @throws IllegalArgumentException if a name is not an engine this task can run
   */
  public static DatabaseEngine[] requested() {
    final String requested = System.getProperty("orchestra-engines");
    if (requested == null || requested.trim().isEmpty()) {
      return new DatabaseEngine[] {DatabaseEngine.PG};
    }

    final List<DatabaseEngine> engines = new ArrayList<>();
    for (final String name : requested.split(",")) {
      final String token = name.trim().toLowerCase(Locale.ROOT);
      if (token.isEmpty()) {
        continue;
      }

      switch (token) {
        case "pg":
        case "postgres":
        case "postgresql":
          engines.add(DatabaseEngine.PG);
          break;
        case "mysql":
          engines.add(DatabaseEngine.MYSQL);
          break;
        case "mariadb":
          throw new IllegalArgumentException(
              "orchestra-engines=mariadb is out of scope for this task: a MariaDB server is not run. The "
                  + "MariaDB driver is a separate axis - use -Dorchestra-drivers=mariadb against a MySQL "
                  + "engine, which is what CI does.");
        default:
          throw new IllegalArgumentException(
              "orchestra-engines names '" + token + "', which is not an engine this task can run. "
                  + "Supported: pg, mysql.");
      }
    }

    if (engines.isEmpty()) {
      return new DatabaseEngine[] {DatabaseEngine.PG};
    }
    return engines.toArray(new DatabaseEngine[0]);
  }

  /**
   * Returns the container instrument that provides an engine.
   *
   * <p>Public and static so the runner can bind a default with it, which keeps the default and the varied slots
   * from disagreeing about what an engine means - the same reason {@code EngineVariation.databaseFor} exists.
   *
   * @param engine the engine to provide
   * @return the instrument to bind to the {@code Database} role
   */
  public static InstrumentDefinition databaseFor(final DatabaseEngine engine) {
    switch (engine) {
      case PG:
        return new PostgresContainerInstrumentDefinition();
      case MYSQL:
        return new MySqlContainerInstrumentDefinition();
      default:
        throw new IllegalArgumentException(
            "No container instrument is wired for engine " + engine + ". Add the pairing rather than "
                + "letting a composition provision a database the suite was not asked for.");
    }
  }

  @Override
  public List<Composition> process(final List<Composition> compositions) {
    final List<Composition> expanded = new ArrayList<>(compositions.size() * this.engines.length);

    for (final Composition composition : compositions) {
      for (final DatabaseEngine engine : this.engines) {
        expanded.add(composition
            // Both roles, in the same slot, and a fresh definition instance per slot because definitions hold
            // per-composition state.
            .withInstrument(Database.class, databaseFor(engine))
            // Rebuilt rather than adjusted, because the shape is what the in-container conditions read: a slot
            // whose database is MySQL and whose shape says PG skips every MySQL-gated test and runs the
            // PostgreSQL-gated ones against a server that cannot answer them.
            .withInstrument(TestShape.class, new TestShapeInstrumentDefinition(
                engine,
                DatabaseEngineDeployment.DOCKER,
                // No IAM user: a container has no IAM. Matches the runner's default shape.
                null,
                this.features))
            .withDisplayName(
                append(composition.getDisplayName(), engine.name().toLowerCase(Locale.ROOT))));
      }
    }
    return expanded;
  }

  /** Same convention as {@link EngineVariation}, so slot names read alike across the two axes. */
  private static String append(final String current, final String label) {
    return current == null || current.isEmpty() || "default".equals(current)
        ? label
        : current + "-" + label;
  }
}
