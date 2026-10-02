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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.orchestra.Composition;
import software.amazon.orchestra.EnvConfiguration;
import software.amazon.orchestra.Instrument;
import software.amazon.orchestra.InstrumentDefinition;
import software.amazon.orchestra.SimpleInstrument;
import software.amazon.orchestra.contract.Database;
import software.amazon.orchestra.instruments.aws.DatabaseClusterState;

/**
 * Creates the {@code rds_tools} extension on an RDS Multi-AZ PostgreSQL database, and lets the IAM user use it.
 *
 * <p>Multi-AZ has no {@code aurora_replica_status()}. Topology comes from {@code rds_tools.show_topology()},
 * and that function exists only once the extension has been created; nothing creates it automatically. Without
 * it the topology query fails, {@code getAuroraInstanceIds} returns an empty list, and
 * {@code TestDriverProvider.checkClusterHealth} spends ten minutes waiting for a writer to appear before
 * failing on {@code expected: <false> but was: <true>} - a message that says nothing about a missing extension.
 * Every class annotated {@code @MakeSureFirstInstanceWriter} fails that way, ten minutes apiece.
 *
 * <h2>Why this is an instrument and not a step in the container</h2>
 *
 * <p>It was a step in the container, run from {@code integration.container.TestEnvironment.create()}, on the
 * reasoning that needing an extension is a property of this suite rather than of the environment. That
 * reasoning holds for <em>who decides</em> to create it - which is why this class lives in this repository and
 * not in Orchestra - but not for <em>when</em>, and the placement was wrong for three reasons.
 *
 * <p>A container is handed an environment that is ready to use. Provisioning from inside one inverts that: the
 * workload becomes responsible for a step every other workload sharing the database depends on.
 *
 * <p>Nothing says which container does it. A composition may hand one database to several containers, and each
 * of them ran this on startup - so the work was repeated as many times as there were containers, and which one
 * got there first was a race. It was harmless only because {@code CREATE EXTENSION IF NOT EXISTS} is
 * idempotent, which is luck rather than design.
 *
 * <p>The grants could not be placed correctly at all. They belong with {@code IamDatabaseUserInstrumentDefinition},
 * which creates the user, but that instrument runs while the environment is being built - before the container
 * existed to create the schema it would grant on. Provisioning-time ordering is the mechanism that fits: this
 * runs after the database and after the IAM user, so the schema exists before the grants and the user exists
 * before it is granted anything.
 *
 * <h2>Nothing to tear down</h2>
 *
 * <p>The extension and the grants live inside the database, so deleting it removes them. See
 * {@link IamDatabaseUserInstrumentDefinition} for the same argument about the user itself.
 */
public class RdsToolsExtensionInstrumentDefinition implements InstrumentDefinition {

  private static final Logger LOGGER =
      Logger.getLogger(RdsToolsExtensionInstrumentDefinition.class.getName());

  /** The JDBC name of the only engine that has this extension. */
  private static final String POSTGRESQL = "postgresql";

  private final String iamUsername;

  /**
   * Creates a definition for the extension, and optionally for granting a user access to it.
   *
   * @param iamUsername the IAM database user to grant usage to, or {@code null} when the composition has no
   *     IAM user to grant anything to
   */
  public RdsToolsExtensionInstrumentDefinition(final String iamUsername) {
    this.iamUsername = iamUsername;
  }

  @Override
  public List<Class<?>> getProvisionDependencies() {
    // Both edges are declared, and both are required rather than optional.
    //
    // The database, because this connects to it with the master credentials it publishes. The IAM user,
    // because the grants below name it and a grant to a user that does not exist yet fails.
    //
    // Required, because an absent one is a mistake worth hearing about: Orchestra rejects the composition by
    // name before it provisions anything, where an optional dependency that matches nothing is skipped
    // silently and leaves the order to the sequence of the builder's calls.
    return Arrays.asList(Database.class, IamDatabaseUserInstrumentDefinition.class);
  }

  @Override
  public Instrument build(final EnvConfiguration configuration, final Composition composition)
      throws SQLException {

    final DatabaseClusterState database =
        composition.getInstrumentState(Database.class, DatabaseClusterState.class);

    // The engine comes from what was provisioned rather than from a constructor argument, so this instrument
    // is added once and still does the right thing in an engine matrix, where EngineVariation rebinds the
    // database per slot. MySQL has no rds_tools.
    if (!POSTGRESQL.equals(database.engine())) {
      LOGGER.fine(() -> "Skipping rds_tools: the database in '" + composition.getDisplayName()
          + "' is " + database.engine() + ", which has no such extension.");
      return new SimpleInstrument(this, null);
    }

    // The writer, because CREATE EXTENSION and GRANT are writes: a reader connection would fail on a
    // read-only transaction rather than on anything that names the cause.
    final String url = "jdbc:" + database.engine() + "://" + database.writerEndpoint() + ":"
        + database.port() + "/" + database.databaseName();

    try (Connection connection =
             DriverManager.getConnection(url, database.username(), database.password());
        Statement statement = connection.createStatement()) {

      statement.execute("CREATE EXTENSION IF NOT EXISTS rds_tools");

      // Granted here rather than in the IAM instrument because this is the first point at which the schema
      // exists. Reading topology and blue/green switchover status is what the IAM tests do as this user, and
      // without these two the attempt fails as though the deployment were unhealthy.
      if (!StringUtils.isNullOrEmpty(this.iamUsername)) {
        statement.execute("GRANT USAGE ON SCHEMA rds_tools TO " + this.iamUsername);
        statement.execute("GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA rds_tools TO " + this.iamUsername);
      }
    }

    LOGGER.info(() -> "rds_tools is ready on " + database.clusterIdentifier() + ".");
    return new SimpleInstrument(this, null);
  }

  @Override
  public void destroy(final EnvConfiguration configuration, final Composition composition) {
    // Nothing to do; the extension goes away with the database. See the class comment.
  }
}
