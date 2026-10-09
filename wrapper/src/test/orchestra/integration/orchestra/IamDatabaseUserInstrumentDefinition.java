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
import java.util.List;
import java.util.logging.Logger;
import software.amazon.orchestra.Composition;
import software.amazon.orchestra.EnvConfiguration;
import software.amazon.orchestra.Instrument;
import software.amazon.orchestra.InstrumentDefinition;
import software.amazon.orchestra.SimpleInstrument;
import software.amazon.orchestra.contract.Database;
import software.amazon.orchestra.instruments.aws.DatabaseClusterState;

/**
 * Grants a database user permission to authenticate with IAM.
 *
 * <p>The migrated equivalent of {@code TestEnvironment.configureIamAccess}. IAM authentication needs two
 * things that look like one: an AWS identity allowed to generate a token, and a <em>database</em> user with
 * {@code rds_iam} granted. Credentials in the container cover the first. Nothing covered the second, which
 * is why the IAM tests failed with a null username even after credentials reached the container - the user
 * they were meant to connect as had never been created.
 *
 * <p>An instrument rather than a step inside the cluster instrument, because it is a different concern with
 * a different lifetime: the cluster is an AWS resource, this is a row in the database. Keeping them apart is
 * also what lets a composition provision a cluster without IAM, which is what the {@code IAM} feature flag
 * is for.
 *
 * <p>It lives in this repository rather than in Orchestra deliberately. The SQL is engine-specific and
 * expresses what <em>this</em> suite needs its IAM user to be able to do; a general-purpose framework
 * guessing at grants would be guessing at a consumer's authorization model.
 *
 * <h2>Nothing to tear down</h2>
 *
 * <p>The user lives inside the database, so deleting the cluster deletes it. An explicit {@code DROP} in
 * teardown would be work that only matters in the one case it cannot serve - a reused cluster - and there
 * the {@code CREATE} path already drops first.
 */
public class IamDatabaseUserInstrumentDefinition implements InstrumentDefinition {

  private static final Logger LOGGER =
      Logger.getLogger(IamDatabaseUserInstrumentDefinition.class.getName());

  private final String username;

  /**
   * Creates a definition granting IAM access to one user.
   *
   * @param username the database user to create, matching what the in-container tests connect as
   */
  public IamDatabaseUserInstrumentDefinition(final String username) {
    this.username = username;
  }

  @Override
  public List<Class<?>> getProvisionDependencies() {
    // After the cluster: this connects to it with the master credentials it publishes.
    return java.util.Collections.<Class<?>>singletonList(Database.class);
  }

  @Override
  public Instrument build(final EnvConfiguration configuration, final Composition composition)
      throws SQLException {

    final DatabaseClusterState cluster =
        composition.getInstrumentState(Database.class, DatabaseClusterState.class);

    // The writer endpoint, and it has to be the writer: CREATE USER and GRANT are writes, and a reader
    // connection would fail on a read-only transaction rather than on anything informative.
    //
    // The subprotocol comes from the database rather than from a constructor argument, which is what makes
    // this instrument work in an engine matrix without being rebound per slot: DatabaseClusterState.engine()
    // is already the JDBC name of whatever was provisioned.
    final String engine = cluster.engine();
    final String url = "jdbc:" + engine + "://" + cluster.writerEndpoint() + ":" + cluster.port()
        + "/" + cluster.databaseName();

    try (Connection connection =
             DriverManager.getConnection(url, cluster.username(), cluster.password());
         Statement statement = connection.createStatement()) {

      for (final String sql : grants(engine, cluster.databaseName())) {
        statement.execute(sql);
      }
    }

    LOGGER.info(() -> "IAM database user '" + this.username + "' is ready on "
        + cluster.clusterIdentifier() + ".");

    return new SimpleInstrument(this, null);
  }

  /**
   * Returns the statements that create the IAM user and let it do what the suite needs.
   *
   * <p>Ported from {@code AuroraTestUtility.addAuroraAwsIamUser}, including the grants that are not obviously
   * about IAM. The two engines have nothing in common here beyond the drop-then-create shape: PostgreSQL
   * makes a role and grants it the {@code rds_iam} role, while MySQL names the authentication plugin on the
   * user itself and then needs privileges granted per host pattern.
   *
   * <p>Dropped first so a reused database converges instead of failing on an existing user, and in that
   * order rather than as a replace, because PostgreSQL has no {@code CREATE OR REPLACE} for roles.
   *
   * <p>{@code REPLICATION CLIENT} and {@code SELECT ON mysql.*} are on the list because the blue/green tests
   * read switchover status as this user, which the harness's comment records as the reason. Nothing else in
   * the suite needs them, and leaving them out would fail only in blue/green mode - the mode that is hardest
   * to attribute a failure in, since it costs an hour to reach.
   *
   * @param engine the JDBC name of the provisioned engine
   * @param databaseName the database that was created
   * @return the statements, in the order they must run
   */
  private List<String> grants(final String engine, final String databaseName) {
    if ("postgresql".equals(engine)) {
      return java.util.Arrays.asList(
          "DROP USER IF EXISTS " + this.username + ";",
          "CREATE USER " + this.username + ";",
          "GRANT rds_iam TO " + this.username + ";",
          "GRANT ALL PRIVILEGES ON DATABASE " + databaseName + " TO " + this.username + ";");
    }

    if ("mysql".equals(engine)) {
      return java.util.Arrays.asList(
          "DROP USER IF EXISTS " + this.username + ";",
          // The plugin name is what makes an IAM token a valid password for this user. Without it the user
          // exists, the token is minted, and authentication fails as though the password were wrong.
          "CREATE USER " + this.username + " IDENTIFIED WITH AWSAuthenticationPlugin AS 'RDS';",
          "GRANT ALL PRIVILEGES ON " + databaseName + ".* TO '" + this.username + "'@'%';",
          "GRANT REPLICATION CLIENT ON *.* TO '" + this.username + "'@'%';",
          "GRANT SELECT ON mysql.* TO '" + this.username + "'@'%';");
    }

    throw new UnsupportedOperationException(
        "No IAM user statements are known for the " + engine + " engine. Add them rather than provisioning "
            + "a database whose IAM tests cannot authenticate.");
  }

  @Override
  public void destroy(final EnvConfiguration configuration, final Composition composition) {
    // Nothing to do; the user goes away with the cluster. See the class comment.
  }
}
