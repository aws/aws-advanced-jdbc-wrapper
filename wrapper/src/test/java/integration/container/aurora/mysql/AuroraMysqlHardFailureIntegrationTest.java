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

package integration.container.aurora.mysql;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AuroraMysqlHardFailureIntegrationTest extends AuroraMysqlBaseTest {
  protected final HashSet<String> instancesToCrash = new HashSet<>();
  protected ExecutorService crashInstancesExecutorService;

  /** Current reader dies, execute a “write“ statement, an exception should be thrown. */
  @Test
  public void test_readerDiesAndExecuteWriteQuery()
      throws SQLException, InterruptedException {

    // Connect to Instance2 which is a reader.
    assertTrue(clusterSize >= 5, "Minimal cluster configuration: 1 writer + 4 readers");
    final String readerEndpoint = instanceIDs[1];

    final Properties props = initDefaultProps();
    try (final Connection conn = connectToInstance(readerEndpoint + DB_CONN_STR_SUFFIX, AURORA_MYSQL_PORT, props)) {
      // Crash Instance2.
      startCrashingInstances(readerEndpoint);
      makeSureInstancesDown(readerEndpoint);

      final Statement testStmt1 = conn.createStatement();

      // Assert that an exception with SQLState code S1009 is thrown.
      final SQLException exception =
          assertThrows(
              SQLException.class, () -> testStmt1.executeUpdate("DROP TABLE IF EXISTS test2_6"));
      assertEquals("S1009", exception.getSQLState());

      // This ensures that the write statement above did not kick off a driver side failover.
      assertFirstQueryThrows(conn, "08S02");
    }
  }

  @BeforeEach
  private void validateCluster() throws InterruptedException, SQLException, IOException {
    crashInstancesExecutorService = Executors.newFixedThreadPool(clusterSize);
    instancesToCrash.clear();
    for (final String id : instanceIDs) {
      crashInstancesExecutorService.submit(() -> {
        while (true) {
          if (instancesToCrash.contains(id)) {
            try (final Connection conn = connectToInstance(id + DB_CONN_STR_SUFFIX, AURORA_MYSQL_PORT, initFailoverDisabledProps());
                final Statement myStmt = conn.createStatement()) {
              myStmt.execute("ALTER SYSTEM CRASH INSTANCE");
            } catch (final SQLException e) {
              // Do nothing and keep creating connection to crash instance.
            }
          }
          TimeUnit.MILLISECONDS.sleep(100);
        }
      });
    }
    crashInstancesExecutorService.shutdown();

    super.setUpEach();
  }

  @AfterEach
  private void reviveInstancesAndCloseTestConnection() throws InterruptedException {
    instancesToCrash.clear();
    crashInstancesExecutorService.shutdownNow();

    makeSureInstancesUp(instanceIDs, false);
  }

  protected void startCrashingInstances(String... instances) {
    instancesToCrash.addAll(Arrays.asList(instances));
  }

  protected void makeSureInstancesDown(String... instances) throws InterruptedException {
    final ExecutorService executorService = Executors.newFixedThreadPool(instances.length);
    final HashSet<String> remainingInstances = new HashSet<>(Arrays.asList(instances));

    for (final String id : instances) {
      executorService.submit(() -> {
        while (true) {
          try (final Connection conn = connectToInstance(id + DB_CONN_STR_SUFFIX, AURORA_MYSQL_PORT, initFailoverDisabledProps())) {
            // Continue waiting until instance is down.
          } catch (final SQLException e) {
            remainingInstances.remove(id);
            break;
          }
          TimeUnit.MILLISECONDS.sleep(500);
        }
        return null;
      });
    }
    executorService.shutdown();
    executorService.awaitTermination(120, TimeUnit.SECONDS);

    assertTrue(remainingInstances.isEmpty(), "The following instances are still up: \n"
        + String.join("\n", remainingInstances));
  }
}
