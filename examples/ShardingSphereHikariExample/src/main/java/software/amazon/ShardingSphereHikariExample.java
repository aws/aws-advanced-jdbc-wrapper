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

package software.amazon;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import javax.sql.DataSource;
import org.apache.shardingsphere.driver.api.yaml.YamlShardingSphereDataSourceFactory;
import software.amazon.jdbc.plugin.failover.FailoverFailedSQLException;
import software.amazon.jdbc.plugin.failover.FailoverSuccessSQLException;
import software.amazon.jdbc.plugin.failover.TransactionStateUnknownSQLException;

public class ShardingSphereHikariExample {

  public static void main(String[] args) throws Exception {
    final File yamlFile = new File(ShardingSphereHikariExample.class.getClassLoader().getResource("sharding.yml")
        .getFile());

    final DataSource dataSource = YamlShardingSphereDataSourceFactory.createDataSource(yamlFile);

    initializeTables(dataSource);

    int maxRetries = 3;
    int attempt = 0;

    try (Connection conn = dataSource.getConnection()) {
      while (attempt < maxRetries) {
        try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO example (id, status) VALUES (?, ?)")) {
          stmt.setInt(1, 1);
          stmt.setInt(2, 100);
          stmt.executeUpdate();

          stmt.setInt(1, 2);
          stmt.setInt(2, 200);
          stmt.executeUpdate();

          System.out.println("Data inserted");
          break;
        } catch (FailoverSuccessSQLException e) {
          attempt++;
          System.out.println("Failover occurred, retrying... (attempt " + attempt + ")");
          if (attempt >= maxRetries) {
            throw e;
          }
        } catch (TransactionStateUnknownSQLException e) {
          // User application should check the status of the failed transaction and restart it if needed. See:
          // https://github.com/aws/aws-advanced-jdbc-wrapper/blob/main/docs/using-the-jdbc-driver/using-plugins/UsingTheFailoverPlugin.md#08007---transaction-resolution-unknown
          throw e;
        } catch (FailoverFailedSQLException e) {
          // User application should open a new connection, check the results of the failed transaction and re-run it if
          // needed. See:
          // https://github.com/aws/aws-advanced-jdbc-wrapper/blob/main/docs/using-the-jdbc-driver/using-plugins/UsingTheFailoverPlugin.md#08001---unable-to-establish-sql-connection
          throw e;
        } catch (Exception e) {
          // Unexpected exception unrelated to failover. This should be handled by the user application.
          throw e;
        }
      }
    }

  }

  private static void initializeTables(DataSource dataSource) throws Exception {
    try (Connection conn = dataSource.getConnection();
        Statement stmt = conn.createStatement()) {

      // Create table
      stmt.execute("CREATE TABLE IF NOT EXISTS example (id INT, status INT)");
      System.out.println("Table initialized");
    }
  }
}
