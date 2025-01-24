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

import com.zaxxer.hikari.HikariConfig;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import software.amazon.jdbc.ConnectionProviderManager;
import software.amazon.jdbc.Driver;
import software.amazon.jdbc.HikariPooledConnectionProvider;

public class InternalConnectionPoolPasswordWarning {

  public static void main(String[] args) throws SQLException {
    final String url = "jdbc:aws-wrapper:postgresql://test-db.cluster-XYZ.us-east-2.rds.amazonaws.com:5432/mydb";
    final String user = "username";
    final String correctPassword = "correct_password";
    final String wrongPassword = "wrong_password";

    Driver.setCustomConnectionProvider(
        new HikariPooledConnectionProvider((hostSpec, props) -> new HikariConfig()));

    // Create an internal connection pool with the correct password
    final Connection conn = DriverManager.getConnection(url, user, correctPassword);
    // Finished with connection. The connection is not actually closed here, instead it will be
    // returned to the pool but will remain open.
    conn.close();

    // Even though we use the wrong password, the original connection 'conn' will be returned by the
    // pool and we can still use it.
    try (Connection wrongPasswordConn = DriverManager.getConnection(url, user, wrongPassword)) {
      wrongPasswordConn.createStatement().executeQuery("SELECT 1");
    }

    // Closes all pools and removes all cached pool connections.
    ConnectionProviderManager.releaseResources();

    // Correctly throws an exception - creates a fresh connection pool which will check the password
    // because there are no cached pool connections.
    try (Connection wrongPasswordConn = DriverManager.getConnection(url, user, wrongPassword)) {
      // Will not reach - exception will be thrown
    }
  }
}
