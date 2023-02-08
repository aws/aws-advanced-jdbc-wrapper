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

package software.amazon.jdbc.plugin.readwritesplitting;

import static org.junit.jupiter.api.Assertions.assertNotEquals;

import integration.refactored.DatabaseEngine;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.PropertyDefinition;

public class SwappableStatementTest {
  private static final String url = "my-url";
  private static final String username = "username";
  private static final String password = "password";

  @Test
  public void swappableStatementTest() throws SQLException {
    DriverManager.registerDriver(new software.amazon.jdbc.Driver());
    DriverManager.registerDriver(new org.postgresql.Driver());

    final Properties props = new Properties();
    props.setProperty(PropertyDefinition.PLUGINS.name, "auroraHostList,readWriteSplitting");
    props.setProperty(PropertyDefinition.USER.name, username);
    props.setProperty(PropertyDefinition.PASSWORD.name, password);

    // Switch to new reader after each transaction
    props.setProperty(ReadWriteSplittingPlugin.LOAD_BALANCE_READ_ONLY_TRAFFIC.name, "true");

    try (Connection conn = DriverManager.getConnection(url, props)) {
      Statement reusableStmt = conn.createStatement();
      String writerId = executeInstanceIdQuery(reusableStmt);

      conn.setReadOnly(true);
      conn.setAutoCommit(false);
      String readerId = executeInstanceIdQuery(reusableStmt);
      assertNotEquals(writerId, readerId);
      conn.commit();

      for (int i = 0; i < 5; i++) {
        String nextReaderId = executeInstanceIdQuery(reusableStmt);
        assertNotEquals(readerId, nextReaderId);
        readerId = nextReaderId;
        conn.commit();
      }
    }
  }

  private String executeInstanceIdQuery(Statement stmt)
      throws SQLException {
    try (final ResultSet rs = stmt.executeQuery("SELECT aurora_db_instance_identifier()")) {
      if (rs.next()) {
        String result = rs.getString(1);
        System.out.println(result);
        return result;
      }
    }
    return null;
  }

}
