/*
 *
 *     Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License").
 *     You may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package software.amazon;

import software.amazon.jdbc.PropertyDefinition;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class AwsIamAuthenticationPostgresqlExample {
  public static final String POSTGRESQL_CONNECTION_STRING =
      "jdbc:aws-wrapper:postgresql://db-identifier.cluster-XYZ.us-east-2.rds.amazonaws.com:5432/employees";
  private static final String USERNAME = "john_smith";

  public static void main(String[] args) throws SQLException {

    final Properties properties = new Properties();

    // Enable AWS IAM database authentication and configure driver property values
    properties.setProperty(PropertyDefinition.PLUGINS.name, "iam");
    properties.setProperty(PropertyDefinition.USER.name, USERNAME);

    // Attempt a connection
    try (Connection conn = DriverManager.getConnection(POSTGRESQL_CONNECTION_STRING, properties);
        Statement statement = conn.createStatement();
        ResultSet result = statement.executeQuery("select aurora_db_instance_identifier()")) {

      System.out.println(Util.getResult(result));
    }
  }
}
