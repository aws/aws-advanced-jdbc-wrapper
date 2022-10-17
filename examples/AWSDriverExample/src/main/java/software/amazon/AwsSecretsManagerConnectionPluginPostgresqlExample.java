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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import software.amazon.jdbc.PropertyDefinition;

public class AwsSecretsManagerConnectionPluginPostgresqlExample {

  private static final String CONNECTION_STRING = "jdbc:aws-wrapper:postgresql://db-identifier.cluster-XYZ.us-east-2.rds.amazonaws.com:5432/employees";

  public static void main(String[] args) throws SQLException {
    // Set the AWS Secrets Manager Connection Plugin parameters and the JDBC Wrapper parameters.
    final Properties properties = new Properties();
    properties.setProperty("secretsManagerRegion", "us-east-2");
    properties.setProperty("secretsManagerSecretId", "secretId");

    // Enable the AWS Secrets Manager Connection Plugin.
    properties.setProperty(PropertyDefinition.PLUGINS.name, "awsSecretsManager");

    // Try and make a connection:
    try (final Connection conn = DriverManager.getConnection(CONNECTION_STRING, properties);
         final Statement statement = conn.createStatement();
         final ResultSet rs = statement.executeQuery("SELECT * FROM employees")) {
      System.out.println(Util.getResult(rs));
    }
  }
}
