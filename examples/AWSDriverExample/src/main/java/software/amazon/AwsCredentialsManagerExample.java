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

import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.authentication.AwsCredentialsManager;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class AwsCredentialsManagerExample {

  public static final String POSTGRESQL_URL = "db-identifier-postgres.XYZ.us-east-2.rds.amazonaws.com";
  public static final String POSTGRESQL_CONNECTION_STRING =
      "jdbc:aws-wrapper:postgresql://" + POSTGRESQL_URL + ":5432/employees";
  private static final String POSTGRESQL_IAM_USER = "pg_iam_user";

  public static final String MYSQL_URL = "db-identifier-mysql.XYZ.us-east-2.rds.amazonaws.com";
  public static final String MYSQL_CONNECTION_STRING =
      "jdbc:aws-wrapper:mysql://" + MYSQL_URL + ":3306/items";
  private static final String MYSQL_IAM_USER = "mysql_iam_user";

  public static void main(String[] args) throws SQLException {

    // Configure AwsCredentialsManager to use EnvironmentVariableCredentialsProvider when connecting
    // to MySQL and DefaultCredentialsProvider otherwise.
    AwsCredentialsManager.setCustomHandler((hostSpec, props) -> {
      if (MYSQL_URL.equals(hostSpec.getHost())) {
        return EnvironmentVariableCredentialsProvider.create();
      } else {
        return DefaultCredentialsProvider.create();
      }
    });

    // Enable AWS IAM database authentication and configure driver property values.
    final Properties postgresProps = new Properties();
    postgresProps.setProperty(PropertyDefinition.PLUGINS.name, "iam");
    postgresProps.setProperty(PropertyDefinition.USER.name, POSTGRESQL_IAM_USER);

    // Connect to Postgres
    try (Connection conn = DriverManager.getConnection(POSTGRESQL_CONNECTION_STRING, postgresProps);
         Statement statement = conn.createStatement();
         ResultSet result = statement.executeQuery("SELECT pg_catalog.aurora_db_instance_identifier()")) {

      System.out.println(Util.getResult(result));
    }

    // Enable AWS IAM database authentication and configure driver property values.
    final Properties mysqlProps = new Properties();
    mysqlProps.setProperty(PropertyDefinition.PLUGINS.name, "iam");
    mysqlProps.setProperty(PropertyDefinition.USER.name, MYSQL_IAM_USER);

    // Connect to MySQL
    try (Connection conn = DriverManager.getConnection(MYSQL_CONNECTION_STRING, mysqlProps);
         Statement statement = conn.createStatement();
         ResultSet result = statement.executeQuery("SELECT @@aurora_server_id")) {

      System.out.println(Util.getResult(result));
    }
  }
}
