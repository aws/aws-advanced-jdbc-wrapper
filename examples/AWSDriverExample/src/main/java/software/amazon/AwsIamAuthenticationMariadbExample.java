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

public class AwsIamAuthenticationMariadbExample {
  public static final String MYSQL_CONNECTION_STRING =
      "jdbc:aws-wrapper:mysql://db-identifier.cluster-XYZ.us-east-2.rds.amazonaws.com:3306?permitMysqlScheme";

  private static final String USERNAME = "john_smith";

  public static void main(String[] args) {

    final Properties properties = new Properties();

    // Enable AWS IAM database authentication and configure driver property values
    properties.setProperty(PropertyDefinition.USER.name, USERNAME);
    properties.setProperty(PropertyDefinition.PLUGINS.name, "iam");

    // The following two properties are required when using the IAM Authentication plugin with the MariaDB driver.
    properties.setProperty("sslMode", "verify-ca");

    // The value of this property should be the path to an SSL certificate.
    // The certificates can be found here: https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.SSL.html.
    properties.setProperty("serverSslCert", "path/to/certificate.pem");

    // Attempt a connection
    try (Connection conn = DriverManager.getConnection(MYSQL_CONNECTION_STRING, properties);
        Statement stmt1 = conn.createStatement();
        ResultSet rs = stmt1.executeQuery("SELECT 1")) {

      while (rs.next()) {
        System.out.println(rs.getString(1));
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
