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
import software.amazon.jdbc.plugin.FederatedAuthConnectionPlugin;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class FederatedAuthConnectionPluginExample {

  private static final String CONNECTION_STRING = "jdbc:aws-wrapper:postgresql://db-identifier.XYZ.us-east-2.rds.amazonaws.com:5432/employees";

  public static void main(String[] args) throws SQLException {
    // Set the AWS Federated Authentication Connection Plugin parameters and the JDBC Wrapper parameters.
    final Properties properties = new Properties();

    // Enable the AWS Federated Authentication Connection Plugin.
    properties.setProperty(PropertyDefinition.PLUGINS.name, "federatedAuth");
    properties.setProperty(FederatedAuthConnectionPlugin.IDP_HOST.name, "ec2amaz-ab3cdef.example.com");
    properties.setProperty(FederatedAuthConnectionPlugin.IAM_ROLE_ARN.name, "arn:aws:iam::123456789012:role/adfs_example_iam_role");
    properties.setProperty(FederatedAuthConnectionPlugin.IAM_IDP_ARN.name, "arn:aws:iam::123456789012:saml-provider/adfs_example");
    properties.setProperty(FederatedAuthConnectionPlugin.IAM_REGION.name, "us-east-2");
    properties.setProperty(FederatedAuthConnectionPlugin.IDP_USER_NAME.name, "someFederatedUsername@teamatlas.example.com");
    properties.setProperty(FederatedAuthConnectionPlugin.IDP_USER_PASSWORD.name, "somePassword");
    properties.setProperty(PropertyDefinition.USER.name, "someIamUser");


    // Try and make a connection:
    try (final Connection conn = DriverManager.getConnection(CONNECTION_STRING, properties);
         final Statement statement = conn.createStatement();
         final ResultSet rs = statement.executeQuery("SELECT 1")) {
      System.out.println(Util.getResult(rs));
    }
  }
}
