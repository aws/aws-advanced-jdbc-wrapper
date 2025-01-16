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
import software.amazon.jdbc.plugin.federatedauth.OktaAuthPlugin;

public class OktaAuthPluginExample {

  private static final String CONNECTION_STRING = "jdbc:aws-wrapper:postgresql://database.cluster-xyz.us-east-1.rds.amazonaws.com:5432/postgres";

  public static void main(String[] args) throws SQLException {
    // Set the Okta Authentication Connection Plugin parameters and the JDBC Wrapper parameters.
    final Properties properties = new Properties();

    // Enable the Okta Authentication Connection Plugin.
    properties.setProperty(PropertyDefinition.PLUGINS.name, "okta");
    properties.setProperty(OktaAuthPlugin.IDP_ENDPOINT.name, "123456789.okta.com");
    properties.setProperty(OktaAuthPlugin.APP_ID.name, "abc12345678");
    properties.setProperty(OktaAuthPlugin.IAM_ROLE_ARN.name, "arn:aws:iam::123456789:role/OktaAccessRole");
    properties.setProperty(OktaAuthPlugin.IAM_IDP_ARN.name, "arn:aws:iam::123456789:saml-provider/OktaSAMLIdp");
    properties.setProperty(OktaAuthPlugin.IAM_REGION.name, "us-east-1");
    properties.setProperty(OktaAuthPlugin.IDP_USERNAME.name, "user@example.com");
    properties.setProperty(OktaAuthPlugin.IDP_PASSWORD.name, "abcd");
    properties.setProperty(OktaAuthPlugin.DB_USER.name, "iamDbUser");

    // Try and make a connection:
    try (final Connection conn = DriverManager.getConnection(CONNECTION_STRING, properties);
         final Statement statement = conn.createStatement();
         final ResultSet rs = statement.executeQuery("SELECT 1")) {
      System.out.println(Util.getResult(rs));
    }
  }
}
