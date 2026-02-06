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
import software.amazon.jdbc.authentication.AwsCredentialsManager;
import software.amazon.jdbc.plugin.federatedauth.OktaAuthPlugin;

public class OktaAuthPluginExample {

  private static final String CONNECTION_STRING = "jdbc:aws-wrapper:postgresql://gdb-test-pg.global-gvcb7gbn2rvs.global.rds.amazonaws.com:5432/postgres";

  public static void main(String[] args) throws SQLException {
    // Set the Okta Authentication Connection Plugin parameters and the JDBC Wrapper parameters.
    final Properties properties = new Properties();

    // Enable the AWS Federated Authentication Connection Plugin.
    properties.setProperty(PropertyDefinition.PLUGINS.name, "okta");

    properties.setProperty(OktaAuthPlugin.IDP_ENDPOINT.name, "integrator-3100847.okta.com");
    properties.setProperty(OktaAuthPlugin.APP_ID.name, "exkzvgq3f3wLAjcfx697");
    properties.setProperty(OktaAuthPlugin.IDP_USERNAME.name, "karen.chen@improving.com");
    properties.setProperty(OktaAuthPlugin.IDP_PASSWORD.name, "!FQJ5@Gs");

    properties.setProperty(OktaAuthPlugin.IAM_ROLE_ARN.name, "arn:aws:iam::346558184882:role/OktaAccessRole");
    properties.setProperty(OktaAuthPlugin.IAM_IDP_ARN.name, "arn:aws:iam::346558184882:saml-provider/OktaSAMLIdp");

    properties.setProperty(OktaAuthPlugin.IAM_REGION.name, "us-east-2");
    properties.setProperty(OktaAuthPlugin.DB_USER.name, "jane_doe");

    properties.setProperty("wrapperDialect", "global-aurora-pg");
    properties.setProperty("failoverMode", "writer");
    properties.setProperty("clusterId", "test-global");
    properties.setProperty("globalClusterInstanceHostPatterns",
        "?.cwpu2jclcwdc.us-east-2.rds.amazonaws.com,?.crksgsebp0nr.us-west-2.rds.amazonaws.com");

    // Try and make a connection:
    try (final Connection conn = DriverManager.getConnection(CONNECTION_STRING, properties);
        final Statement statement = conn.createStatement();
        final ResultSet rs = statement.executeQuery("SELECT * FROM aurora_db_instance_identifier()")) {
      System.out.println(Util.getResult(rs));
    }
  }
}
