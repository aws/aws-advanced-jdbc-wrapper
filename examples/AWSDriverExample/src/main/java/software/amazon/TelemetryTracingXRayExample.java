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

import com.amazonaws.xray.AWSXRay;
import com.amazonaws.xray.AWSXRayRecorderBuilder;
import com.amazonaws.xray.entities.Segment;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import software.amazon.jdbc.PropertyDefinition;

public class TelemetryTracingXRayExample {
  // User configures connection properties here
  public static final String POSTGRESQL_CONNECTION_STRING =
      "jdbc:aws-wrapper:postgresql://db-identifier-postgres.XYZ.us-east-2.rds.amazonaws.com:5432/postgres";

  private static final String USERNAME = "username";
  private static final String PASSWORD = "password";

  private static final String SQL_DBLIST = "select datname from pg_database;";
  private static final String SQL_SLEEP = "select pg_sleep(20);";

  public static void main(String[] args) throws SQLException {
    // Initiating an XRay recorder in the code
    AWSXRayRecorderBuilder builder = AWSXRayRecorderBuilder.standard();
    AWSXRay.setGlobalRecorder(builder.build());

    // Properties
    final Properties properties = new Properties();
    properties.setProperty(PropertyDefinition.PLUGINS.name, "dataCache,efm2,failover");
    properties.setProperty(PropertyDefinition.USER.name, USERNAME);
    properties.setProperty(PropertyDefinition.PASSWORD.name, PASSWORD);

    properties.setProperty(PropertyDefinition.ENABLE_TELEMETRY.name, String.valueOf(true));
    properties.setProperty(PropertyDefinition.TELEMETRY_SUBMIT_TOPLEVEL.name, String.valueOf(false));
    // Traces: Available values are XRAY, OTLP and NONE
    properties.setProperty(PropertyDefinition.TELEMETRY_TRACES_BACKEND.name, "XRAY");
    // Metrics: Available values are OTLP and NONE
    properties.setProperty(PropertyDefinition.TELEMETRY_METRICS_BACKEND.name, "NONE");

    // Application
    System.out.println("-- running application");

    // This example opens a telemetry segment inside the code.
    try (Segment segment = AWSXRay.beginSegment("application")) {
      try (final Connection conn = DriverManager.getConnection(POSTGRESQL_CONNECTION_STRING, properties);
           final Statement statement = conn.createStatement();
           final ResultSet rs = statement.executeQuery(SQL_DBLIST)) {
        System.out.println(Util.getResult(rs));
      }
    }
    System.out.println("-- end of application");
  }
}
