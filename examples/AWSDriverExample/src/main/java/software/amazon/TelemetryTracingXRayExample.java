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
      "jdbc:aws-wrapper:postgresql://atlas-postgres.cluster-czygpppufgy4.us-east-2.rds.amazonaws.com:5432/postgres";

  private static final String SQL_DBLIST = "select datname from pg_database;";
  private static final String SQL_SLEEP = "select pg_sleep(20);";
  private static final String SQL_TABLELIST = "select * from information_schema.tables where table_schema='public';";

  public TelemetryTracingXRayExample() {
  }

  public void runExampleQuery1(Properties properties) throws SQLException {
    try (final Connection conn = DriverManager.getConnection(POSTGRESQL_CONNECTION_STRING, properties);
         final Statement statement = conn.createStatement();
         final ResultSet rs = statement.executeQuery(SQL_DBLIST)) {
      //AWSXRay.beginSegment("part of application");
      System.out.println(Util.getResult(rs));
      //AWSXRay.endSegment();
    }
  }

  public void runExampleQuery2(Properties properties) throws SQLException {
    try (final Connection conn = DriverManager.getConnection(POSTGRESQL_CONNECTION_STRING, properties);
         final Statement statement = conn.createStatement();
         final ResultSet rs = statement.executeQuery(SQL_TABLELIST)) {
      //AWSXRay.beginSegment("part of application");
      System.out.println(Util.getResult(rs));
      //AWSXRay.endSegment();
    }
  }

  public void runExampleQuery3(Properties properties) throws SQLException {
    try (final Connection conn = DriverManager.getConnection(POSTGRESQL_CONNECTION_STRING, properties);
         final Statement statement = conn.createStatement();
         final ResultSet rs = statement.executeQuery(SQL_SLEEP)) {
      //AWSXRay.beginSegment("part of application");
      System.out.println(Util.getResult(rs));
      //AWSXRay.endSegment();
    }
  }

  public static void main(String[] args) throws SQLException {
    final TelemetryTracingXRayExample example = new TelemetryTracingXRayExample();

    AWSXRayRecorderBuilder builder = AWSXRayRecorderBuilder.standard();
    AWSXRay.setGlobalRecorder(builder.build());

    final Properties properties = new Properties();
    properties.setProperty(PropertyDefinition.PLUGINS.name, "dataCache, efm, failover");
    properties.setProperty(PropertyDefinition.USER.name, MetricsUtil.USERNAME);
    properties.setProperty(PropertyDefinition.PASSWORD.name, MetricsUtil.PASSWORD);

    properties.setProperty(PropertyDefinition.ENABLE_TELEMETRY.name, String.valueOf(true));
    properties.setProperty(PropertyDefinition.TELEMETRY_SUBMIT_TOPLEVEL.name, String.valueOf(true));
    // Traces: Available values are XRAY, OTLP and NONE
    properties.setProperty(PropertyDefinition.TELEMETRY_TRACES_BACKEND.name, "XRAY");
    // Metrics: Available values are OTLP and NONE
    properties.setProperty(PropertyDefinition.TELEMETRY_METRICS_BACKEND.name, "NONE");

    System.out.println("-- starting metrics e2e test");

    System.out.println("-- running application");
    try (Segment segment = AWSXRay.beginSegment("application")) {
      example.runExampleQuery3(properties);
    }
    System.out.println("-- end of application");
  }
}
