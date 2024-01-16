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
import java.sql.SQLException;
import java.util.Properties;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.DriverMetaDataConnectionPlugin;

/**
 * Establish a basic connection to the PostgreSQL database and retrieve the driver name.
 */
public class DriverMetaDataConnectionPluginExample {

  public static final String DRIVER_NAME = "PostgreSQL JDBC Driver";
  public static final String USERNAME = "user";
  public static final String PASSWORD = "pass";
  public static final String POSTGRESQL_CONNECTION_STRING =
      "jdbc:aws-wrapper:postgresql://db.cluster-XYZ.REGION.rds.amazonaws.com:5432/postgres";

  public static void main(String[] args) throws SQLException {
    final Properties properties = new Properties();
    properties.setProperty("user", USERNAME);
    properties.setProperty("password", PASSWORD);
    PropertyDefinition.PLUGINS.set(properties, "driverMetaData,failover,efm2");

    // DriverMetaDataConnectionPlugin Settings
    // Override the return value of DatabaseMetaData#getDriverName to "PostgreSQL JDBC Driver"
    DriverMetaDataConnectionPlugin.WRAPPER_DRIVER_NAME.set(properties, DRIVER_NAME);

    try (Connection conn = DriverManager.getConnection(POSTGRESQL_CONNECTION_STRING, properties)) {
      System.out.println(conn.getMetaData().getDriverName());
    }
  }
}
