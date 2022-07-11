/*
*    Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
* 
*    Licensed under the Apache License, Version 2.0 (the "License").
*    You may not use this file except in compliance with the License.
*    You may obtain a copy of the License at
* 
*    http://www.apache.org/licenses/LICENSE-2.0
* 
*    Unless required by applicable law or agreed to in writing, software
*    distributed under the License is distributed on an "AS IS" BASIS,
*    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*    See the License for the specific language governing permissions and
*    limitations under the License.
*/

package integration;

import static org.junit.jupiter.api.Assertions.assertTrue;

import integration.util.TestSettings;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import software.aws.rds.jdbc.proxydriver.wrapper.ConnectionWrapper;

@Disabled
public class PostgresTests {

  @Test
  public void testOpenConnection() throws SQLException {

    if (!org.postgresql.Driver.isRegistered()) {
      org.postgresql.Driver.register();
    }

    if (!software.aws.rds.jdbc.proxydriver.Driver.isRegistered()) {
      software.aws.rds.jdbc.proxydriver.Driver.register();
    }

    Properties props = new Properties();
    props.setProperty("user", TestSettings.postgresqlUser);
    props.setProperty("password", TestSettings.postgresqlPassword);

    Connection conn =
        DriverManager.getConnection(
            "aws-proxy-jdbc:postgresql://"
                + TestSettings.postgresqlServerName
                + "/"
                + TestSettings.postgresqlDatabase,
            props);

    assertTrue(conn instanceof ConnectionWrapper);
    assertTrue(conn.isWrapperFor(org.postgresql.PGConnection.class));

    assertTrue(conn.isValid(10));
    conn.close();
  }
}
