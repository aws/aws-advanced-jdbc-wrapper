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

package integration.container.standard.postgres;

import integration.container.standard.StandardBaseTest;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.BeforeAll;
import org.postgresql.PGProperty;
import software.amazon.jdbc.Driver;

public class StandardPostgresBaseTest extends StandardBaseTest {

  @BeforeAll
  public static void setUpPostgres() throws SQLException, IOException, ClassNotFoundException {
    DB_CONN_STR_PREFIX = "jdbc:aws-wrapper:postgresql://";
    STANDARD_WRITER = System.getenv("STANDARD_POSTGRES_WRITER");
    STANDARD_READER = System.getenv("STANDARD_POSTGRES_READER");
    STANDARD_PORT = Integer.parseInt(System.getenv("STANDARD_POSTGRES_PORT"));
    STANDARD_DB = System.getenv("STANDARD_POSTGRES_DB");
    STANDARD_USERNAME = System.getenv("STANDARD_POSTGRES_USERNAME");
    STANDARD_PASSWORD = System.getenv("STANDARD_POSTGRES_PASSWORD");
    QUERY_FOR_HOSTNAME = "SELECT inet_server_addr()";
    instanceIDs = new String[]{STANDARD_WRITER, STANDARD_READER};

    setUp();
    if (!org.postgresql.Driver.isRegistered()) {
      org.postgresql.Driver.register();
    }

    if (!Driver.isRegistered()) {
      Driver.register();
    }
  }

  protected Connection connectCustomUrl(String url, Properties props) throws SQLException {
    return DriverManager.getConnection(url, props);
  }

  @Override
  protected Properties initDefaultProps() {
    final Properties props = initDefaultPropsNoTimeouts();
    props.setProperty(PGProperty.CONNECT_TIMEOUT.getName(), "3");
    props.setProperty(PGProperty.SOCKET_TIMEOUT.getName(), "3");

    return props;
  }

  @Override
  protected Properties initDefaultPropsNoTimeouts() {
    final Properties props = new Properties();
    props.setProperty(PGProperty.USER.getName(), STANDARD_USERNAME);
    props.setProperty(PGProperty.PASSWORD.getName(), STANDARD_PASSWORD);
    props.setProperty(PGProperty.TCP_KEEP_ALIVE.getName(), Boolean.FALSE.toString());

    return props;
  }
}
