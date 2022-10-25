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

package integration.container.standard.mariadb;

import com.mysql.cj.conf.PropertyKey;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import integration.container.standard.StandardBaseTest;
import integration.util.ContainerHelper;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import software.amazon.jdbc.Driver;

public class StandardMariadbBaseTest extends StandardBaseTest {

  @BeforeAll
  public static void setUpMariadb() throws SQLException, IOException, ClassNotFoundException {
    DB_CONN_STR_PREFIX = "jdbc:aws-wrapper:mariadb://";
    STANDARD_HOST = System.getenv("STANDARD_MARIADB_HOST");
    STANDARD_PORT = Integer.parseInt(System.getenv("STANDARD_MARIADB_PORT"));
    STANDARD_DB = System.getenv("STANDARD_MARIADB_DB");
    STANDARD_USERNAME = System.getenv("STANDARD_MARIADB_USERNAME");
    STANDARD_PASSWORD = System.getenv("STANDARD_MARIADB_PASSWORD");
    setUp();
    Class.forName("org.mariadb.jdbc.Driver");

    if (!Driver.isRegistered()) {
      Driver.register();
    }

    try {
      DriverManager.deregisterDriver(DriverManager.getDriver("jdbc:mysql://"));
    } catch (SQLException e) {
      System.out.println("MySQL driver is already deregistered");
    }
  }

  @Override
  protected Properties initDefaultProps() {
    final Properties props = initDefaultPropsNoTimeouts();
    props.setProperty(PropertyKey.connectTimeout.getKeyName(), "3");
    props.setProperty(PropertyKey.socketTimeout.getKeyName(), "3");

    return props;
  }

  @Override
  protected Properties initDefaultPropsNoTimeouts() {
    final Properties props = new Properties();
    props.setProperty(PropertyKey.USER.getKeyName(), STANDARD_USERNAME);
    props.setProperty(PropertyKey.PASSWORD.getKeyName(), STANDARD_PASSWORD);
    props.setProperty(PropertyKey.tcpKeepAlive.getKeyName(), Boolean.FALSE.toString());

    return props;
  }
}

