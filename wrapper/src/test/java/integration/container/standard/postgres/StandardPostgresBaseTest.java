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

import com.amazon.awslabs.jdbc.Driver;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
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
import org.postgresql.PGProperty;

public class StandardPostgresBaseTest {
  protected static final String DB_CONN_STR_PREFIX = "aws-jdbc-wrapper:postgresql://";
  protected static final String STANDARD_POSTGRES_HOST = System.getenv("STANDARD_POSTGRES_HOST");
  protected static final int STANDARD_POSTGRES_PORT = Integer.parseInt(System.getenv("STANDARD_POSTGRES_PORT"));
  protected static final String STANDARD_POSTGRES_DB = System.getenv("STANDARD_POSTGRES_DB");
  protected static final String STANDARD_POSTGRES_USERNAME = System.getenv("STANDARD_POSTGRES_USERNAME");
  protected static final String STANDARD_POSTGRES_PASSWORD = System.getenv("STANDARD_POSTGRES_PASSWORD");

  protected static final String TOXIPROXY_HOST = System.getenv("TOXIPROXY_HOST");
  protected static ToxiproxyClient toxiproxyClient;
  protected static final int TOXIPROXY_CONTROL_PORT = 8474;

  protected static final String PROXIED_DOMAIN_NAME_SUFFIX = System.getenv("PROXIED_DOMAIN_NAME_SUFFIX");
  protected static final String PROXY_PORT = System.getenv("PROXY_PORT");
  protected static Proxy proxy;
  protected static final Map<String, Proxy> proxyMap = new HashMap<>();

  protected final ContainerHelper containerHelper = new ContainerHelper();

  @BeforeAll
  public static void setUp() throws SQLException, IOException {
    toxiproxyClient = new ToxiproxyClient(TOXIPROXY_HOST, TOXIPROXY_CONTROL_PORT);
    proxy = getProxy(toxiproxyClient, STANDARD_POSTGRES_HOST, STANDARD_POSTGRES_PORT);
    proxyMap.put(STANDARD_POSTGRES_HOST, proxy);

    if (!org.postgresql.Driver.isRegistered()) {
      org.postgresql.Driver.register();
    }

    if (!Driver.isRegistered()) {
      Driver.register();
    }
  }

  @BeforeEach
  public void setUpEach() {
    proxyMap.forEach((instance, proxy) -> containerHelper.enableConnectivity(proxy));
  }

  protected static Proxy getProxy(ToxiproxyClient proxyClient, String host, int port) throws IOException {
    final String upstream = host + ":" + port;
    return proxyClient.getProxy(upstream);
  }

  protected String getUrl() {
    String url =
        DB_CONN_STR_PREFIX + STANDARD_POSTGRES_HOST + ":" + STANDARD_POSTGRES_PORT + "/" + STANDARD_POSTGRES_DB;
    return url;
  }

  protected Connection connect() throws SQLException {
    return DriverManager.getConnection(getUrl(), initDefaultProps());
  }

  protected String getProxiedUrl() {
    String url = DB_CONN_STR_PREFIX + STANDARD_POSTGRES_HOST + PROXIED_DOMAIN_NAME_SUFFIX + ":" + PROXY_PORT + "/"
        + STANDARD_POSTGRES_DB;
    return url;
  }

  protected Connection connectCustomUrl(String url, Properties props) throws SQLException {
    return DriverManager.getConnection(url, props);
  }

  protected Connection connectToProxy() throws SQLException {
    return DriverManager.getConnection(getProxiedUrl(), initDefaultProps());
  }

  protected Properties initDefaultProps() {
    final Properties props = initDefaultPropsNoTimeouts();
    props.setProperty(PGProperty.CONNECT_TIMEOUT.getName(), "3");
    props.setProperty(PGProperty.SOCKET_TIMEOUT.getName(), "3");

    return props;
  }

  protected Properties initDefaultPropsNoTimeouts() {
    final Properties props = new Properties();
    props.setProperty(PGProperty.USER.getName(), STANDARD_POSTGRES_USERNAME);
    props.setProperty(PGProperty.PASSWORD.getName(), STANDARD_POSTGRES_PASSWORD);
    props.setProperty(PGProperty.TCP_KEEP_ALIVE.getName(), Boolean.FALSE.toString());

    return props;
  }
}
