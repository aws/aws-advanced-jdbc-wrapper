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

package integration.container.standard;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import integration.util.ContainerHelper;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;

public abstract class StandardBaseTest {
  protected static String DB_CONN_STR_PREFIX;
  protected static String STANDARD_WRITER;
  protected static String STANDARD_READER;
  protected static Integer STANDARD_PORT;
  protected static String STANDARD_DB;
  protected static String STANDARD_USERNAME;
  protected static String STANDARD_PASSWORD;

  protected static final String TOXIPROXY_WRITER = System.getenv("TOXIPROXY_WRITER");
  protected static final String TOXIPROXY_READER = System.getenv("TOXIPROXY_READER");
  protected static ToxiproxyClient toxiproxyWriter;
  protected static ToxiproxyClient toxiproxyReader;
  protected static final int TOXIPROXY_CONTROL_PORT = 8474;

  protected static final String PROXIED_DOMAIN_NAME_SUFFIX = System.getenv("PROXIED_DOMAIN_NAME_SUFFIX");
  protected static final String PROXY_PORT = System.getenv("PROXY_PORT");
  protected static Proxy proxyWriter;
  protected static Proxy proxyReader;
  protected static final Map<String, Proxy> proxyMap = new HashMap<>();

  protected final ContainerHelper containerHelper = new ContainerHelper();

  protected static String QUERY_FOR_HOSTNAME;
  protected static final int clusterSize = 2;
  protected static String[] instanceIDs;

  protected static void setUp() throws SQLException, IOException, ClassNotFoundException {
    toxiproxyWriter = new ToxiproxyClient(TOXIPROXY_WRITER, TOXIPROXY_CONTROL_PORT);
    toxiproxyReader = new ToxiproxyClient(TOXIPROXY_READER, TOXIPROXY_CONTROL_PORT);

    proxyWriter = getProxy(toxiproxyWriter, STANDARD_WRITER, STANDARD_PORT);
    proxyReader = getProxy(toxiproxyReader, STANDARD_READER, STANDARD_PORT);

    proxyMap.put(STANDARD_WRITER, proxyWriter);
    proxyMap.put(STANDARD_READER, proxyReader);
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
    return DB_CONN_STR_PREFIX + STANDARD_WRITER + ":" + STANDARD_PORT + "," + STANDARD_READER + ":" + STANDARD_PORT
        + "/" + STANDARD_DB;
  }

  protected Connection connect() throws SQLException {
    return DriverManager.getConnection(getUrl(), initDefaultProps());
  }

  protected Connection connect(Properties props) throws SQLException {
    return DriverManager.getConnection(getUrl(), props);
  }

  protected String getProxiedUrl() {
    return DB_CONN_STR_PREFIX + STANDARD_WRITER + PROXIED_DOMAIN_NAME_SUFFIX + ":" + PROXY_PORT + "," + STANDARD_READER
        + PROXIED_DOMAIN_NAME_SUFFIX + ":" + PROXY_PORT + "/"
        + STANDARD_DB;
  }

  protected Connection connectToProxy() throws SQLException {
    return DriverManager.getConnection(getProxiedUrl(), initDefaultProps());
  }

  protected Connection connectToProxy(Properties props) throws SQLException {
    return DriverManager.getConnection(getProxiedUrl(), props);
  }

  protected String queryInstanceId(Connection conn) throws SQLException {
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery(QUERY_FOR_HOSTNAME);
    rs.next();
    return rs.getString(1);
  }

  protected abstract Properties initDefaultProps();

  protected abstract Properties initDefaultPropsNoTimeouts();
}
