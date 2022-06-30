/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package integration.container.standard.postgres;

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
import software.aws.rds.jdbc.proxydriver.Driver;

public class StandardPostgresBaseTest {
  protected static final String DB_CONN_STR_PREFIX = "aws-proxy-jdbc:postgresql://";
  protected static final String TEST_HOST = System.getenv("TEST_HOST");
  protected static final String TEST_PORT = "5432";
//  protected static final String TEST_PORT = System.getenv("TEST_PORT");
  protected static final String TEST_DB = System.getenv("TEST_DB");
  protected static final String TEST_USERNAME = "test";
//  protected static final String TEST_USERNAME = System.getenv("TEST_USERNAME");
  protected static final String TEST_PASSWORD = "root";
//  protected static final String TEST_PASSWORD = System.getenv("TEST_PASSWORD");

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
    proxy = getProxy(toxiproxyClient, TEST_HOST, Integer.parseInt(TEST_PORT));
    proxyMap.put(TEST_HOST, proxy);

    DriverManager.registerDriver(new Driver());
  }

  @BeforeEach
  public void setUpEach() {
    proxyMap.forEach((instance, proxy) -> containerHelper.enableConnectivity(proxy));
  }

  protected static Proxy getProxy(ToxiproxyClient proxyClient, String host, int port) throws IOException {
    final String upstream = host + ":" + port;
    return proxyClient.getProxy(upstream);
  }

  protected Connection connect() throws SQLException {
    String url = DB_CONN_STR_PREFIX + TEST_HOST + ":" + TEST_PORT + "/" + TEST_DB;
    return DriverManager.getConnection(url, initDefaultProps());
  }

  protected Connection connectToProxy() throws SQLException {
    String url = DB_CONN_STR_PREFIX + TEST_HOST + PROXIED_DOMAIN_NAME_SUFFIX + ":" + PROXY_PORT + "/" + TEST_DB;
    return DriverManager.getConnection(url, initDefaultProps());
  }

  protected Properties initDefaultProps() {
    final Properties props = initDefaultPropsNoTimeouts();
    props.setProperty(PGProperty.CONNECT_TIMEOUT.getName(), "3");
    props.setProperty(PGProperty.SOCKET_TIMEOUT.getName(), "3");

    return props;
  }

  protected Properties initDefaultPropsNoTimeouts() {
    final Properties props = new Properties();
    props.setProperty(PGProperty.USER.getName(), TEST_USERNAME);
    props.setProperty(PGProperty.PASSWORD.getName(), TEST_PASSWORD);
    props.setProperty(PGProperty.TCP_KEEP_ALIVE.getName(), Boolean.FALSE.toString());

    return props;
  }
}