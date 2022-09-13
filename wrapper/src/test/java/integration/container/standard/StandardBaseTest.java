package integration.container.standard;

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
import org.junit.jupiter.api.BeforeEach;

public abstract class StandardBaseTest {
  protected static String DB_CONN_STR_PREFIX;
  protected static String STANDARD_HOST;
  protected static Integer STANDARD_PORT;
  protected static String STANDARD_DB;
  protected static String STANDARD_USERNAME;
  protected static String STANDARD_PASSWORD;

  protected static final String TOXIPROXY_HOST = System.getenv("TOXIPROXY_HOST");
  protected static ToxiproxyClient toxiproxyClient;
  protected static final int TOXIPROXY_CONTROL_PORT = 8474;

  protected static final String PROXIED_DOMAIN_NAME_SUFFIX = System.getenv("PROXIED_DOMAIN_NAME_SUFFIX");
  protected static final String PROXY_PORT = System.getenv("PROXY_PORT");
  protected static Proxy proxy;
  protected static final Map<String, Proxy> proxyMap = new HashMap<>();

  protected final ContainerHelper containerHelper = new ContainerHelper();

  protected static void setUp() throws SQLException, IOException, ClassNotFoundException {
    toxiproxyClient = new ToxiproxyClient(TOXIPROXY_HOST, TOXIPROXY_CONTROL_PORT);
    proxy = getProxy(toxiproxyClient, STANDARD_HOST, STANDARD_PORT);
    proxyMap.put(STANDARD_HOST, proxy);
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
        DB_CONN_STR_PREFIX + STANDARD_HOST + ":" + STANDARD_PORT + "/" + STANDARD_DB;
    return url;
  }

  protected Connection connect() throws SQLException {
    return DriverManager.getConnection(getUrl(), initDefaultProps());
  }

  protected String getProxiedUrl() {
    String url = DB_CONN_STR_PREFIX + STANDARD_HOST + PROXIED_DOMAIN_NAME_SUFFIX + ":" + PROXY_PORT + "/"
        + STANDARD_DB;
    return url;
  }

  protected Connection connectToProxy() throws SQLException {
    return DriverManager.getConnection(getProxiedUrl(), initDefaultProps());
  }

  protected abstract Properties initDefaultProps();

  protected abstract Properties initDefaultPropsNoTimeouts();
}
