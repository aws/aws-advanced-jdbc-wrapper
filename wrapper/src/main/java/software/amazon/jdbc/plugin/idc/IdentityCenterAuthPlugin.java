package software.amazon.jdbc.plugin.idc;

import com.sun.net.httpserver.HttpServer;
import java.awt.Desktop;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.client.utils.URLEncodedUtils;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sso.SsoClient;
import software.amazon.awssdk.services.sso.model.GetRoleCredentialsRequest;
import software.amazon.awssdk.services.sso.model.GetRoleCredentialsResponse;
import software.amazon.awssdk.services.sso.model.RoleCredentials;
import software.amazon.awssdk.services.ssooidc.SsoOidcClient;
import software.amazon.awssdk.services.ssooidc.model.CreateTokenRequest;
import software.amazon.awssdk.services.ssooidc.model.CreateTokenResponse;
import software.amazon.awssdk.services.ssooidc.model.RegisterClientRequest;
import software.amazon.awssdk.services.ssooidc.model.RegisterClientResponse;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.AbstractConnectionPlugin;
import software.amazon.jdbc.plugin.TokenInfo;
import software.amazon.jdbc.plugin.iam.IamTokenUtility;
import software.amazon.jdbc.util.FullServicesContainer;
import software.amazon.jdbc.util.GDBRegionUtils;
import software.amazon.jdbc.util.IamAuthUtils;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.RdsUrlType;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.RegionUtils;
import software.amazon.jdbc.util.StateSnapshotProvider;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.telemetry.TelemetryCounter;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryGauge;

public class IdentityCenterAuthPlugin extends AbstractConnectionPlugin implements StateSnapshotProvider {

  private static final Logger LOGGER = Logger.getLogger(IdentityCenterAuthPlugin.class.getName());
  private static final Set<String> subscribedMethods =
      Collections.unmodifiableSet(new HashSet<String>() {
        {
          add(JdbcMethod.CONNECT.methodName);
          add(JdbcMethod.FORCECONNECT.methodName);
        }
      });

  private static final int DEFAULT_TOKEN_EXPIRATION_SEC = 15 * 60 - 30;
  private static final long DISPOSAL_TIME_NANO = TimeUnit.MINUTES.toNanos(15);

  // PKCE / Authorization Code flow constants
  private static final String AUTH_CODE_GRANT_TYPE = "authorization_code";
  private static final String CLIENT_TYPE = "public";
  private static final String REDIRECT_URI_BASE = "http://127.0.0.1";
  private static final String CHALLENGE_METHOD = "S256";
  private static final int CODE_VERIFIER_BYTE_LENGTH = 60;

  // OIDC endpoint building
  private static final String OIDC_SUBDOMAIN = "oidc";
  private static final String AMAZON_COM_DOMAIN = "amazonaws.com";
  private static final String AMAZON_CHINA_DOMAIN = "amazonaws.com.cn";
  private static final String AUTHORIZE_ENDPOINT = "/authorize";

  // Defaults
  private static final int DEFAULT_LISTEN_PORT = 7890;
  private static final int DEFAULT_IDC_RESPONSE_TIMEOUT_SEC = 120;
  private static final String DEFAULT_CLIENT_DISPLAY_NAME = "aws-advanced-jdbc-wrapper";

  private static final String SUCCESS_RESPONSE =
      "<html><body><h2>Authorization successful.</h2>"
          + "<p>You may close this window.</p></body></html>";
  private static final String ERROR_RESPONSE =
      "<html><body><h2>Authorization failed.</h2>"
          + "<p>Missing authorization code. Please try again.</p></body></html>";

  // Connection properties
  public static final AwsWrapperProperty IDC_REGION = new AwsWrapperProperty(
      "idcRegion", null,
      "The AWS region where the IAM Identity Center instance is hosted.");

  public static final AwsWrapperProperty IDC_START_URL = new AwsWrapperProperty(
      "idcStartUrl", null,
      "The issuer URL for the IAM Identity Center instance.");

  public static final AwsWrapperProperty IDC_ACCOUNT_ID = new AwsWrapperProperty(
      "idcAccountId", null,
      "The AWS account ID to assume a role in via IAM Identity Center.");

  public static final AwsWrapperProperty IDC_ROLE_NAME = new AwsWrapperProperty(
      "idcRoleName", null,
      "The IAM Identity Center permission set role name to assume.");

  public static final AwsWrapperProperty IDC_LISTEN_PORT = new AwsWrapperProperty(
      "idcListenPort", String.valueOf(DEFAULT_LISTEN_PORT),
      "The local port to listen on for the authorization code callback.");

  public static final AwsWrapperProperty IDC_RESPONSE_TIMEOUT = new AwsWrapperProperty(
      "idcResponseTimeout", String.valueOf(DEFAULT_IDC_RESPONSE_TIMEOUT_SEC),
      "Timeout in seconds to wait for the user to complete browser authorization.");

  public static final AwsWrapperProperty IDC_CLIENT_DISPLAY_NAME = new AwsWrapperProperty(
      "idcClientDisplayName", DEFAULT_CLIENT_DISPLAY_NAME,
      "The display name for the OIDC client registration.");

  public static final AwsWrapperProperty IAM_HOST = new AwsWrapperProperty(
      "iamHost", null,
      "Overrides the host that is used to generate the IAM token.");

  public static final AwsWrapperProperty IAM_DEFAULT_PORT = new AwsWrapperProperty(
      "iamDefaultPort", "-1",
      "Overrides default port that is used to generate the IAM token.");

  public static final AwsWrapperProperty IAM_TOKEN_EXPIRATION = new AwsWrapperProperty(
      "iamTokenExpiration", String.valueOf(DEFAULT_TOKEN_EXPIRATION_SEC),
      "IAM token cache expiration in seconds.");

  // Cache for RegisterClientResponse keyed by issuerUrl:region:port
  private static final Map<String, RegisterClientResponse> registerClientCache =
      new HashMap<String, RegisterClientResponse>();

  protected final FullServicesContainer servicesContainer;
  protected final PluginService pluginService;
  protected final RdsUtils rdsUtils = new RdsUtils();
  protected RegionUtils regionUtils;

  static {
    PropertyDefinition.registerPluginProperties(IdentityCenterAuthPlugin.class);
  }

  private final TelemetryFactory telemetryFactory;
  private final TelemetryGauge cacheSizeGauge;
  private final TelemetryCounter fetchTokenCounter;
  private final IamTokenUtility iamTokenUtility;

  public IdentityCenterAuthPlugin(final @NonNull FullServicesContainer servicesContainer) {
    this(servicesContainer, IamAuthUtils.getTokenUtility());
  }

  IdentityCenterAuthPlugin(final @NonNull FullServicesContainer servicesContainer, IamTokenUtility utility) {
    this.iamTokenUtility = utility;
    this.servicesContainer = servicesContainer;
    this.pluginService = servicesContainer.getPluginService();
    this.telemetryFactory = servicesContainer.getTelemetryFactory();

    this.servicesContainer.getStorageService().registerItemClassIfAbsent(
        TokenInfo.class, false, DISPOSAL_TIME_NANO, null, null);

    this.cacheSizeGauge = this.telemetryFactory.createGauge("idc.tokenCache.size",
        () -> (long) this.servicesContainer.getStorageService().size(TokenInfo.class));
    this.fetchTokenCounter = this.telemetryFactory.createCounter("idc.fetchToken.count");
  }

  @Override
  public Set<String> getSubscribedMethods() {
    return subscribedMethods;
  }

  @Override
  public Connection connect(String driverProtocol, HostSpec hostSpec, Properties props,
      boolean isInitialConnection, JdbcCallable<Connection, SQLException> connectFunc) throws SQLException {
    return connectInternal(driverProtocol, hostSpec, props, connectFunc);
  }

  @Override
  public Connection forceConnect(String driverProtocol, HostSpec hostSpec, Properties props,
      boolean isInitialConnection, JdbcCallable<Connection, SQLException> forceConnectFunc) throws SQLException {
    return connectInternal(driverProtocol, hostSpec, props, forceConnectFunc);
  }

  private String getAndVerify(AwsWrapperProperty propertyToVerify, Properties props) throws SQLException {
    final String prop = propertyToVerify.getString(props);
    if (StringUtils.isNullOrEmpty(prop)) {
      throw new SQLException("IdentityCenterAuthPlugin requires " + propertyToVerify.name + " to be set.");
    }
    return prop;
  }

  private Connection connectInternal(String driverProtocol, HostSpec hostSpec, Properties props,
      JdbcCallable<Connection, SQLException> connectFunc) throws SQLException {

    final String idcRegion = getAndVerify(IDC_REGION, props);
    final String idcStartUrl = getAndVerify(IDC_START_URL, props);
    final String idcAccountId = getAndVerify(IDC_ACCOUNT_ID, props);
    final String idcRoleName = getAndVerify(IDC_ROLE_NAME, props);

    if (StringUtils.isNullOrEmpty(idcRegion)) {
      throw new SQLException("IdentityCenterAuthPlugin requires 'idcRegion' to be set.");
    }
    if (StringUtils.isNullOrEmpty(idcStartUrl)) {
      throw new SQLException("IdentityCenterAuthPlugin requires 'idcStartUrl' to be set.");
    }
    if (StringUtils.isNullOrEmpty(idcAccountId)) {
      throw new SQLException("IdentityCenterAuthPlugin requires 'idcAccountId' to be set.");
    }
    if (StringUtils.isNullOrEmpty(idcRoleName)) {
      throw new SQLException("IdentityCenterAuthPlugin requires 'idcRoleName' to be set.");
    }
    if (StringUtils.isNullOrEmpty(PropertyDefinition.USER.getString(props))) {
      throw new SQLException("IdentityCenterAuthPlugin requires 'user' (database user) to be set.");
    }

    final int listenPort = IDC_LISTEN_PORT.getInteger(props);
    final int responseTimeoutSec = IDC_RESPONSE_TIMEOUT.getInteger(props);
    final String clientDisplayName = IDC_CLIENT_DISPLAY_NAME.getString(props);

    final HostSpec host = IamAuthUtils.getIamHost(IAM_HOST.getString(props), hostSpec);
    final int port = IamAuthUtils.getIamPort(
        IAM_DEFAULT_PORT.getInteger(props), hostSpec,
        this.pluginService.getDialect().getDefaultPort());

    final RdsUrlType type = rdsUtils.identifyRdsType(host.getHost());
    this.regionUtils = type == RdsUrlType.RDS_GLOBAL_WRITER_CLUSTER ? new GDBRegionUtils() : new RegionUtils();
    final Region rdsRegion = this.regionUtils.getRegion(host, props, IDC_REGION.name);
    if (rdsRegion == null) {
      throw new SQLException(
          Messages.get("IamAuthConnectionPlugin.unableToDetermineRegion", new Object[] {IDC_REGION.name}));
    }

    final int tokenExpirationSec = IAM_TOKEN_EXPIRATION.getInteger(props);
    final String cacheKey = IamAuthUtils.getCacheKey(
        PropertyDefinition.USER.getString(props), host.getHost(), port, rdsRegion);

    final TokenInfo tokenInfo = this.servicesContainer.getStorageService().get(TokenInfo.class, cacheKey);
    final boolean isCachedToken = tokenInfo != null && !tokenInfo.isExpired();

    if (isCachedToken && tokenExpirationSec > 0) {
      LOGGER.finest(() -> Messages.get(
          "AuthenticationToken.useCachedToken", new Object[] {tokenInfo.getToken()}));
      PropertyDefinition.PASSWORD.set(props, tokenInfo.getToken());
    } else {
      updateAuthenticationToken(props, host.getHost(), port, rdsRegion, idcRegion,
          idcStartUrl, idcAccountId, idcRoleName, listenPort, responseTimeoutSec,
          clientDisplayName, cacheKey, tokenExpirationSec);
    }

    try {
      return connectFunc.call();
    } catch (final SQLException exception) {
      LOGGER.finest(() -> Messages.get(
          "IamAuthConnectionPlugin.connectException", new Object[] {exception}));

      if (!this.pluginService.isLoginException(exception, this.pluginService.getTargetDriverDialect())
          || !isCachedToken) {
        throw exception;
      }

      updateAuthenticationToken(props, host.getHost(), port, rdsRegion, idcRegion,
          idcStartUrl, idcAccountId, idcRoleName, listenPort, responseTimeoutSec,
          clientDisplayName, cacheKey, tokenExpirationSec);
      return connectFunc.call();

    } catch (final Exception exception) {
      LOGGER.warning(() -> Messages.get(
          "IamAuthConnectionPlugin.unhandledException", new Object[] {exception}));
      throw new SQLException(exception);
    }
  }

  private void updateAuthenticationToken(
      final Properties props, final String hostname, final int port,
      final Region rdsRegion, final String idcRegion, final String idcStartUrl,
      final String idcAccountId, final String idcRoleName,
      final int listenPort, final int responseTimeoutSec, final String clientDisplayName,
      final String cacheKey, final int tokenExpirationSec) throws SQLException {

    final Instant tokenExpiry = Instant.now().plus(tokenExpirationSec, ChronoUnit.SECONDS);

    if (this.fetchTokenCounter != null) {
      this.fetchTokenCounter.inc();
    }

    final AwsCredentialsProvider credentialsProvider = getIdcCredentialsProvider(
        idcRegion, idcStartUrl, idcAccountId, idcRoleName,
        listenPort, responseTimeoutSec, clientDisplayName);

    final String token = IamAuthUtils.generateAuthenticationToken(
        this.iamTokenUtility, this.pluginService,
        PropertyDefinition.USER.getString(props),
        hostname, port, rdsRegion, credentialsProvider);

    LOGGER.finest(() -> Messages.get(
        "AuthenticationToken.generatedNewToken", new Object[] {token}));

    PropertyDefinition.PASSWORD.set(props, token);
    this.servicesContainer.getStorageService().set(cacheKey, new TokenInfo(token, tokenExpiry));
  }

  private AwsCredentialsProvider getIdcCredentialsProvider(
      final String idcRegion, final String idcStartUrl,
      final String idcAccountId, final String idcRoleName,
      final int listenPort, final int responseTimeoutSec,
      final String clientDisplayName) throws SQLException {

    final Region region = Region.of(idcRegion);
    final String redirectUri = REDIRECT_URI_BASE + ":" + listenPort;

    try (SsoOidcClient oidcClient = SsoOidcClient.builder().region(region).build()) {

      // 1. Register client (cached)
      final RegisterClientResponse registerResponse =
          getOrRegisterClient(oidcClient, clientDisplayName, idcStartUrl, redirectUri, idcRegion, listenPort);

      // 2. Generate PKCE code verifier and challenge
      final String codeVerifier = generateCodeVerifier();
      final String codeChallenge = generateCodeChallenge(codeVerifier);

      // 3. Generate CSRF state
      final String state = generateState();

      // 4. Start local server and open browser
      final String authCode = fetchAuthorizationCode(
          idcRegion, registerResponse.clientId(), redirectUri,
          codeChallenge, state, listenPort, responseTimeoutSec);

      // 5. Exchange authorization code for access token
      final CreateTokenResponse tokenResponse = oidcClient.createToken(
          CreateTokenRequest.builder()
              .clientId(registerResponse.clientId())
              .clientSecret(registerResponse.clientSecret())
              .code(authCode)
              .grantType(AUTH_CODE_GRANT_TYPE)
              .codeVerifier(codeVerifier)
              .redirectUri(redirectUri)
              .build());

      LOGGER.fine("Successfully obtained SSO access token from IAM Identity Center.");

      if (tokenResponse.accessToken() == null) {
        throw new SQLException("IdC authentication failed: access token was null.");
      }

      // 6. Exchange SSO access token for temporary IAM role credentials
      try (SsoClient ssoClient = SsoClient.builder().region(region).build()) {
        final GetRoleCredentialsResponse roleCredentialsResponse = ssoClient.getRoleCredentials(
            GetRoleCredentialsRequest.builder()
                .accessToken(tokenResponse.accessToken())
                .accountId(idcAccountId)
                .roleName(idcRoleName)
                .build());

        final RoleCredentials roleCreds = roleCredentialsResponse.roleCredentials();

        LOGGER.fine("Obtained temporary IAM credentials via IAM Identity Center for account "
            + idcAccountId + ", role " + idcRoleName + ".");

        return StaticCredentialsProvider.create(
            AwsSessionCredentials.create(
                roleCreds.accessKeyId(),
                roleCreds.secretAccessKey(),
                roleCreds.sessionToken()));
      }
    } catch (final SQLException e) {
      throw e;
    } catch (final Exception e) {
      throw new SQLException(
          "Failed to obtain credentials via IAM Identity Center: " + e.getMessage(), e);
    }
  }

  /**
   * Returns a cached RegisterClientResponse or registers a new client.
   */
  private RegisterClientResponse getOrRegisterClient(
      final SsoOidcClient oidcClient, final String clientDisplayName,
      final String issuerUrl, final String redirectUri,
      final String idcRegion, final int listenPort) {

    final String cacheKey = issuerUrl + ":" + idcRegion + ":" + listenPort;

    synchronized (registerClientCache) {
      final RegisterClientResponse cached = registerClientCache.get(cacheKey);
      if (cached != null
          && cached.clientSecretExpiresAt() != null
          && System.currentTimeMillis() < cached.clientSecretExpiresAt() * 1000L) {
        LOGGER.fine("Using cached RegisterClient response.");
        return cached;
      }
    }

    final RegisterClientResponse response = oidcClient.registerClient(
        RegisterClientRequest.builder()
            .clientName(clientDisplayName)
            .clientType(CLIENT_TYPE)
            .scopes("sso:account:access")
            .issuerUrl(issuerUrl)
            .redirectUris(redirectUri)
            .grantTypes(AUTH_CODE_GRANT_TYPE)
            .build());

    LOGGER.fine("Registered OIDC client with IAM Identity Center.");

    synchronized (registerClientCache) {
      registerClientCache.put(cacheKey, response);
    }

    return response;
  }

  /**
   * Opens the user's browser to the IdC /authorize endpoint and waits for the
   * authorization code callback on a local HTTP server.
   */
  private String fetchAuthorizationCode(
      final String idcRegion, final String clientId, final String redirectUri,
      final String codeChallenge, final String state,
      final int listenPort, final int responseTimeoutSec) throws SQLException {

    final CompletableFuture<String> authCodeFuture = new CompletableFuture<>();
    HttpServer server = null;

    try {
      server = HttpServer.create(new InetSocketAddress("127.0.0.1", listenPort), 0);

      server.createContext("/", exchange -> {
        try {
          final String query = exchange.getRequestURI().getQuery();
          final Map<String, String> params = parseQueryParams(query);

          final String incomingState = params.get("state");
          if (!state.equals(incomingState)) {
            sendResponse(exchange, 400, ERROR_RESPONSE);
            authCodeFuture.completeExceptionally(
                new SQLException("CSRF state mismatch: expected " + state + ", got " + incomingState));
            return;
          }

          final String code = params.get("code");
          if (StringUtils.isNullOrEmpty(code)) {
            sendResponse(exchange, 400, ERROR_RESPONSE);
            authCodeFuture.completeExceptionally(
                new SQLException("No authorization code received in callback."));
            return;
          }

          sendResponse(exchange, 200, SUCCESS_RESPONSE);
          authCodeFuture.complete(code);
        } catch (final Exception e) {
          authCodeFuture.completeExceptionally(e);
        }
      });

      server.start();
      LOGGER.fine("Listening for authorization callback on port " + listenPort);

      // Build the authorize URL and open the browser
      openBrowser(idcRegion, clientId, redirectUri, codeChallenge, state);

      // Wait for the user to complete authorization
      return authCodeFuture.get(responseTimeoutSec, TimeUnit.SECONDS);

    } catch (final TimeoutException e) {
      throw new SQLException(
          "IdC authentication failed: timed out waiting for browser authorization.", e);
    } catch (final SQLException e) {
      throw e;
    } catch (final Exception e) {
      final Throwable cause = e.getCause();
      if (cause instanceof SQLException) {
        throw (SQLException) cause;
      }
      throw new SQLException(
          "IdC authentication failed: error during browser authorization flow: " + e.getMessage(), e);
    } finally {
      if (server != null) {
        server.stop(0);
      }
    }
  }

  /**
   * Opens the default browser to the IdC /authorize endpoint with PKCE parameters.
   */
  private void openBrowser(
      final String idcRegion, final String clientId, final String redirectUri,
      final String codeChallenge, final String state) throws SQLException {

    try {
      final String oidcHost = buildOidcHost(idcRegion);

      final URI authorizeUri = new URIBuilder()
          .setScheme("https")
          .setHost(oidcHost)
          .setPath(AUTHORIZE_ENDPOINT)
          .addParameter("response_type", "code")
          .addParameter("client_id", clientId)
          .addParameter("redirect_uri", redirectUri)
          .addParameter("scopes", "sso:account:access")
          .addParameter("state", state)
          .addParameter("code_challenge", codeChallenge)
          .addParameter("code_challenge_method", CHALLENGE_METHOD)
          .build();

      LOGGER.fine("Authorization URL: " + authorizeUri);

      if (Desktop.isDesktopSupported() && Desktop.getDesktop().isSupported(Desktop.Action.BROWSE)) {
        Desktop.getDesktop().browse(authorizeUri);
      } else {
        LOGGER.warning("Desktop browsing not supported. Please open this URL manually: " + authorizeUri);
      }
    } catch (final URISyntaxException | IOException e) {
      throw new SQLException("Failed to open browser for IdC authorization: " + e.getMessage(), e);
    }
  }

  // --- Utility methods ---

  /**
   * Builds the OIDC host URL for the given region (e.g. oidc.us-east-1.amazonaws.com).
   */
  private static String buildOidcHost(final String idcRegion) {
    final String normalized = idcRegion.trim().toLowerCase();
    final String domain = normalized.startsWith("cn-") ? AMAZON_CHINA_DOMAIN : AMAZON_COM_DOMAIN;
    return OIDC_SUBDOMAIN + "." + normalized + "." + domain;
  }

  /**
   * Generates a cryptographically random code verifier for PKCE (base64url-encoded, 60 bytes).
   */
  private static String generateCodeVerifier() {
    final byte[] randomBytes = new byte[CODE_VERIFIER_BYTE_LENGTH];
    new SecureRandom().nextBytes(randomBytes);
    return Base64.getUrlEncoder().withoutPadding().encodeToString(randomBytes);
  }

  /**
   * Generates the code challenge by SHA-256 hashing the code verifier (base64url-encoded).
   */
  private static String generateCodeChallenge(final String codeVerifier) throws SQLException {
    try {
      final byte[] hash = MessageDigest.getInstance("SHA-256")
          .digest(codeVerifier.getBytes(StandardCharsets.US_ASCII));
      return Base64.getUrlEncoder().withoutPadding().encodeToString(hash);
    } catch (final NoSuchAlgorithmException e) {
      throw new SQLException("SHA-256 not available for PKCE code challenge generation.", e);
    }
  }

  /**
   * Generates a random state string for CSRF protection.
   */
  private static String generateState() {
    final byte[] stateBytes = new byte[32];
    new SecureRandom().nextBytes(stateBytes);
    return Base64.getUrlEncoder().withoutPadding().encodeToString(stateBytes);
  }

  /**
   * Parses query parameters from a URI query string.
   */
  private static Map<String, String> parseQueryParams(final String query) {
    final Map<String, String> params = new HashMap<String, String>();
    if (query == null || query.isEmpty()) {
      return params;
    }
    final List<NameValuePair> pairs =
        URLEncodedUtils.parse(query, StandardCharsets.UTF_8);
    for (final NameValuePair pair : pairs) {
      params.put(pair.getName(), pair.getValue());
    }
    return params;
  }

  /**
   * Sends an HTTP response with the given status code and body.
   */
  private static void sendResponse(
      final com.sun.net.httpserver.HttpExchange exchange,
      final int statusCode, final String body) throws IOException {
    final byte[] responseBytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", "text/html; charset=UTF-8");
    exchange.sendResponseHeaders(statusCode, responseBytes.length);
    try (OutputStream os = exchange.getResponseBody()) {
      os.write(responseBytes);
    }
  }
}
