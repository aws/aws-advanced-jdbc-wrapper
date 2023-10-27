package software.amazon.jdbc.plugin;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.LaxRedirectStrategy;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.RdsUtilities;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleWithSamlCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleWithSamlRequest;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.plugin.federatedauth.NonValidatingFactory;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.StringUtils;

public class FederatedAuthConnectionPlugin extends AbstractConnectionPlugin {

  public static AwsWrapperProperty IDP_HOST = new AwsWrapperProperty("idpHost", null,
      "The hosting URL of the Identity Provider");
  public static AwsWrapperProperty IDP_PORT =
      new AwsWrapperProperty("idpPort", "443", "The hosting port of Identity Provider");
  public static AwsWrapperProperty RP_ID = new AwsWrapperProperty("rpIdentifier", "urn:amazon:webservices",
      "The relaying party identifier");

  public static AwsWrapperProperty IAM_ROLE_ARN =
      new AwsWrapperProperty("iamRoleArn", null, "The ARN of the IAM Role that is to be assumed.");
  public static AwsWrapperProperty IAM_IDP_ARN =
      new AwsWrapperProperty("iamIdpArn", null, "The ARN of the Identity Provider");

  public static final AwsWrapperProperty IAM_REGION = new AwsWrapperProperty("iamRegion", null,
      "Overrides AWS region that is used to generate the IAM token");
  public static AwsWrapperProperty FEDERATED_USER_NAME
      = new AwsWrapperProperty("federatedUserName", null, "The federated user name");
  public static AwsWrapperProperty FEDERATED_USER_PASSWORD = new AwsWrapperProperty("federatedUserPassword", null,
      "The federated user password");
  public static final AwsWrapperProperty IAM_DEFAULT_PORT = new AwsWrapperProperty(
      "iamDefaultPort", null,
      "Overrides default port that is used to generate the IAM token");
  public static boolean KEY_SSL_INSECURE = true;
  private static final Pattern SAML_PATTERN = Pattern.compile("SAMLResponse\\W+value=\"([^\"]+)\"");
  private static final Pattern IAM_URL_PATTERN =
      Pattern.compile("^(https)://[-a-zA-Z0-9+&@#/%?=~_!:,.']*[-a-zA-Z0-9+&@#/%=~_']");
  private static final Logger LOGGER = Logger.getLogger(IamAuthConnectionPlugin.class.getName());

  protected final PluginService pluginService;


  private static final Set<String> subscribedMethods =
      Collections.unmodifiableSet(new HashSet<String>() {
        {
          add("connect");
          add("forceConnect");
        }
      });

  static {
    PropertyDefinition.registerPluginProperties(IamAuthConnectionPlugin.class);
  }

  @Override
  public Set<String> getSubscribedMethods() {
    return subscribedMethods;
  }

  public FederatedAuthConnectionPlugin(final PluginService pluginService) {
    this.pluginService = pluginService;
    try {
      Class.forName("software.amazon.awssdk.services.sts.model.AssumeRoleWithSamlRequest");
    } catch (final ClassNotFoundException e) {
      // TODO: add message key for this
      throw new RuntimeException("FederatedAuthConnectionPlugin.javaSdkNotInClasspath: please include software.amazon.awssdk:sts:2.x.x as a dependency");
    }
  }


  @Override
  public Connection connect(
      final String driverProtocol,
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {
    return connectInternal(driverProtocol, hostSpec, props, connectFunc);
  }

  private Connection connectInternal(String driverProtocol, HostSpec hostSpec, Properties props,
      JdbcCallable<Connection, SQLException> connectFunc) throws SQLException {

    Supplier<AssumeRoleWithSamlRequest> assumeRoleWithSamlRequestSupplier = () -> {
      String samlAssertion = getSamlAssertion(props);

      AssumeRoleWithSamlRequest request = AssumeRoleWithSamlRequest.builder()
          .samlAssertion(samlAssertion)
          .roleArn(IAM_ROLE_ARN.getString(props))
          .principalArn(IAM_IDP_ARN.getString(props))
          .build();
      return request;
    };

    Region region = Region.of(IAM_REGION.getString(props)); // TODO: error handling incase null or empty

    StsClient stsClient = StsClient.builder()
        .region(region)
        .build();

    StsAssumeRoleWithSamlCredentialsProvider credentialsProvider = StsAssumeRoleWithSamlCredentialsProvider.builder()
        .refreshRequest(assumeRoleWithSamlRequestSupplier)
        .asyncCredentialUpdateEnabled(true)
        .stsClient(stsClient)
        .build();

    String token = generateAuthenticationToken(hostSpec,
        props,
        hostSpec.getHost(),
        getPort(props, hostSpec),
        region,
        credentialsProvider);

    PropertyDefinition.PASSWORD.set(props, token);

    return connectFunc.call();
  }

  private String getSamlAssertion(Properties props) {
    String uri = "https://" + IDP_HOST.getString(props) + ':' + IDP_PORT.getString(props)
        + "/adfs/ls/IdpInitiatedSignOn.aspx?loginToRp=" + RP_ID.getString(props);

    CloseableHttpClient client = null;

    try {
      LOGGER.info(String.format("url: {0}", uri));
      validateURL(uri);
      client = getHttpClient();
      HttpGet get = new HttpGet(uri);
      CloseableHttpResponse resp = client.execute(get);

      if (resp.getStatusLine().getStatusCode() != 200) {
        LOGGER.severe("formBasedAuthentication https response:" + EntityUtils.toString(resp.getEntity()));

        throw new IOException(
            "Failed send request: " + resp.getStatusLine().getReasonPhrase());
      }

      String body = EntityUtils.toString(resp.getEntity());

      LOGGER.info(String.format("body: {0}", body));

      List<NameValuePair> parameters = new ArrayList<NameValuePair>();
      for (String inputTag : getInputTagsfromHTML(body)) {
        String name = getValueByKey(inputTag, "name");
        String value = getValueByKey(inputTag, "value");
        String nameLower = name.toLowerCase();

        LOGGER.info(String.format("name: {0}", name));

        if (nameLower.contains("username")) {
          parameters.add(new BasicNameValuePair(name, FEDERATED_USER_NAME.getString(props)));
        } else if (nameLower.contains("authmethod")) {
          if (!value.isEmpty()) {
            parameters.add(new BasicNameValuePair(name, value));
          }
        } else if (nameLower.contains("password")) {
          parameters.add(new BasicNameValuePair(name, FEDERATED_USER_PASSWORD.getString(props)));
        } else if (!name.isEmpty()) {
          parameters.add(new BasicNameValuePair(name, value));
        }
      }

      String action = getFormAction(body);
      if (!StringUtils.isNullOrEmpty(action) && action.startsWith("/")) {
        uri = "https://" + IDP_HOST.getString(props) + ':' + IDP_PORT.getString(props) + action;
      }

      LOGGER.info(String.format("action uri: {0}", uri));

      validateURL(uri);
      HttpPost post = new HttpPost(uri);
      post.setEntity(new UrlEncodedFormEntity(parameters));
      resp = client.execute(post);
      if (resp.getStatusLine().getStatusCode() != 200) {
        throw new IOException(
            "Failed send request: " + resp.getStatusLine().getReasonPhrase());
      }

      String content = EntityUtils.toString(resp.getEntity());
      Matcher matcher = SAML_PATTERN.matcher(content);
      if (!matcher.find()) {
        throw new IOException("Failed to login ADFS.");
      }

      return matcher.group(1);
    } catch (GeneralSecurityException e) {
      throw new RuntimeException(e);
    } catch (ClientProtocolException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      // TODO: close things like clients
      try {
        client.close();
      } catch (IOException e) {
        LOGGER.warning("FederatedAuthConnectionPlugin: CloseableHttpClient may not have not closed successfully");
      }
    }
  }

  private CloseableHttpClient getHttpClient() throws GeneralSecurityException {
    RequestConfig rc = RequestConfig.custom()
        .setSocketTimeout(60000)
        .setConnectTimeout(60000)
        .setExpectContinueEnabled(false)
        .setCookieSpec(CookieSpecs.STANDARD)
        .build();

    HttpClientBuilder builder = HttpClients.custom()
        .setDefaultRequestConfig(rc)
        .setRedirectStrategy(new LaxRedirectStrategy())
        .useSystemProperties(); // this is needed for proxy setting using system properties.

    if (KEY_SSL_INSECURE) {
      SSLContext ctx = SSLContext.getInstance("TLSv1.2");
      TrustManager[] tma = new TrustManager[] {new NonValidatingFactory.NonValidatingTM()};
      ctx.init(null, tma, null);
      SSLSocketFactory factory = ctx.getSocketFactory();

      SSLConnectionSocketFactory sf = new SSLConnectionSocketFactory(
          factory,
          new NoopHostnameVerifier());

      builder.setSSLSocketFactory(sf);
    }

    return builder.build();
  }

  protected List<String> getInputTagsfromHTML(String body) {
    Set<String> distinctInputTags = new HashSet<>();
    List<String> inputTags = new ArrayList<String>();
    Pattern inputTagPattern = Pattern.compile("<input(.+?)/>", Pattern.DOTALL);
    Matcher inputTagMatcher = inputTagPattern.matcher(body);
    while (inputTagMatcher.find()) {
      String tag = inputTagMatcher.group(0);
      String tagNameLower = getValueByKey(tag, "name").toLowerCase();
      if (!tagNameLower.isEmpty() && distinctInputTags.add(tagNameLower)) {
        inputTags.add(tag);
      }
    }
    return inputTags;
  }

  protected String getValueByKey(String input, String key) {
    Pattern keyValuePattern = Pattern.compile("(" + Pattern.quote(key) + ")\\s*=\\s*\"(.*?)\"");
    Matcher keyValueMatcher = keyValuePattern.matcher(input);
    if (keyValueMatcher.find()) {
      return escapeHtmlEntity(keyValueMatcher.group(2));
    }
    return "";
  }

  protected String escapeHtmlEntity(String html) {
    StringBuilder sb = new StringBuilder(html.length());
    int i = 0;
    int length = html.length();
    while (i < length) {
      char c = html.charAt(i);
      if (c != '&') {
        sb.append(c);
        i++;
        continue;
      }

      if (html.startsWith("&amp;", i)) {
        sb.append('&');
        i += 5;
      } else if (html.startsWith("&apos;", i)) {
        sb.append('\'');
        i += 6;
      } else if (html.startsWith("&quot;", i)) {
        sb.append('"');
        i += 6;
      } else if (html.startsWith("&lt;", i)) {
        sb.append('<');
        i += 4;
      } else if (html.startsWith("&gt;", i)) {
        sb.append('>');
        i += 4;
      } else {
        sb.append(c);
        ++i;
      }
    }
    return sb.toString();
  }

  protected String getFormAction(String body) {
    Pattern pattern = Pattern.compile("<form.*?action=\"([^\"]+)\"");
    Matcher m = pattern.matcher(body);
    if (m.find()) {
      return escapeHtmlEntity(m.group(1));
    }
    return null;
  }

  protected void validateURL(String paramString) throws IOException {

    URI authorizeRequestUrl = URI.create(paramString);
    String error = "Invalid url:" + paramString;

    LOGGER.info(String.format("URI: \n%s", authorizeRequestUrl.toString()));
    try {
      if (!authorizeRequestUrl.toURL().getProtocol().equalsIgnoreCase("https")) {
        LOGGER.severe(error);

        throw new IOException(error);
      }

      Matcher matcher = IAM_URL_PATTERN.matcher(paramString);
      if (!matcher.find()) {
        LOGGER.severe("Pattern matching failed:" + error);

        throw new IOException("Pattern matching failed:" + error);
      }
    } catch (MalformedURLException e) {
      throw new IOException(error + " " + e.getMessage(), e);
    }
  }

  String generateAuthenticationToken(final HostSpec originalHostSpec, final Properties props, final String hostname,
      final int port, final Region region, final AwsCredentialsProvider awsCredentialsProvider) {
    final String user = PropertyDefinition.USER.getString(props);
    final RdsUtilities utilities =
        RdsUtilities.builder().credentialsProvider(awsCredentialsProvider).region(region).build();
    return utilities.generateAuthenticationToken((builder) -> builder.hostname(hostname).port(port).username(user));
  }

  private int getPort(Properties props, HostSpec hostSpec) {
    if (!StringUtils.isNullOrEmpty(IAM_DEFAULT_PORT.getString(props))) {
      int defaultPort = IAM_DEFAULT_PORT.getInteger(props);
      if (defaultPort > 0) {
        return defaultPort;
      } else {
        LOGGER.finest(() -> Messages.get("IamAuthConnectionPlugin.invalidPort", new Object[] {defaultPort}));
      }
    }

    if (hostSpec.isPortSpecified()) {
      return hostSpec.getPort();
    } else {
      return this.pluginService.getDialect().getDefaultPort();
    }
  }

  @Override
  public Connection forceConnect(
      final @NonNull String driverProtocol,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props,
      final boolean isInitialConnection,
      final @NonNull JdbcCallable<Connection, SQLException> forceConnectFunc)
      throws SQLException {
    return connectInternal(driverProtocol, hostSpec, props, forceConnectFunc);
  }
}
