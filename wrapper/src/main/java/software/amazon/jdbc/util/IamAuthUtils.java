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

package software.amazon.jdbc.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.plugin.iam.IamTokenUtility;
import software.amazon.jdbc.plugin.iam.LightRdsUtility;
import software.amazon.jdbc.plugin.iam.RegularRdsUtility;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryTraceLevel;

public class IamAuthUtils {

  private static final Logger LOGGER = Logger.getLogger(IamAuthUtils.class.getName());
  private static final String TELEMETRY_FETCH_TOKEN = "fetch authentication token";

  public static HostSpec getIamHost(final @Nullable String iamHost, final HostSpec hostSpec) {
    if (!StringUtils.isNullOrEmpty(iamHost)) {
      return new HostSpecBuilder(hostSpec.getHostAvailabilityStrategy()).copyFrom(hostSpec).host(iamHost).build();
    }
    return hostSpec;
  }

  public static int getIamPort(final int iamDefaultPort, final HostSpec hostSpec, final int dialectDefaultPort) {
    if (iamDefaultPort > 0) {
      return iamDefaultPort;
    } else if (hostSpec.isPortSpecified()) {
      return hostSpec.getPort();
    } else {
      return dialectDefaultPort;
    }
  }

  /**
   * Builds a token cache key that is not scoped to a signing identity.
   *
   * @deprecated tokens cached under this key can be served to a connection that signs with different
   *     credentials. Use {@link #getCacheKey(String, String[], String, String, int, Region)} instead.
   */
  @Deprecated
  public static String getCacheKey(
      final @Nullable String user,
      final String hostname,
      final int port,
      final Region region) {

    return String.format("%s:%s:%d:%s", region, hostname, port, user);
  }

  /**
   * Builds an authentication token cache key that is scoped to the identity used to sign the token.
   *
   * <p>A token is only valid for the AWS identity that signed it. Two connections that target the same
   * region, host, port and database user but sign with different identities (for example, different
   * AWS credentials, or different federated users or roles) must not share a cached token.
   *
   * @param scope    the plugin that owns the token, so different plugins never share an entry.
   * @param identity the components that identify the signing identity, as configured for the connection.
   * @param user     the database user.
   * @param hostname the host the token is generated for.
   * @param port     the port the token is generated for.
   * @param region   the region the token is generated for.
   * @return the cache key.
   */
  public static String getCacheKey(
      final String scope,
      final @Nullable String[] identity,
      final @Nullable String user,
      final String hostname,
      final int port,
      final Region region) {
    final StringBuilder sb = new StringBuilder()
        .append(escapeCacheKeyComponent(scope))
        .append(':').append(region)
        .append(':').append(escapeCacheKeyComponent(hostname))
        .append(':').append(port)
        .append(':').append(escapeCacheKeyComponent(user));
    for (final @Nullable String component : identity) {
      sb.append(':').append(escapeCacheKeyComponent(component));
    }
    return sb.toString();
  }

  /**
   * Returns a value that identifies the given AWS credentials, for use as a cache key component. It is
   * derived from the access key ID, so it changes whenever different credentials are used (for example,
   * after the profile in the credentials file is edited, or when temporary credentials are refreshed).
   * The access key ID is hashed so the raw ID is not kept in the cache key.
   *
   * @param credentials the credentials that sign the token.
   * @return a SHA-256 hash of the access key ID, Base64-encoded.
   */
  public static String getCredentialsIdentity(final AwsCredentials credentials) {
    try {
      final MessageDigest digest = MessageDigest.getInstance("SHA-256");
      final byte[] hash = digest.digest(credentials.accessKeyId().getBytes(StandardCharsets.UTF_8));
      return Base64.getEncoder().encodeToString(hash);
    } catch (final NoSuchAlgorithmException e) {
      throw new IllegalStateException(Messages.get("IamAuthUtils.sha256NotAvailable"), e);
    }
  }

  /**
   * Escapes a cache key component so that values containing the separator cannot make two different
   * identities produce the same key. A null component is encoded differently from an empty one.
   */
  private static String escapeCacheKeyComponent(final @Nullable String component) {
    if (component == null) {
      return "\\0";
    }
    return component.replace("\\", "\\\\").replace(":", "\\:");
  }

  public static IamTokenUtility getTokenUtility() {
    try {
      // RegularRdsUtility requires AWS Java SDK RDS v2.x to be presented in classpath.
      Class.forName("software.amazon.awssdk.services.rds.RdsUtilities");
      return new RegularRdsUtility();
    } catch (final ClassNotFoundException e) {
      // If Java SDK for RDS isn't presented, try to check the required dependency for LightRdsUtility.
      try {
        // LightRdsUtility requires "software.amazon.awssdk.auth"
        // and "software.amazon.awssdk.http-client-spi" libraries.
        Class.forName("software.amazon.awssdk.http.SdkHttpFullRequest");
        Class.forName("software.amazon.awssdk.auth.signer.params.Aws4PresignerParams");

        // Required libraries are presented. Use a lighter version of RDS utility.
        return new LightRdsUtility();

      } catch (final ClassNotFoundException ex) {
        throw new RuntimeException(Messages.get("AuthenticationToken.javaSdkNotInClasspath"), ex);
      }
    }
  }

  public static String generateAuthenticationToken(
      final IamTokenUtility tokenUtils,
      final PluginService pluginService,
      final String user,
      final String hostname,
      final int port,
      final Region region,
      final AwsCredentialsProvider awsCredentialsProvider) {
    final TelemetryFactory telemetryFactory = pluginService.getTelemetryFactory();
    final TelemetryContext telemetryContext = telemetryFactory.openTelemetryContext(
        TELEMETRY_FETCH_TOKEN, TelemetryTraceLevel.NESTED);

    try {
      return tokenUtils.generateAuthenticationToken(awsCredentialsProvider, region, hostname, port, user);
    } catch (final Exception e) {
      if (telemetryContext != null) {
        telemetryContext.setSuccess(false);
        telemetryContext.setException(e);
      }
      throw e;
    } finally {
      if (telemetryContext != null) {
        telemetryContext.closeContext();
      }
    }
  }
}
