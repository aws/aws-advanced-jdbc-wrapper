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

import java.util.logging.Logger;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.jdbc.HostSpec;
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

  public static String getIamHost(final String iamHost, final HostSpec hostSpec) {
    if (!StringUtils.isNullOrEmpty(iamHost)) {
      return iamHost;
    }
    return hostSpec.getHost();
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

  public static String getCacheKey(
      final String user,
      final String hostname,
      final int port,
      final Region region) {

    return String.format("%s:%s:%d:%s", region, hostname, port, user);
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
      telemetryContext.setSuccess(false);
      telemetryContext.setException(e);
      throw e;
    } finally {
      telemetryContext.closeContext();
    }
  }
}
