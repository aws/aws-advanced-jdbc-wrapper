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

package software.amazon.jdbc.plugin.iam;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.CredentialUtils;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.params.Aws4PresignerParams;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.utils.CompletableFutureUtils;
import software.amazon.awssdk.utils.StringUtils;
import software.amazon.jdbc.util.Messages;

public class LightRdsUtility implements IamTokenUtility {

  private static final Logger LOGGER = Logger.getLogger(LightRdsUtility.class.getName());

  // The time the IAM token is good for. https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.html
  private static final Duration EXPIRATION_DURATION = Duration.ofMinutes(15);

  @Override
  public String generateAuthenticationToken(
      final @NonNull AwsCredentialsProvider credentialsProvider,
      final @NonNull Region region,
      final @NonNull String hostname,
      final int port,
      final @NonNull String username) {

    // The following code is inspired by software.amazon.awssdk.services.rds.DefaultRdsUtilities,
    // method generateAuthenticationToken(GenerateAuthenticationTokenRequest request).
    // Update this code when original method changes.

    final Clock clock = Clock.systemUTC();
    final Aws4Signer signer = Aws4Signer.create();

    SdkHttpFullRequest httpRequest = SdkHttpFullRequest.builder()
        .method(SdkHttpMethod.GET)
        .protocol("https")
        .host(hostname)
        .port(port)
        .encodedPath("/")
        .putRawQueryParameter("DBUser", username)
        .putRawQueryParameter("Action", "connect")
        .build();

    Instant expirationTime = Instant.now(clock).plus(EXPIRATION_DURATION);

    final AwsCredentials credentials = CredentialUtils.toCredentials(
        CompletableFutureUtils.joinLikeSync(credentialsProvider.resolveIdentity()));

    Aws4PresignerParams presignRequest = Aws4PresignerParams.builder()
        .signingClockOverride(clock)
        .expirationTime(expirationTime)
        .awsCredentials(credentials)
        .signingName("rds-db")
        .signingRegion(region)
        .build();

    SdkHttpFullRequest fullRequest = signer.presign(httpRequest, presignRequest);
    String signedUrl = fullRequest.getUri().toString();

    // Format should be:
    // <hostname>>:<port>>/?Action=connect&DBUser=<username>>&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Expi...
    // Note: This must be the real RDS hostname, not proxy or tunnels
    String result = StringUtils.replacePrefixIgnoreCase(signedUrl, "https://", "");
    LOGGER.finest(() -> "Generated RDS authentication token with expiration of " + expirationTime);
    return result;
  }
}
