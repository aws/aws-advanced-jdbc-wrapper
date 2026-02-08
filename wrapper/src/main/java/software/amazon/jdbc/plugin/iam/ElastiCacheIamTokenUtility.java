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
import java.time.ZoneId;
import java.util.Objects;
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

public class ElastiCacheIamTokenUtility implements IamTokenUtility {

  private static final Logger LOGGER = Logger.getLogger(ElastiCacheIamTokenUtility.class.getName());
  private static final String PARAM_ACTION = "Action";
  private static final String PARAM_USER = "User";
  private static final String ACTION_NAME = "connect";
  private static final String PARAM_RESOURCE_TYPE = "ResourceType";
  private static final String RESOURCE_TYPE_SERVERLESS_CACHE = "ServerlessCache";
  private static final String SERVICE_NAME = "elasticache";
  private static final String PROTOCOL = "http";
  private static final Duration EXPIRATION_DURATION = Duration.ofSeconds(15 * 60 - 30);
  public static final String SERVERLESS_CACHE_IDENTIFIER = ".serverless.";

  private final Clock clock;
  private String cacheName = null;
  private final Aws4Signer signer;

  public ElastiCacheIamTokenUtility(String cacheName) {
    this.cacheName = Objects.requireNonNull(cacheName, "cacheName cannot be null");
    this.clock = Clock.systemUTC();
    this.signer = Aws4Signer.create();
  }

  // For testing only
  public ElastiCacheIamTokenUtility(String cacheName, Instant fixedInstant, Aws4Signer signer) {
    this.cacheName = Objects.requireNonNull(cacheName, "cacheName cannot be null");
    this.clock = Clock.fixed(fixedInstant, ZoneId.of("UTC"));
    this.signer = signer;
  }

  @Override
  public String generateAuthenticationToken(
      final @NonNull AwsCredentialsProvider credentialsProvider,
      final @NonNull Region region,
      final @NonNull String hostname,
      final int port,
      final @NonNull String username) {

    boolean isServerless = isServerlessCache(hostname);
    if (this.cacheName == null) {
      throw new IllegalArgumentException("Cache name cannot be null for cache with IAM authentication");
    }

    SdkHttpFullRequest.Builder requestBuilder = SdkHttpFullRequest.builder()
        .method(SdkHttpMethod.GET)
        .protocol(PROTOCOL) // ElastiCache uses http, not https
        .host(this.cacheName)
        .encodedPath("/")
        .putRawQueryParameter(PARAM_ACTION, ACTION_NAME)
        .putRawQueryParameter(PARAM_USER, username);

    if (isServerless) {
      requestBuilder.putRawQueryParameter(PARAM_RESOURCE_TYPE, RESOURCE_TYPE_SERVERLESS_CACHE);
    }

    final SdkHttpFullRequest httpRequest = requestBuilder.build();

    final Instant expirationTime = Instant.now(this.clock).plus(EXPIRATION_DURATION);

    final AwsCredentials credentials = CredentialUtils.toCredentials(
        CompletableFutureUtils.joinLikeSync(credentialsProvider.resolveIdentity()));

    final Aws4PresignerParams presignRequest = Aws4PresignerParams.builder()
        .signingClockOverride(this.clock)
        .expirationTime(expirationTime)
        .awsCredentials(credentials)
        .signingName(SERVICE_NAME)
        .signingRegion(region)
        .build();

    final SdkHttpFullRequest fullRequest = this.signer.presign(httpRequest, presignRequest);
    final String signedUrl = fullRequest.getUri().toString();

    // Format should be:
    // Regular: <cache-name>/?Action=connect&User=<username>&X-Amz-Security-Token=...&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=...&X-Amz-SignedHeaders=host&X-Amz-Expires=870&X-Amz-Credential=...&X-Amz-Signature=...
    // Serverless: <cache-name>/?Action=connect&User=<username>&ResourceType=ServerlessCache&X-Amz-Security-Token=...&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=...&X-Amz-SignedHeaders=host&X-Amz-Expires=870&X-Amz-Credential=...&X-Amz-Signature=...
    // Note: This must be the real ElastiCache hostname, not proxy or tunnels
    final String result = StringUtils.replacePrefixIgnoreCase(signedUrl, "http://", "");
    LOGGER.finest(() -> "Generated ElastiCache authentication token with expiration of " + expirationTime);
    return result;
  }

  private boolean isServerlessCache(String hostname) {
    if (hostname == null) {
      throw new IllegalArgumentException("Hostname cannot be null");
    }
    return hostname.contains(SERVERLESS_CACHE_IDENTIFIER);
  }
}
