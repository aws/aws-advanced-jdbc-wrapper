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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;

class LightRdsUtilityTest {

  private static final Instant FIXED_INSTANT = Instant.parse("2024-10-20T00:00:00Z");
  private static final Region REGION = Region.US_WEST_2;
  private static final String HOSTNAME = "test-cluster.cluster-xyz.us-west-2.rds.amazonaws.com";
  private static final int PORT = 5432;
  private static final String USERNAME = "jane_doe";
  private static final AwsCredentials CREDENTIALS =
      AwsBasicCredentials.create("accessKeyId", "secretAccessKey");

  /**
   * Regression test for the {@code NoSuchMethodError} on
   * {@code AwsCredentialsProvider.resolveIdentity()} (see the IAM + Secrets Manager dependency
   * issue). {@link LightRdsUtility} must resolve credentials via the long-stable synchronous
   * {@code resolveCredentials()} and must never call the newer {@code resolveIdentity()} method,
   * which is absent from older transitive {@code auth} versions and triggers the runtime failure.
   */
  @Test
  void generateAuthenticationToken_usesResolveCredentialsAndNeverResolveIdentity() {
    final AwsCredentialsProvider credentialsProvider = mock(AwsCredentialsProvider.class);
    when(credentialsProvider.resolveCredentials()).thenReturn(CREDENTIALS);

    final String token = new LightRdsUtility(FIXED_INSTANT).generateAuthenticationToken(
        credentialsProvider,
        REGION,
        HOSTNAME,
        PORT,
        USERNAME);

    // The credentials must be obtained through resolveCredentials(), never resolveIdentity().
    verify(credentialsProvider).resolveCredentials();
    verify(credentialsProvider, never()).resolveIdentity();

    // Behavior preserved: a valid presigned RDS auth token is produced.
    assertTrue(token.startsWith(HOSTNAME + ":" + PORT),
        () -> "Unexpected token prefix: " + token);
    assertFalse(token.startsWith("https://"), "https:// prefix should be stripped");
    assertTrue(token.contains("Action=connect"), () -> "Missing Action query param: " + token);
    assertTrue(token.contains("DBUser=" + USERNAME), () -> "Missing DBUser query param: " + token);
    assertTrue(token.contains("X-Amz-Algorithm=AWS4-HMAC-SHA256"),
        () -> "Missing SigV4 algorithm: " + token);
    assertTrue(token.contains("X-Amz-Signature="), () -> "Missing SigV4 signature: " + token);
  }

  /**
   * A deterministic clock must yield a deterministic token, confirming the signing path does not
   * depend on wall-clock time and that repeated resolutions are stable.
   */
  @Test
  void generateAuthenticationToken_isDeterministicForFixedClock() {
    final AwsCredentialsProvider credentialsProvider = mock(AwsCredentialsProvider.class);
    when(credentialsProvider.resolveCredentials()).thenReturn(CREDENTIALS);

    final LightRdsUtility utility = new LightRdsUtility(FIXED_INSTANT);
    final String first = utility.generateAuthenticationToken(
        credentialsProvider, REGION, HOSTNAME, PORT, USERNAME);
    final String second = utility.generateAuthenticationToken(
        credentialsProvider, REGION, HOSTNAME, PORT, USERNAME);

    org.junit.jupiter.api.Assertions.assertEquals(first, second);
  }
}
