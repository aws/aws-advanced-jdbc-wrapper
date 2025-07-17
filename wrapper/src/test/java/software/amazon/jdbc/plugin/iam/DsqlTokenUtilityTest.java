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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dsql.DsqlUtilities;
import software.amazon.awssdk.services.dsql.model.GenerateAuthTokenRequest;

class DsqlTokenUtilityTest {

  static final String REGULAR_USER = "testUser";
  static final String ADMIN_USER = "admin";

  private static final Region TEST_REGION = Region.US_EAST_1;
  private static final String TEST_HOSTNAME = String.format("foo0bar1baz2quux3quuux4.dsql.%s.on.aws", TEST_REGION);
  private static final int TEST_PORT = 5432;

  static void assertTokenContainsProperties(final String token, final String hostname, final String username) {
    assertNotNull(token);
    assertFalse(token.isEmpty());

    assertTrue(token.contains(hostname));

    final String expectedAction;
    if (username.equals("admin")) {
      expectedAction = "DbConnectAdmin";
    } else {
      expectedAction = "DbConnect";
    }

    // Include the ampersand to ensure the complete action is compared.
    assertTrue(token.contains("Action=" + expectedAction + "&"));
  }

  private AutoCloseable cleanMocksCallback;
  @Mock private DsqlUtilities mockDsqlUtilities;
  @Mock private AwsCredentialsProvider mockCredentialsProvider;
  @Mock private CompletableFuture<AwsCredentialsIdentity> completableFuture;
  @Mock private AwsCredentialsIdentity mockCredentialsIdentity;
  @Captor private ArgumentCaptor<Consumer<GenerateAuthTokenRequest.Builder>> captor;

  @BeforeEach
  public void setup() throws Exception {
    cleanMocksCallback = MockitoAnnotations.openMocks(this);

    doReturn(completableFuture).when(mockCredentialsProvider).resolveIdentity();
    doReturn(mockCredentialsIdentity).when(completableFuture).get();

    // These must return non-null values in order for signing to proceed.
    doReturn("accessKeyId").when(mockCredentialsIdentity).accessKeyId();
    doReturn("secretAccessKey").when(mockCredentialsIdentity).secretAccessKey();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanMocksCallback.close();
  }

  @ParameterizedTest
  @ValueSource(strings = {REGULAR_USER, ADMIN_USER})
  public void testTokenUtilityCallsCorrectAuthMethod(final String username) {
    final DsqlTokenUtility tokenUtility = new DsqlTokenUtility(mockDsqlUtilities);

    final String expectedToken = "test-token";

    if (username.equals("admin")) {
      doReturn(expectedToken)
          .when(mockDsqlUtilities)
          .generateDbConnectAdminAuthToken(ArgumentMatchers.<Consumer<GenerateAuthTokenRequest.Builder>>any());
    } else {
      doReturn(expectedToken)
          .when(mockDsqlUtilities)
          .generateDbConnectAuthToken(ArgumentMatchers.<Consumer<GenerateAuthTokenRequest.Builder>>any());
    }

    final String actualToken = tokenUtility.generateAuthenticationToken(
        mockCredentialsProvider, TEST_REGION, TEST_HOSTNAME, TEST_PORT, username);

    assertEquals(expectedToken, actualToken);
  }

  @ParameterizedTest
  @ValueSource(strings = {REGULAR_USER, ADMIN_USER})
  public void testTokenUtilityConfiguresBuilder(final String username) {
    final DsqlTokenUtility tokenUtility = new DsqlTokenUtility(mockDsqlUtilities);

    tokenUtility.generateAuthenticationToken(
        mockCredentialsProvider, TEST_REGION, TEST_HOSTNAME, TEST_PORT, username);

    if (username.equals("admin")) {
      verify(mockDsqlUtilities).generateDbConnectAdminAuthToken(captor.capture());
    } else {
      verify(mockDsqlUtilities).generateDbConnectAuthToken(captor.capture());
    }

    GenerateAuthTokenRequest.Builder builder = GenerateAuthTokenRequest.builder();
    captor.getValue().accept(builder);
    GenerateAuthTokenRequest request = builder.build();

    // Verify the builder was configured correctly.
    assertEquals(TEST_HOSTNAME, request.hostname());
    assertEquals(TEST_REGION, request.region());
  }

  @ParameterizedTest
  @ValueSource(strings = {REGULAR_USER, ADMIN_USER})
  public void testTokenGeneratedWithCorrectProperties(final String username) {
    final DsqlTokenUtility tokenUtility = new DsqlTokenUtility();

    final String token = tokenUtility.generateAuthenticationToken(
        mockCredentialsProvider, TEST_REGION, TEST_HOSTNAME, TEST_PORT, username);

    assertTokenContainsProperties(token, TEST_HOSTNAME, username);
  }
}
