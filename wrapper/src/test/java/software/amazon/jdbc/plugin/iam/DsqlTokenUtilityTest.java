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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;

import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dsql.DsqlUtilities;
import software.amazon.awssdk.services.dsql.model.GenerateAuthTokenRequest;

class DsqlTokenUtilityTest {

  static final String REGULAR_USER = "testUser";
  static final String ADMIN_USER = "admin";

  private static final Region TEST_REGION = Region.US_EAST_1;
  private static final String TEST_HOSTNAME = String.format("foo0bar1baz2quux3quuux4.dsql.%s.on.aws", TEST_REGION);
  private static final int TEST_PORT = 5432;

  private AutoCloseable cleanMocksCallback;
  @Mock private DsqlUtilities mockDsqlUtilities;
  @Mock private AwsCredentialsProvider mockCredentialsProvider;
  @Mock private DsqlUtilities.Builder mockBuilder;
  @Captor private ArgumentCaptor<Consumer<GenerateAuthTokenRequest.Builder>> captor;

  private MockedStatic<DsqlUtilities> mockDsqlUtilitiesClass;

  @BeforeEach
  public void setup() throws Exception {
    cleanMocksCallback = MockitoAnnotations.openMocks(this);

    mockDsqlUtilitiesClass = mockStatic(DsqlUtilities.class);
    mockDsqlUtilitiesClass.when(DsqlUtilities::builder).thenReturn(mockBuilder);

    doReturn(mockBuilder).when(mockBuilder).credentialsProvider(any());
    doReturn(mockBuilder).when(mockBuilder).region(any());
    doReturn(mockDsqlUtilities).when(mockBuilder).build();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanMocksCallback.close();
    if (mockDsqlUtilitiesClass != null) {
      mockDsqlUtilitiesClass.close();
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {REGULAR_USER, ADMIN_USER})
  public void testTokenUtilityCallsCorrectAuthMethod(final String username) {
    final DsqlTokenUtility tokenUtility = new DsqlTokenUtility();

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
  public void testTokenRequestHasProvidedProperties(final String username) {
    final DsqlTokenUtility tokenUtility = new DsqlTokenUtility();

    tokenUtility.generateAuthenticationToken(
        mockCredentialsProvider, TEST_REGION, TEST_HOSTNAME, TEST_PORT, username);

    if (username.equals("admin")) {
      verify(mockDsqlUtilities).generateDbConnectAdminAuthToken(captor.capture());
    } else {
      verify(mockDsqlUtilities).generateDbConnectAuthToken(captor.capture());
    }

    final GenerateAuthTokenRequest.Builder builder = GenerateAuthTokenRequest.builder();
    captor.getValue().accept(builder);
    final GenerateAuthTokenRequest request = builder.build();

    assertEquals(TEST_HOSTNAME, request.hostname());
    assertEquals(TEST_REGION, request.region());
  }

  @ParameterizedTest
  @ValueSource(strings = {REGULAR_USER, ADMIN_USER})
  public void testBuilderConfiguredWithProvidedProperties(final String username) {
    final DsqlTokenUtility tokenUtility = new DsqlTokenUtility();
    tokenUtility.generateAuthenticationToken(
        mockCredentialsProvider, TEST_REGION, TEST_HOSTNAME, TEST_PORT, username);

    verify(mockBuilder).credentialsProvider(mockCredentialsProvider);
    verify(mockBuilder).region(TEST_REGION);
    verify(mockBuilder).build();
  }
}
