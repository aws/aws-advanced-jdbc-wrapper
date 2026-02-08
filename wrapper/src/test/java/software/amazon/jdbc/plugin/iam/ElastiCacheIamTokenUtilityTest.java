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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.params.Aws4PresignerParams;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.regions.Region;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

public class ElastiCacheIamTokenUtilityTest {
  @Mock private AwsCredentialsProvider mockCredentialsProvider;
  @Mock private AwsCredentials mockCredentials;
  @Mock private Aws4Signer mockSigner;
  @Mock private SdkHttpFullRequest mockSignedRequest;

  private AutoCloseable closeable;
  private ElastiCacheIamTokenUtility tokenUtility;
  private final Instant fixedInstant = Instant.parse("2025-01-01T12:00:00Z");

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  @Test
  void testConstructor_WithCacheName() {
    tokenUtility = new ElastiCacheIamTokenUtility("test-cache");
    assertNotNull(tokenUtility);
  }

  @Test
  void testConstructor_WithCacheNameAndFixedInstant() {
    tokenUtility = new ElastiCacheIamTokenUtility("test-cache", fixedInstant, mockSigner);
    assertNotNull(tokenUtility);
  }

  @Test
  void testConstructor_NullCacheName() {
    assertThrows(NullPointerException.class, () ->
        new ElastiCacheIamTokenUtility(null));
  }

  @Test
  void testConstructor_NullCacheNameWithInstant() {
    assertThrows(NullPointerException.class, () ->
        new ElastiCacheIamTokenUtility(null, fixedInstant, mockSigner));
  }

    @Test
    void testGenerateAuthenticationToken_RegularCache() {
      // Setup mock credentials provider to return mockCredentials
      when(mockCredentialsProvider.resolveIdentity())
        .thenReturn((CompletableFuture) CompletableFuture.completedFuture(mockCredentials));

      // Add custom presign behavior to capture and verify arguments
      when(mockSigner.presign(any(SdkHttpFullRequest.class), any(Aws4PresignerParams.class)))
        .thenAnswer(invocation -> {
          SdkHttpFullRequest request = invocation.getArgument(0);
          Aws4PresignerParams presignParams = invocation.getArgument(1);

          // Verify SdkHttpFullRequest
          assertEquals("test-cache", request.host());
          assertEquals("/", request.encodedPath());
          assertEquals("connect", request.rawQueryParameters().get("Action").get(0));
          assertEquals("testuser", request.rawQueryParameters().get("User").get(0));
          assertFalse(request.rawQueryParameters().containsKey("ResourceType"));

          // Verify Aws4PresignerParams
          assertEquals("elasticache", presignParams.signingName());
          assertEquals(Region.US_EAST_1, presignParams.signingRegion());
          assertEquals(mockCredentials, presignParams.awsCredentials());

          Instant expectedExpiration = fixedInstant.plus(Duration.ofSeconds(15 * 60 - 30));
          assertEquals(expectedExpiration, presignParams.expirationTime().get());
          assertEquals(fixedInstant, presignParams.signingClockOverride().get().instant());

          return mockSignedRequest;
        });

      when(mockSignedRequest.getUri()).thenReturn(java.net.URI.create("http://test-cache/result"));

      tokenUtility = new ElastiCacheIamTokenUtility("test-cache", fixedInstant, mockSigner);

      String token = tokenUtility.generateAuthenticationToken(
        mockCredentialsProvider, Region.US_EAST_1, "test-cache.cache.amazonaws.com", 6379, "testuser");

      assertEquals("test-cache/result", token);
      verify(mockSigner).presign(any(SdkHttpFullRequest.class), any(Aws4PresignerParams.class));
    }

  @Test
  void testGenerateAuthenticationToken_ServerlessCache() {
    // Setup mock credentials provider to return mockCredentials
    when(mockCredentialsProvider.resolveIdentity())
            .thenReturn((CompletableFuture) CompletableFuture.completedFuture(mockCredentials));

    // Add custom presign behavior to capture and verify arguments
    when(mockSigner.presign(any(SdkHttpFullRequest.class), any(Aws4PresignerParams.class)))
      .thenAnswer(invocation -> {
        SdkHttpFullRequest request = invocation.getArgument(0);
        Aws4PresignerParams presignParams = invocation.getArgument(1);

        // Verify SdkHttpFullRequest
        assertEquals("test-cache", request.host());
        assertEquals("/", request.encodedPath());
        assertEquals("connect", request.rawQueryParameters().get("Action").get(0));
        assertEquals("testuser", request.rawQueryParameters().get("User").get(0));
        assertEquals("ServerlessCache", request.rawQueryParameters().get("ResourceType").get(0));

        // Verify Aws4PresignerParams
        assertEquals("elasticache", presignParams.signingName());
        assertEquals(Region.US_EAST_1, presignParams.signingRegion());
        assertEquals(mockCredentials, presignParams.awsCredentials());

        Instant expectedExpiration = fixedInstant.plus(Duration.ofSeconds(15 * 60 - 30));
        assertEquals(expectedExpiration, presignParams.expirationTime().get());
        assertEquals(fixedInstant, presignParams.signingClockOverride().get().instant());

        return mockSignedRequest;
      });

    when(mockSignedRequest.getUri()).thenReturn(java.net.URI.create("http://test-cache.serverless.cache.amazonaws.com/result"));

    tokenUtility = new ElastiCacheIamTokenUtility("test-cache", fixedInstant, mockSigner);

    String token = tokenUtility.generateAuthenticationToken(
        mockCredentialsProvider, Region.US_EAST_1, "test-cache.serverless.cache.amazonaws.com", 6379, "testuser");

    assertEquals("test-cache.serverless.cache.amazonaws.com/result", token);
    verify(mockSigner).presign(any(SdkHttpFullRequest.class), any(Aws4PresignerParams.class));
  }

  @Test
  void testGenerateAuthenticationToken_NullCacheName() {
    tokenUtility = new ElastiCacheIamTokenUtility("test-cache", fixedInstant, mockSigner);

    // Use reflection to set cacheName to null to test the validation
    try {
      java.lang.reflect.Field field = ElastiCacheIamTokenUtility.class.getDeclaredField("cacheName");
      field.setAccessible(true);
      field.set(tokenUtility, null);

      assertThrows(IllegalArgumentException.class, () ->
          tokenUtility.generateAuthenticationToken(
              mockCredentialsProvider, Region.US_EAST_1, "test-host", 6379, "testuser"));
    } catch (Exception e) {
      fail("Reflection failed: " + e.getMessage());
    }
  }

  @Test
  void testGenerateAuthenticationToken_NullHostname() {
    tokenUtility = new ElastiCacheIamTokenUtility("test-cache", fixedInstant, mockSigner);

    assertThrows(IllegalArgumentException.class, () ->
        tokenUtility.generateAuthenticationToken(
            mockCredentialsProvider, Region.US_EAST_1, null, 6379, "testuser"));
  }
}
