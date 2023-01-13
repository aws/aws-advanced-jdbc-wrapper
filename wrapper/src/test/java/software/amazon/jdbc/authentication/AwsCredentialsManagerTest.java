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

package software.amazon.jdbc.authentication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

class AwsCredentialsManagerTest {

  private AutoCloseable closeable;

  @Mock AwsCredentialsProvider mockProvider1;
  @Mock AwsCredentialsProvider mockProvider2;
  @Mock Supplier<AwsCredentialsProvider> mockHandler;

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    when(mockHandler.get()).thenReturn(mockProvider1);
    AwsCredentialsManager.resetCustomHandler();
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  @Test
  public void testCustomHandlerGetterSetter() {
    assertTrue(AwsCredentialsManager.getProvider() instanceof DefaultCredentialsProvider);

    AwsCredentialsManager.setCustomHandler(() -> mockProvider2, 15, TimeUnit.SECONDS);
    assertEquals(mockProvider2, AwsCredentialsManager.getProvider());
    assertEquals(mockProvider2, AwsCredentialsManager.providerCache);
    assertEquals(15, AwsCredentialsManager.timeout);
    assertEquals(TimeUnit.SECONDS, AwsCredentialsManager.timeoutUnit);

    AwsCredentialsManager.setCustomHandler(mockHandler);
    assertEquals(mockHandler, AwsCredentialsManager.handler);
    assertNull(AwsCredentialsManager.providerCache);
    assertEquals(mockProvider1, AwsCredentialsManager.getProvider());
    assertNull(AwsCredentialsManager.providerCache);
    assertEquals(0, AwsCredentialsManager.timeout);
    assertNull(AwsCredentialsManager.timeoutUnit);

    AwsCredentialsManager.getProvider();
    verify(mockHandler, times(2)).get();
  }

  @Test
  public void testResetCustomHandler() {
    AwsCredentialsManager.setCustomHandler(() -> mockProvider2, 15, TimeUnit.SECONDS);
    AwsCredentialsManager.resetCustomHandler();
    assertNull(AwsCredentialsManager.providerCache);
    assertEquals(0, AwsCredentialsManager.timeout);
    assertNull(AwsCredentialsManager.timeoutUnit);
  }

  @Test
  public void testCache() throws InterruptedException {
    AwsCredentialsManager.setCustomHandler(mockHandler, 15, TimeUnit.SECONDS);
    AwsCredentialsManager.getProvider();
    AwsCredentialsManager.getProvider();
    verify(mockHandler, times(1)).get();

    AwsCredentialsManager.configureCache(10000, TimeUnit.MILLISECONDS);
    assertEquals(10000,  AwsCredentialsManager.timeout);
    assertEquals(TimeUnit.MILLISECONDS, AwsCredentialsManager.timeoutUnit);
    assertEquals(mockHandler, AwsCredentialsManager.handler);
    assertEquals(mockProvider1, AwsCredentialsManager.providerCache);

    AwsCredentialsManager.setCustomHandler(mockHandler, 100, TimeUnit.MILLISECONDS);
    AwsCredentialsManager.getProvider();
    Thread.sleep(100);
    AwsCredentialsManager.getProvider();
    verify(mockHandler, times(3)).get();
  }
}
