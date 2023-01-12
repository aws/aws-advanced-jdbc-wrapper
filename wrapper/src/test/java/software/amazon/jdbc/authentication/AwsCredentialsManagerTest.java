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
  @Mock Supplier<AwsCredentialsProvider> mockSupplier;

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    when(mockSupplier.get()).thenReturn(mockProvider2);
    AwsCredentialsManager.resetCustomSupplier();
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  @Test
  public void testCustomSupplierGetterSetter() {
    assertTrue(AwsCredentialsManager.getProvider() instanceof DefaultCredentialsProvider);

    AwsCredentialsManager.setCustomSupplier(() -> mockProvider1, 15, TimeUnit.SECONDS);
    assertEquals(mockProvider1, AwsCredentialsManager.getProvider());
    assertEquals(mockProvider1, AwsCredentialsManager.providerCache);
    assertEquals(15, AwsCredentialsManager.cacheTimeout);
    assertEquals(TimeUnit.SECONDS, AwsCredentialsManager.timeoutUnit);

    AwsCredentialsManager.setCustomSupplier(mockSupplier);
    assertEquals(mockSupplier, AwsCredentialsManager.customSupplier);
    assertNull(AwsCredentialsManager.providerCache);
    assertEquals(mockProvider2, AwsCredentialsManager.getProvider());
    assertNull(AwsCredentialsManager.providerCache);
    assertEquals(0, AwsCredentialsManager.cacheTimeout);
    assertNull(AwsCredentialsManager.timeoutUnit);

    AwsCredentialsManager.getProvider();
    verify(mockSupplier, times(2)).get();
  }

  @Test
  public void testResetCustomSupplier() {
    AwsCredentialsManager.setCustomSupplier(() -> mockProvider1, 15, TimeUnit.SECONDS);
    AwsCredentialsManager.resetCustomSupplier();
    assertNull(AwsCredentialsManager.providerCache);
    assertEquals(0, AwsCredentialsManager.cacheTimeout);
    assertNull(AwsCredentialsManager.timeoutUnit);
  }

  @Test
  public void testCacheExpiration() throws InterruptedException {
    AwsCredentialsManager.setCustomSupplier(mockSupplier, 15, TimeUnit.SECONDS);
    AwsCredentialsManager.getProvider();
    AwsCredentialsManager.getProvider();
    verify(mockSupplier, times(1)).get();

    AwsCredentialsManager.setCustomSupplier(mockSupplier, 100, TimeUnit.MILLISECONDS);
    AwsCredentialsManager.getProvider();
    Thread.sleep(100);
    AwsCredentialsManager.getProvider();
    verify(mockSupplier, times(3)).get();
  }
}
