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
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;

class AwsCredentialsManagerTest {

  private AutoCloseable closeable;

  @Mock AwsCredentialsProvider mockProvider1;
  @Mock AwsCredentialsProvider mockProvider2;
  @Mock AwsCredentialsProviderHandler mockHandler;
  @Mock HostSpec mockHostSpec;
  @Mock Properties mockProps;

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    when(mockHandler.getAwsCredentialsProvider(any(HostSpec.class),
        any(Properties.class))).thenReturn(mockProvider1);
    AwsCredentialsManager.resetCustomHandler();
    AwsCredentialsManager.clearReusableProviders();
  }

  @AfterEach
  void tearDown() throws Exception {
    AwsCredentialsManager.resetCustomHandler();
    AwsCredentialsManager.clearReusableProviders();
    closeable.close();
  }

  @Test
  public void testAwsCredentialsManager() {
    final String postgresUrl = "db-identifier-postgres.XYZ.us-east-2.rds.amazonaws.com";
    final HostSpec postgresHostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host(postgresUrl)
        .build();

    final String mysqlUrl = "db-identifier-mysql.XYZ.us-east-2.rds.amazonaws.com";
    final HostSpec mysqlHostSpec = new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host(mysqlUrl)
        .build();

    AwsCredentialsManager.setCustomHandler((hostSpec, props) -> {
      if (postgresUrl.equals(hostSpec.getHost())) {
        return mockProvider1;
      } else {
        return mockProvider2;
      }
    });

    assertEquals(mockProvider1, AwsCredentialsManager.getProvider(postgresHostSpec, mockProps));
    assertEquals(mockProvider2, AwsCredentialsManager.getProvider(mysqlHostSpec, mockProps));

    AwsCredentialsManager.resetCustomHandler();
    assertTrue(AwsCredentialsManager.getProvider(postgresHostSpec,
        mockProps) instanceof DefaultCredentialsProvider);
  }

  @Test
  public void testNullProvider() {
    AwsCredentialsManager.setCustomHandler(((hostSpec, props) -> null));
    assertTrue(AwsCredentialsManager.getProvider(mockHostSpec, mockProps) instanceof DefaultCredentialsProvider);
  }

  @Test
  public void testReusableProviderReusedForSameSettings() {
    final Properties props = new Properties();
    PropertyDefinition.AWS_PROFILE.set(props, "profileA");

    final AwsCredentialsProvider first =
        AwsCredentialsManager.getReusableProvider(mockHostSpec, props, Region.US_EAST_2, false);

    assertSame(first, AwsCredentialsManager.getReusableProvider(mockHostSpec, props, Region.US_EAST_2, false));
  }

  @Test
  public void testReusableProviderNotSharedAcrossProfilesOrRegions() {
    final Properties propsA = new Properties();
    PropertyDefinition.AWS_PROFILE.set(propsA, "profileA");
    final Properties propsB = new Properties();
    PropertyDefinition.AWS_PROFILE.set(propsB, "profileB");

    final AwsCredentialsProvider providerA =
        AwsCredentialsManager.getReusableProvider(mockHostSpec, propsA, Region.US_EAST_2, false);

    assertNotSame(providerA, AwsCredentialsManager.getReusableProvider(mockHostSpec, propsB, Region.US_EAST_2, false));
    assertNotSame(providerA, AwsCredentialsManager.getReusableProvider(mockHostSpec, propsA, Region.US_WEST_2, false));
    assertNotSame(providerA, AwsCredentialsManager.getReusableProvider(mockHostSpec, new Properties(),
        Region.US_EAST_2, false));
  }

  @Test
  public void testReusableProviderRefreshReplacesProvider() {
    final Properties props = new Properties();
    final AwsCredentialsProvider first =
        AwsCredentialsManager.getReusableProvider(mockHostSpec, props, Region.US_EAST_2, false);

    final AwsCredentialsProvider refreshed =
        AwsCredentialsManager.getReusableProvider(mockHostSpec, props, Region.US_EAST_2, true);

    assertNotSame(first, refreshed);
    assertSame(refreshed, AwsCredentialsManager.getReusableProvider(mockHostSpec, props, Region.US_EAST_2, false));
  }

  @Test
  public void testReusableProviderNotUsedForCustomHandler() {
    final Properties props = new Properties();
    AwsCredentialsManager.setCustomHandler((hostSpec, p) -> mockProvider1);
    assertSame(mockProvider1, AwsCredentialsManager.getReusableProvider(mockHostSpec, props, Region.US_EAST_2, false));

    // The handler's provider must not have been kept for later connections that use the default provider.
    AwsCredentialsManager.resetCustomHandler();
    assertTrue(AwsCredentialsManager.getReusableProvider(mockHostSpec, props, Region.US_EAST_2, false)
        instanceof DefaultCredentialsProvider);
  }
}
