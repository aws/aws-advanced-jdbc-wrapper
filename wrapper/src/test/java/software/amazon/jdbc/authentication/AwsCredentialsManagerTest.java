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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import software.amazon.jdbc.HostSpec;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

import java.util.Properties;

class AwsCredentialsManagerTest {

  private AutoCloseable closeable;

  @Mock AwsCredentialsProvider mockProvider1;
  @Mock AwsCredentialsProvider mockProvider2;
  @Mock AwsCredentialsProviderHandler mockHandler;
  @Mock Properties mockProps;

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    when(mockHandler.getAwsCredentialsProvider(any(HostSpec.class),
        any(Properties.class))).thenReturn(mockProvider1);
    AwsCredentialsManager.resetCustomHandler();
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  @Test
  public void testAwsCredentialsManager() {
    final String postgresUrl = "db-identifier-postgres.XYZ.us-east-2.rds.amazonaws.com";
    final HostSpec postgresHostSpec = new HostSpec(postgresUrl);

    final String mysqlUrl = "db-identifier-mysql.XYZ.us-east-2.rds.amazonaws.com";
    final HostSpec mysqlHostSpec = new HostSpec(mysqlUrl);

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
}
