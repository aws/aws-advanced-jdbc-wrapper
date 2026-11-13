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

package software.amazon.jdbc.plugin.encryption.factory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.sql.SQLException;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.plugin.encryption.model.EncryptionConfig;
import software.amazon.jdbc.plugin.encryption.wrapper.EncryptingDataSource;

public class EncryptingDataSourceFactoryTest {

  private static final String ARN = "arn:aws:kms:us-east-1:111111111111:key/abc";

  @Test
  void build_refusesToStart_withoutAnAllowList() {
    final SQLException e = assertThrows(SQLException.class,
        () -> builder().build());

    assertTrue(e.getMessage().contains(EncryptionConfig.ALLOWED_MASTER_KEY_ARNS.name + " is required"),
        e.getMessage());
  }

  @Test
  void build_starts_withAllowedMasterKeyArns() throws Exception {
    final EncryptingDataSource dataSource = builder().allowedMasterKeyArns(ARN).build();
    dataSource.close();
  }

  @Test
  @SuppressWarnings("deprecation")
  void build_starts_withTheDeprecatedKmsKeyArn() throws Exception {
    final EncryptingDataSource dataSource = builder().kmsKeyArn(ARN).build();
    dataSource.close();
  }

  @Test
  @SuppressWarnings("deprecation")
  void kmsKeyArn_andAllowedMasterKeyArns_lastCallWins() {
    assertThrows(SQLException.class, () -> builder().kmsKeyArn(ARN).allowedMasterKeyArns(null).build());
  }

  @Test
  void createWithDefaults_usesTheKeyAsTheAllowList() throws Exception {
    final EncryptingDataSource dataSource =
        EncryptingDataSourceFactory.createWithDefaults(mock(DataSource.class), ARN, "us-east-1");
    dataSource.close();
  }

  @Test
  void createWithDefaults_acceptsACommaSeparatedList() throws Exception {
    final EncryptingDataSource dataSource = EncryptingDataSourceFactory.createWithDefaults(
        mock(DataSource.class), ARN + ",arn:aws:kms:us-east-1:111111111111:key/def", "us-east-1");
    assertEquals(2, dataSource.getEncryptionPlugin().getConfig().getAllowedMasterKeyArns().size());
    dataSource.close();
  }

  private static EncryptingDataSourceFactory.Builder builder() {
    return new EncryptingDataSourceFactory.Builder()
        .dataSource(mock(DataSource.class))
        .region("us-east-1");
  }
}
