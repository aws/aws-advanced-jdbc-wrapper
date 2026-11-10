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

package software.amazon.jdbc.plugin.encryption;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.plugin.encryption.model.EncryptionConfig;

public class KmsEncryptionUtilityTest {

  private final KmsEncryptionUtility utility = new KmsEncryptionUtility(mock(PluginService.class));

  @AfterEach
  void tearDown() {
    utility.cleanup();
  }

  @Test
  void initialize_refusesToStart_withoutAnAllowList() {
    final SQLException e = assertThrows(SQLException.class, () -> utility.initialize(props()));

    assertTrue(e.getMessage().contains(EncryptionConfig.ALLOWED_MASTER_KEY_ARNS.name + " is required"),
        e.getMessage());
  }

  @ParameterizedTest
  @ValueSource(strings = {"", " ", " , "})
  void initialize_refusesToStart_withABlankAllowList(String allowList) {
    final Properties props = props();
    props.setProperty(EncryptionConfig.ALLOWED_MASTER_KEY_ARNS.name, allowList);

    final SQLException e = assertThrows(SQLException.class, () -> utility.initialize(props));

    assertTrue(e.getMessage().contains(EncryptionConfig.ALLOWED_MASTER_KEY_ARNS.name + " is required"),
        e.getMessage());
  }

  @Test
  void initialize_starts_withAnAllowList() {
    final Properties props = props();
    props.setProperty(EncryptionConfig.ALLOWED_MASTER_KEY_ARNS.name, "arn:aws:kms:us-east-1:111111111111:key/abc");

    assertDoesNotThrow(() -> utility.initialize(props));
  }

  private static Properties props() {
    final Properties props = new Properties();
    props.setProperty(EncryptionConfig.KMS_REGION.name, "us-east-1");
    return props;
  }
}
