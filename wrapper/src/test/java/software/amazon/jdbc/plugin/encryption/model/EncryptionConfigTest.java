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

package software.amazon.jdbc.plugin.encryption.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class EncryptionConfigTest {

  @Test
  public void testValidSchemaNames() {
    EncryptionConfig config = EncryptionConfig.builder()
        .kmsRegion("us-east-1")
        .encryptionMetadataSchema("encrypt")
        .build();

    config.validate(); // Should not throw
    assertEquals(SchemaName.of("encrypt"), config.getEncryptionMetadataSchema());

    // Test with underscore
    config = EncryptionConfig.builder()
        .kmsRegion("us-east-1")
        .encryptionMetadataSchema("my_schema")
        .build();
    config.validate();
    assertEquals(SchemaName.of("my_schema"), config.getEncryptionMetadataSchema());

    // Test with underscore prefix
    config = EncryptionConfig.builder()
        .kmsRegion("us-east-1")
        .encryptionMetadataSchema("_schema")
        .build();
    config.validate();
    assertEquals(SchemaName.of("_schema"), config.getEncryptionMetadataSchema());

    // Test with numbers
    config = EncryptionConfig.builder()
        .kmsRegion("us-east-1")
        .encryptionMetadataSchema("schema123")
        .build();
    config.validate();
    assertEquals(SchemaName.of("schema123"), config.getEncryptionMetadataSchema());
  }

  @Test
  public void testInvalidSchemaName_SqlInjection() {
    // Validation now happens eagerly at builder setter time via SchemaName.of()
    assertThrows(IllegalArgumentException.class, () ->
        EncryptionConfig.builder()
            .kmsRegion("us-east-1")
            .encryptionMetadataSchema("schema; DROP TABLE users--"));

    assertThrows(IllegalArgumentException.class, () ->
        EncryptionConfig.builder()
            .kmsRegion("us-east-1")
            .encryptionMetadataSchema("schema' OR '1'='1"));

    assertThrows(IllegalArgumentException.class, () ->
        EncryptionConfig.builder()
            .kmsRegion("us-east-1")
            .encryptionMetadataSchema("schema--comment"));
  }

  @Test
  public void testInvalidSchemaName_Empty() {
    assertThrows(IllegalArgumentException.class, () ->
        EncryptionConfig.builder()
            .kmsRegion("us-east-1")
            .encryptionMetadataSchema(""));

    assertThrows(IllegalArgumentException.class, () ->
        EncryptionConfig.builder()
            .kmsRegion("us-east-1")
            .encryptionMetadataSchema("   "));
  }

  @Test
  public void testInvalidSchemaName_SpecialCharacters() {
    assertThrows(IllegalArgumentException.class, () ->
        EncryptionConfig.builder()
            .kmsRegion("us-east-1")
            .encryptionMetadataSchema("schema.table"));

    assertThrows(IllegalArgumentException.class, () ->
        EncryptionConfig.builder()
            .kmsRegion("us-east-1")
            .encryptionMetadataSchema("schema*"));

    assertThrows(IllegalArgumentException.class, () ->
        EncryptionConfig.builder()
            .kmsRegion("us-east-1")
            .encryptionMetadataSchema("schema@host"));
  }
}
