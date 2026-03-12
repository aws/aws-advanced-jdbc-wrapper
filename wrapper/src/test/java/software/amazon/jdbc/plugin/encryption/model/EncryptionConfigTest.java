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
    assertEquals("encrypt", config.getEncryptionMetadataSchema());
    
    // Test with underscore
    config = EncryptionConfig.builder()
        .kmsRegion("us-east-1")
        .encryptionMetadataSchema("my_schema")
        .build();
    config.validate();
    assertEquals("my_schema", config.getEncryptionMetadataSchema());
    
    // Test with dollar sign
    config = EncryptionConfig.builder()
        .kmsRegion("us-east-1")
        .encryptionMetadataSchema("$schema")
        .build();
    config.validate();
    assertEquals("$schema", config.getEncryptionMetadataSchema());
    
    // Test with numbers
    config = EncryptionConfig.builder()
        .kmsRegion("us-east-1")
        .encryptionMetadataSchema("schema123")
        .build();
    config.validate();
    assertEquals("schema123", config.getEncryptionMetadataSchema());
  }

  @Test
  public void testInvalidSchemaName_SqlInjection() {
    // Test SQL injection attempts
    EncryptionConfig config = EncryptionConfig.builder()
        .kmsRegion("us-east-1")
        .encryptionMetadataSchema("schema; DROP TABLE users--")
        .build();
    
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, config::validate);
    assertNotNull(ex.getMessage());
    
    config = EncryptionConfig.builder()
        .kmsRegion("us-east-1")
        .encryptionMetadataSchema("schema' OR '1'='1")
        .build();
    
    ex = assertThrows(IllegalArgumentException.class, config::validate);
    assertNotNull(ex.getMessage());
    
    config = EncryptionConfig.builder()
        .kmsRegion("us-east-1")
        .encryptionMetadataSchema("schema--comment")
        .build();
    
    ex = assertThrows(IllegalArgumentException.class, config::validate);
    assertNotNull(ex.getMessage());
  }

  @Test
  public void testInvalidSchemaName_SqlKeywords() {
    EncryptionConfig config = EncryptionConfig.builder()
        .kmsRegion("us-east-1")
        .encryptionMetadataSchema("DROP")
        .build();
    
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, config::validate);
    assertNotNull(ex.getMessage());
    
    config = EncryptionConfig.builder()
        .kmsRegion("us-east-1")
        .encryptionMetadataSchema("DELETE")
        .build();
    
    ex = assertThrows(IllegalArgumentException.class, config::validate);
    assertNotNull(ex.getMessage());
    
    config = EncryptionConfig.builder()
        .kmsRegion("us-east-1")
        .encryptionMetadataSchema("SELECT")
        .build();
    
    ex = assertThrows(IllegalArgumentException.class, config::validate);
    assertNotNull(ex.getMessage());
  }

  @Test
  public void testInvalidSchemaName_Empty() {
    EncryptionConfig config = EncryptionConfig.builder()
        .kmsRegion("us-east-1")
        .encryptionMetadataSchema("")
        .build();
    
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, config::validate);
    assertNotNull(ex.getMessage());
    
    config = EncryptionConfig.builder()
        .kmsRegion("us-east-1")
        .encryptionMetadataSchema("   ")
        .build();
    
    ex = assertThrows(IllegalArgumentException.class, config::validate);
    assertNotNull(ex.getMessage());
  }

  @Test
  public void testInvalidSchemaName_SpecialCharacters() {
    EncryptionConfig config = EncryptionConfig.builder()
        .kmsRegion("us-east-1")
        .encryptionMetadataSchema("schema.table")
        .build();
    
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, config::validate);
    assertNotNull(ex.getMessage());
    
    config = EncryptionConfig.builder()
        .kmsRegion("us-east-1")
        .encryptionMetadataSchema("schema*")
        .build();
    
    ex = assertThrows(IllegalArgumentException.class, config::validate);
    assertNotNull(ex.getMessage());
    
    config = EncryptionConfig.builder()
        .kmsRegion("us-east-1")
        .encryptionMetadataSchema("schema@host")
        .build();
    
    ex = assertThrows(IllegalArgumentException.class, config::validate);
    assertNotNull(ex.getMessage());
  }
}
