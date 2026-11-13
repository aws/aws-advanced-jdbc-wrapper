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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class EncryptionConfigTest {

  @Test
  public void testFailOnUnmappedParameter_defaultsToTrue() {
    assertTrue(EncryptionConfig.builder().kmsRegion("us-east-1").build()
        .isFailOnUnmappedParameter());

    Properties props = new Properties();
    props.setProperty(EncryptionConfig.KMS_REGION.name, "us-east-1");
    assertTrue(EncryptionConfig.fromProperties(props).isFailOnUnmappedParameter());
  }

  @Test
  public void testFailOnUnmappedParameter_falseFromProperties() {
    Properties props = new Properties();
    props.setProperty(EncryptionConfig.KMS_REGION.name, "us-east-1");
    props.setProperty(EncryptionConfig.FAIL_ON_UNMAPPED_PARAMETER.name, "false");

    EncryptionConfig config = EncryptionConfig.fromProperties(props);

    assertFalse(config.isFailOnUnmappedParameter());
    assertNotEquals(
        EncryptionConfig.builder().kmsRegion("us-east-1").build(),
        EncryptionConfig.builder().kmsRegion("us-east-1").failOnUnmappedParameter(false).build());
  }

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

  @Test
  public void testAllowedMasterKeyArns_emptyByDefault() {
    Properties props = new Properties();
    props.setProperty(EncryptionConfig.KMS_REGION.name, "us-east-1");
    EncryptionConfig config = EncryptionConfig.fromProperties(props);

    assertTrue(config.getAllowedMasterKeyArns().isEmpty());
    assertFalse(config.restrictsMasterKeys());
    assertTrue(config.isMasterKeyAllowed("arn:aws:kms:us-east-1:111111111111:key/any"));
  }

  @Test
  public void testAllowedMasterKeyArns_parsesCommaSeparatedList() {
    Properties props = new Properties();
    props.setProperty(EncryptionConfig.KMS_REGION.name, "us-east-1");
    props.setProperty(EncryptionConfig.ALLOWED_MASTER_KEY_ARNS.name, " key-a , ,alias/b,key-a,  ");

    EncryptionConfig config = EncryptionConfig.fromProperties(props);

    assertEquals(new LinkedHashSet<>(Arrays.asList("key-a", "alias/b")), config.getAllowedMasterKeyArns());
    assertTrue(config.restrictsMasterKeys());
  }

  @Test
  public void testAllowedMasterKeyArns_blankListPlacesNoRestriction() {
    assertFalse(EncryptionConfig.builder().kmsRegion("us-east-1").allowedMasterKeyArns(" , ").build()
        .restrictsMasterKeys());
    assertFalse(EncryptionConfig.builder().kmsRegion("us-east-1")
        .allowedMasterKeyArns(Arrays.asList(null, " ")).build()
        .restrictsMasterKeys());
  }

  @Test
  public void testAllowedMasterKeyArns_isUnmodifiable() {
    EncryptionConfig config = EncryptionConfig.builder().kmsRegion("us-east-1").allowedMasterKeyArns("key-a").build();

    assertThrows(UnsupportedOperationException.class, () -> config.getAllowedMasterKeyArns().add("key-b"));
  }

  @Test
  public void testIsMasterKeyAllowed_requiresAnExactMatch() {
    EncryptionConfig config = EncryptionConfig.builder()
        .kmsRegion("us-east-1")
        .allowedMasterKeyArns(Collections.singletonList("arn:aws:kms:us-east-1:111111111111:key/abc"))
        .build();

    assertTrue(config.isMasterKeyAllowed("arn:aws:kms:us-east-1:111111111111:key/abc"));
    // The same key in another form, or in another case, is not on the list.
    assertFalse(config.isMasterKeyAllowed("abc"));
    assertFalse(config.isMasterKeyAllowed("ARN:AWS:KMS:US-EAST-1:111111111111:KEY/ABC"));
    assertFalse(config.isMasterKeyAllowed("arn:aws:kms:us-east-1:222222222222:key/abc"));
    assertFalse(config.isMasterKeyAllowed(""));
    assertFalse(config.isMasterKeyAllowed(null));
  }

  @Test
  public void testToString_doesNotPrintAllowedMasterKeyArns() {
    final String text = EncryptionConfig.builder()
        .kmsRegion("us-east-1")
        .allowedMasterKeyArns("arn:aws:kms:us-east-1:111111111111:key/abc,alias/b")
        .build()
        .toString();

    assertFalse(text.contains("111111111111"), text);
    assertFalse(text.contains("alias/b"), text);
    assertTrue(text.contains("allowedMasterKeyArns=<2 keys>"), text);
  }

  @Test
  public void testAllowedMasterKeyArns_partOfEquality() {
    assertNotEquals(
        EncryptionConfig.builder().kmsRegion("us-east-1").build(),
        EncryptionConfig.builder().kmsRegion("us-east-1").allowedMasterKeyArns("key-a").build());
    assertEquals(
        EncryptionConfig.builder().kmsRegion("us-east-1").allowedMasterKeyArns("key-a,key-b").build(),
        EncryptionConfig.builder().kmsRegion("us-east-1").allowedMasterKeyArns(Arrays.asList("key-a", "key-b"))
            .build());
  }
}
