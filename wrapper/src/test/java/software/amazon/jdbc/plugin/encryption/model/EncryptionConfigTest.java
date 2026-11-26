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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

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

  // ---- builder defaults -----------------------------------------------------------------------

  @Test
  public void testBuilderDefaults_matchThePropertyDefaults() {
    final Properties props = new Properties();
    props.setProperty(EncryptionConfig.KMS_REGION.name, "us-east-1");

    final EncryptionConfig fromBuilder = EncryptionConfig.builder().kmsRegion("us-east-1").build();
    final EncryptionConfig fromProperties = EncryptionConfig.fromProperties(props);

    assertEquals(fromProperties, fromBuilder);
    assertEquals(fromProperties.getEncryptionMetadataSchema(), fromBuilder.getEncryptionMetadataSchema());
    assertEquals(3, fromBuilder.getMaxRetries());
    assertEquals(Duration.ofHours(1), fromBuilder.getDataKeyCacheExpiration());
  }

  // ---- KMS region -------------------------------------------------------------------------------

  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      // a key ARN
      "arn:aws:kms:us-west-2:111111111111:key/abc | us-west-2",
      // an alias ARN
      "arn:aws:kms:eu-west-1:111111111111:alias/payments | eu-west-1",
      // other partitions
      "arn:aws-cn:kms:cn-north-1:111111111111:key/abc | cn-north-1",
      "arn:aws-us-gov:kms:us-gov-west-1:111111111111:key/abc | us-gov-west-1",
      // aliases and bare key ids carry no region and are skipped
      "'alias/payments,arn:aws:kms:ap-south-1:111111111111:key/abc,1234abcd-12ab-34cd-56ef-1234567890ab' | ap-south-1",
      // several ARNs in one region
      "'arn:aws:kms:us-west-2:111111111111:key/old,arn:aws:kms:us-west-2:111111111111:key/new' | us-west-2",
  })
  public void testFromProperties_takesTheRegionFromTheKeyArns(String allowList, String expectedRegion) {
    final Properties props = new Properties();
    props.setProperty(EncryptionConfig.ALLOWED_MASTER_KEY_ARNS.name, allowList);

    assertEquals(expectedRegion, EncryptionConfig.fromProperties(props).getKmsRegion());
  }

  @ParameterizedTest
  @ValueSource(strings = {"", " "})
  public void testFromProperties_treatsABlankRegionAsUnset(String region) {
    final Properties props = new Properties();
    props.setProperty(EncryptionConfig.KMS_REGION.name, region);
    props.setProperty(EncryptionConfig.ALLOWED_MASTER_KEY_ARNS.name, "arn:aws:kms:us-west-2:111111111111:key/abc");

    assertEquals("us-west-2", EncryptionConfig.fromProperties(props).getKmsRegion());
  }

  @Test
  public void testFromProperties_explicitRegionWins_overTheKeyArns_andWarns() {
    final Properties props = new Properties();
    props.setProperty(EncryptionConfig.KMS_REGION.name, "eu-central-1");
    props.setProperty(EncryptionConfig.ALLOWED_MASTER_KEY_ARNS.name,
        "arn:aws:kms:us-west-2:111111111111:key/abc,arn:aws:kms:us-east-1:111111111111:key/def");

    final List<LogRecord> warnings = captureWarnings(() ->
        assertEquals("eu-central-1", EncryptionConfig.fromProperties(props).getKmsRegion()));

    assertEquals(1, warnings.size());
    final String message = warnings.get(0).getMessage();
    assertTrue(message.contains("eu-central-1"), message);
    assertFalse(message.contains("111111111111"), message);
  }

  @Test
  public void testFromProperties_warns_whenOneOfSeveralKeyArnsIsInAnotherRegion() {
    final Properties props = new Properties();
    props.setProperty(EncryptionConfig.KMS_REGION.name, "sa-east-1");
    props.setProperty(EncryptionConfig.ALLOWED_MASTER_KEY_ARNS.name,
        "arn:aws:kms:sa-east-1:111111111111:key/abc,arn:aws:kms:me-south-1:111111111111:key/def");

    final List<LogRecord> warnings = captureWarnings(() ->
        assertEquals("sa-east-1", EncryptionConfig.fromProperties(props).getKmsRegion()));

    assertEquals(1, warnings.size());
  }

  @Test
  public void testFromProperties_warnsAboutTheSameMismatchOnlyOnce() {
    final Properties props = new Properties();
    props.setProperty(EncryptionConfig.KMS_REGION.name, "af-south-1");
    props.setProperty(EncryptionConfig.ALLOWED_MASTER_KEY_ARNS.name, "arn:aws:kms:ap-east-1:111111111111:key/abc");
    final Properties otherKey = new Properties();
    otherKey.setProperty(EncryptionConfig.KMS_REGION.name, "af-south-1");
    otherKey.setProperty(EncryptionConfig.ALLOWED_MASTER_KEY_ARNS.name, "arn:aws:kms:ap-east-1:222222222222:key/def");

    final List<LogRecord> warnings = captureWarnings(() -> {
      EncryptionConfig.fromProperties(props);
      EncryptionConfig.fromProperties(props);
      EncryptionConfig.fromProperties(otherKey);
    });

    assertEquals(1, warnings.size());
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "arn:aws:kms:us-west-2:111111111111:key/abc",
      "alias/payments,arn:aws:kms:us-west-2:111111111111:key/abc",
      "alias/payments"})
  public void testFromProperties_explicitRegion_doesNotWarn_whenNoKeyArnNamesAnotherRegion(String allowList) {
    final Properties props = new Properties();
    props.setProperty(EncryptionConfig.KMS_REGION.name, "us-west-2");
    props.setProperty(EncryptionConfig.ALLOWED_MASTER_KEY_ARNS.name, allowList);

    final List<LogRecord> warnings = captureWarnings(() ->
        assertEquals("us-west-2", EncryptionConfig.fromProperties(props).getKmsRegion()));

    assertTrue(warnings.isEmpty(), warnings.toString());
  }

  private static List<LogRecord> captureWarnings(Runnable action) {
    final Logger logger = Logger.getLogger(EncryptionConfig.class.getName());
    final List<LogRecord> records = new ArrayList<>();
    final Handler handler = new Handler() {
      @Override
      public void publish(LogRecord record) {
        if (record.getLevel().intValue() >= Level.WARNING.intValue()) {
          records.add(record);
        }
      }

      @Override
      public void flush() {
      }

      @Override
      public void close() {
      }
    };
    logger.addHandler(handler);
    try {
      action.run();
    } finally {
      logger.removeHandler(handler);
    }
    return records;
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "alias/payments",
      "1234abcd-12ab-34cd-56ef-1234567890ab",
      "alias/payments,1234abcd-12ab-34cd-56ef-1234567890ab",
      // an ARN with an empty region field names no region
      "arn:aws:kms::111111111111:key/abc",
      " , "})
  public void testFromProperties_refusesWithoutARegion_whenNoKeyArnNamesOne(String allowList) {
    final Properties props = new Properties();
    props.setProperty(EncryptionConfig.ALLOWED_MASTER_KEY_ARNS.name, allowList);

    final IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> EncryptionConfig.fromProperties(props));

    assertTrue(e.getMessage().startsWith(EncryptionConfig.KMS_REGION.name + " is required"), e.getMessage());
    assertTrue(e.getMessage().contains(EncryptionConfig.ALLOWED_MASTER_KEY_ARNS.name), e.getMessage());
  }

  @Test
  public void testFromProperties_refusesWithoutARegionOrAnAllowList() {
    final IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> EncryptionConfig.fromProperties(new Properties()));

    assertTrue(e.getMessage().startsWith(EncryptionConfig.KMS_REGION.name + " is required"), e.getMessage());
  }

  @Test
  public void testFromProperties_refusesWithoutARegion_whenTheKeyArnsAreInSeveralRegions() {
    final Properties props = new Properties();
    props.setProperty(EncryptionConfig.ALLOWED_MASTER_KEY_ARNS.name,
        "arn:aws:kms:us-west-2:111111111111:key/abc,arn:aws:kms:us-east-1:222222222222:key/def");

    final IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> EncryptionConfig.fromProperties(props));

    assertTrue(e.getMessage().contains("more than one region"), e.getMessage());
    assertTrue(e.getMessage().contains(EncryptionConfig.KMS_REGION.name), e.getMessage());
    assertFalse(e.getMessage().contains("111111111111"), e.getMessage());
    assertFalse(e.getMessage().contains("222222222222"), e.getMessage());
  }

  @Test
  public void testFromProperties_doesNotModifyTheProperties() {
    final Properties props = new Properties();
    props.setProperty(EncryptionConfig.ALLOWED_MASTER_KEY_ARNS.name, "arn:aws:kms:us-west-2:111111111111:key/abc");
    final Properties before = new Properties();
    before.putAll(props);

    EncryptionConfig.fromProperties(props);

    assertEquals(before, props);
  }
}
