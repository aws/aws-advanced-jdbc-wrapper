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

package software.amazon.jdbc.plugin.encryption.logging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.kms.model.KmsException;

public class SanitizerTest {

  private static final String ARN = "arn:aws:kms:us-east-1:111111111111:key/12345678-aaaa-bbbb-cccc-abcdefabcdef";

  @Test
  void arn_masksAccountRegionAndTheMiddleOfTheKeyId() {
    assertEquals("arn:aws:kms:***:***:key/1234***cdef", Sanitizer.arn(ARN));
    assertEquals("arn:aws:kms:***:***:key/1234***cdef", Sanitizer.arn("key/12345678-aaaa-bbbb-cccc-abcdefabcdef"));
    assertEquals("arn:aws:kms:***:***:key/***", Sanitizer.arn("arn:aws:kms:us-east-1:111111111111:key/short"));
    assertEquals("arn:aws:kms:***:***:key/not-***-arn", Sanitizer.arn("not-an-arn"));
    assertEquals("arn:aws:kms:***:***:key/1234***cdef", Sanitizer.arn("12345678-aaaa-bbbb-cccc-abcdefabcdef"));
    assertEquals("arn:aws:kms:***:***:key/mrk-***cdef", Sanitizer.arn("mrk-1234567890abcdef"));
    assertFalse(Sanitizer.arn("alias/prod-payments-key").contains("prod"));
    assertEquals("arn:aws:kms:***:***:key/***", Sanitizer.arn("/leading"));
    assertEquals("arn:aws:kms:***:***:key/***", Sanitizer.arn("trailing/"));
    assertNull(Sanitizer.arn(null));
  }

  @Test
  void arn_masksTheKeyIdTheSameWayAsKeyId() {
    final String id = "12345678-aaaa-bbbb-cccc-abcdefabcdef";
    assertTrue(Sanitizer.arn(ARN).endsWith("/" + Sanitizer.keyId(id)));
    assertFalse(Sanitizer.arn(ARN).contains(id));
  }

  @Test
  void arn_isIdempotent() {
    assertEquals(Sanitizer.arn(ARN), Sanitizer.arn(Sanitizer.arn(ARN)));
  }

  @Test
  void arn_replacesControlCharactersSoAForgedArnCannotForgeLogLines() {
    final String forged = "arn:aws:kms:us-east-1:222222222222:key/evil\nSEVERE: forged line";
    final String masked = Sanitizer.arn(forged);
    assertEquals("arn:aws:kms:***:***:key/evil***line", masked);

    // Control characters in the part of the key id that is kept are replaced too.
    final String edges = Sanitizer.arn("key/\n\r\u2028\u0085-middle-\u2029\n\r\u0000");
    assertFalse(edges.contains("\n") || edges.contains("\r") || edges.contains("\u2028")
        || edges.contains("\u2029") || edges.contains("\u0085") || edges.contains("\u0000"), edges);
    assertEquals("arn:aws:kms:***:***:key/????***????", edges);
    assertEquals("????***????", Sanitizer.keyId("\n\r\u2028\u0085-middle-\u2029\n\r\u0000"));
  }

  @Test
  void arnList_masksEveryEntry() {
    final String other = "arn:aws:kms:eu-west-1:999999999999:key/99999999-aaaa-bbbb-cccc-000000000000";
    final String masked = Sanitizer.arnList(ARN + " , " + other + ",alias/mine,,");
    assertEquals("arn:aws:kms:***:***:key/1234***cdef, arn:aws:kms:***:***:key/9999***0000, "
        + "arn:aws:kms:***:***:alias/***", masked);
    assertEquals("", Sanitizer.arnList(""));
    assertNull(Sanitizer.arnList(null));
  }

  @Test
  void keyId_keepsTheFirstAndLastFourCharacters() {
    assertEquals("1234***cdef", Sanitizer.keyId("12345678-aaaa-bbbb-cccc-abcdefabcdef"));
    assertEquals("***", Sanitizer.keyId("12345678"));
    assertEquals("1234***6789", Sanitizer.keyId("123456789"));
    assertNull(Sanitizer.keyId(null));
  }

  @Test
  void connectionUrl_masksUserInfoAndPasswordParameters() {
    assertEquals("jdbc:postgresql://***:***@host:5432/db?x=a@b&password=***&PWD=***",
        Sanitizer.connectionUrl("jdbc:postgresql://user:p%40ss:w@rd@host:5432/db?x=a@b&password=secret&PWD=other"));
    assertEquals("jdbc:mysql://host/db?user=u&password=***",
        Sanitizer.connectionUrl("jdbc:mysql://host/db?user=u&password=s3"));
    assertEquals("jdbc:mysql://host/db?pwd=***&b=1", Sanitizer.connectionUrl("jdbc:mysql://host/db?pwd=s3&b=1"));
    assertNull(Sanitizer.connectionUrl(null));
  }

  @Test
  void connectionUrl_doesNotOverMatch() {
    // The host and port look like user:password to a regex that is not bounded by the authority.
    final String plain = "jdbc:postgresql://host:5432/db?x=a@b";
    assertEquals(plain, Sanitizer.connectionUrl(plain));
    // A user name without a password is masked too.
    assertEquals("jdbc:postgresql://***@host/db", Sanitizer.connectionUrl("jdbc:postgresql://user@host/db"));
    // Deliberate: "host/db@x" has the same shape as a password containing '/', so the host is
    // masked rather than risk logging the password. See Sanitizer.connectionUrl.
    assertEquals("jdbc:mysql://***@x", Sanitizer.connectionUrl("jdbc:mysql://host/db@x"));
    // A password containing '/' is masked whole.
    assertEquals("jdbc:postgresql://***:***@host/db", Sanitizer.connectionUrl("jdbc:postgresql://u:pa/ss@host/db"));
    // Two URLs: each authority is masked on its own, and the text between them is kept.
    assertEquals("a://***:***@h1/x b://***:***@h2/y", Sanitizer.connectionUrl("a://u:p@h1/x b://v:q@h2/y"));
  }

  @Test
  void connectionUrl_runsInLinearTimeOnTheOldRegexWorstCase() {
    // The old pattern ://[^:/@]+:[^@]*@ rescanned to the end of the input from every "://x:".
    final String crafted = new String(new char[100_000]).replace("\0", "://a:");
    final String masked = assertTimeoutPreemptively(Duration.ofSeconds(2), () -> Sanitizer.connectionUrl(crafted));
    assertEquals(crafted, masked);

    final String withCredentials = crafted + "@host/db";
    assertTimeoutPreemptively(Duration.ofSeconds(2), () -> Sanitizer.connectionUrl(withCredentials));

    final String manyAts = "://u:" + new String(new char[100_000]).replace("\0", "@") + "host";
    assertEquals("://***:***@host", assertTimeoutPreemptively(Duration.ofSeconds(2),
        () -> Sanitizer.connectionUrl(manyAts)));
  }

  @Test
  void errorSummary_ofAnAwsError_keepsOnlyTheCodeAndStatus() {
    final KmsException error = (KmsException) KmsException.builder()
        .message("arn:aws:iam::333333333333:role/SecretRoleName is not authorized on " + ARN)
        .statusCode(400)
        .awsErrorDetails(AwsErrorDetails.builder().errorCode("AccessDeniedException").build())
        .build();
    assertEquals("KmsException[errorCode=AccessDeniedException, httpStatus=400]", Sanitizer.errorSummary(error));

    final KmsException noDetails = (KmsException) KmsException.builder().message(ARN).statusCode(500).build();
    assertEquals("KmsException[errorCode=unknown, httpStatus=500]", Sanitizer.errorSummary(noDetails));
    assertNull(Sanitizer.errorSummary(null));
  }

  @Test
  void errorSummary_ofAnotherError_masksArnsInTheMessage() {
    final String summary = Sanitizer.errorSummary(new IllegalStateException(
        "role arn:aws:iam::333333333333:role/SecretRoleName cannot use " + ARN + ", retry.\nSEVERE: forged"));
    assertEquals("IllegalStateException: role arn:*** cannot use "
        + "arn:aws:kms:***:***:key/1234***cdef, retry.?SEVERE: forged", summary);
    assertEquals("IllegalStateException", Sanitizer.errorSummary(new IllegalStateException()));
    assertEquals("IllegalStateException", Sanitizer.errorSummary(new IllegalStateException("")));
  }

  @Test
  void errorMessage_isBoundedAndLinear() {
    assertEquals(200, Sanitizer.errorMessage(new String(new char[10_000]).replace("\0", "x")).length());
    final String crafted = new String(new char[100_000]).replace("\0", "arn:");
    assertTimeoutPreemptively(Duration.ofSeconds(2), () -> Sanitizer.errorMessage(crafted));
    // "learn:" is not an ARN.
    assertEquals("learn:more", Sanitizer.errorMessage("learn:more"));
  }

  @Test
  void arn_masksAliasesCompletely() {
    assertEquals("arn:aws:kms:***:***:alias/***",
        Sanitizer.arn("arn:aws:kms:us-east-1:111122223333:alias/prod-payments-key"));
    assertEquals("arn:aws:kms:***:***:alias/***", Sanitizer.arn("alias/prod-payments-key"));
    assertEquals("arn:aws:kms:***:***:alias/***", Sanitizer.arn(" Alias/prodpayments"));
    assertEquals(Sanitizer.arn("alias/x"), Sanitizer.arn(Sanitizer.arn("alias/x")));
  }

  @Test
  void arn_replacesAnArnThatIsNotAKmsKeyWhole() {
    assertEquals("arn:***", Sanitizer.arn("arn:aws:iam::123456789012:role/Administrator"));
  }

  @Test
  void connectionUrl_masksPasswordLikeParameters() {
    assertEquals("jdbc:postgresql://h/db?user=u&sslpassword=***",
        Sanitizer.connectionUrl("jdbc:postgresql://h/db?user=u&sslpassword=s3cret"));
    assertEquals("jdbc:postgresql://h/db?trustStorePassword=***&a=1",
        Sanitizer.connectionUrl("jdbc:postgresql://h/db?trustStorePassword=x&a=1"));
    assertEquals("jdbc:x://h/db?token=***", Sanitizer.connectionUrl("jdbc:x://h/db?token=t"));
    // Parameters that do not look like secrets are left alone.
    assertEquals("jdbc:x://h/db?ssl=true&user=u", Sanitizer.connectionUrl("jdbc:x://h/db?ssl=true&user=u"));
  }

  @Test
  void errorMessage_masksUrlsAndSecretsInFreeText() {
    final String masked = Sanitizer.errorMessage(
        "Connection to jdbc:postgresql://u:pw@host/db?password=zzz failed, secret=abc ok");
    assertFalse(masked.contains("pw@") || masked.contains("zzz") || masked.contains("abc"), masked);
    assertTrue(masked.contains("failed"), masked);
    assertEquals("arn:aws:kms:***:***:alias/***",
        Sanitizer.errorMessage("arn:aws:kms:us-east-1:111122223333:alias/prod"));
  }

  @Test
  void errorMessage_leavesOrdinaryTextAlone() {
    // Table names, SQL fragments, numbers and UUIDs are not secrets and stay readable.
    final String text = "relation orders_2024 not found at 123456789012 rows, id 12345678-1234-1234-1234-123456789012";
    assertEquals(text, Sanitizer.errorMessage(text));
  }

  @Test
  void errorMessage_replacesFormatAndBidiCharacters() {
    final char[] hidden = {0x202E, 0x200B, 0x2066, 0xFEFF, 0x061C, 0x00AD};
    final StringBuilder input = new StringBuilder();
    for (final char c : hidden) {
      input.append('a').append(c);
    }
    assertEquals("a?a?a?a?a?a?", Sanitizer.errorMessage(input.toString()));
  }
}
