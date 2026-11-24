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

package software.amazon.jdbc.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;

class IamAuthUtilsTest {

  private static final String HOST = "db.cluster-xyz.us-east-2.rds.amazonaws.com";

  @Test
  void testCacheKeyIncludesScopeAndIdentity() {
    assertEquals(
        "iam:us-east-2:" + HOST + ":5432:dbUser:identityA",
        key("iam", new @Nullable String[] {"identityA"}, "dbUser"));
  }

  @Test
  void testCacheKeyDiffersByIdentity() {
    assertNotEquals(
        key("iam", new @Nullable String[] {"identityA"}, "dbUser"),
        key("iam", new @Nullable String[] {"identityB"}, "dbUser"));
  }

  @Test
  void testCacheKeyDiffersByScope() {
    assertNotEquals(
        key("iam", new @Nullable String[] {"x"}, "dbUser"),
        key("okta", new @Nullable String[] {"x"}, "dbUser"));
  }

  @Test
  void testCacheKeyDistinguishesNullFromEmptyIdentity() {
    assertNotEquals(
        key("iam", new @Nullable String[] {null}, "dbUser"),
        key("iam", new @Nullable String[] {""}, "dbUser"));
  }

  @Test
  void testCacheKeySeparatorInComponentsCannotCollide() {
    // Without escaping, both of these would produce "...:a:b:c".
    assertNotEquals(
        key("iam", new @Nullable String[] {"a:b", "c"}, "dbUser"),
        key("iam", new @Nullable String[] {"a", "b:c"}, "dbUser"));
    assertNotEquals(
        key("iam", new @Nullable String[] {"b"}, "dbUser:a"),
        key("iam", new @Nullable String[] {"a:b"}, "dbUser"));
  }

  @Test
  void testCredentialsIdentityFollowsAccessKeyId() {
    final String identityA = IamAuthUtils.getCredentialsIdentity(
        StaticCredentialsProvider.create(AwsBasicCredentials.create("AKIAEXAMPLEA", "secret1")));
    final String identityB = IamAuthUtils.getCredentialsIdentity(
        StaticCredentialsProvider.create(AwsBasicCredentials.create("AKIAEXAMPLEB", "secret1")));
    final String identityARotatedSecret = IamAuthUtils.getCredentialsIdentity(
        StaticCredentialsProvider.create(AwsBasicCredentials.create("AKIAEXAMPLEA", "secret2")));

    assertNotEquals(identityA, identityB);
    assertEquals(identityA, identityARotatedSecret);
  }

  @Test
  void testCredentialsIdentityDiffersForTemporaryCredentials() {
    // Each set of temporary credentials has its own access key ID.
    assertNotEquals(
        IamAuthUtils.getCredentialsIdentity(StaticCredentialsProvider.create(
            AwsSessionCredentials.create("ASIAEXAMPLE1", "secret", "token1"))),
        IamAuthUtils.getCredentialsIdentity(StaticCredentialsProvider.create(
            AwsSessionCredentials.create("ASIAEXAMPLE2", "secret", "token2"))));
  }

  @Test
  void testCredentialsIdentityDoesNotExposeAccessKeyIdOrSecret() {
    final String identity = IamAuthUtils.getCredentialsIdentity(
        StaticCredentialsProvider.create(AwsBasicCredentials.create("AKIAEXAMPLEA", "secretValue")));

    assertFalse(identity.contains("AKIAEXAMPLEA"));
    assertFalse(identity.contains("secretValue"));
  }

  private static String key(final String scope, final @Nullable String[] identity, final String user) {
    return IamAuthUtils.getCacheKey(scope, identity, user, HOST, 5432, Region.US_EAST_2);
  }

  @Test
  void test_passwordFingerprint_equalForEqualPasswords() {
    assertEquals(IamAuthUtils.getPasswordFingerprint("s3cret"), IamAuthUtils.getPasswordFingerprint("s3cret"));
    assertNotEquals(IamAuthUtils.getPasswordFingerprint("s3cret"), IamAuthUtils.getPasswordFingerprint("s3cret2"));
  }

  @Test
  void test_passwordFingerprint_doesNotContainPassword() {
    final String fingerprint = IamAuthUtils.getPasswordFingerprint("s3cret");
    assertNotNull(fingerprint);
    assertFalse(fingerprint.contains("s3cret"));
  }

  @Test
  void test_passwordFingerprint_distinguishesNullAndEmpty() {
    assertNull(IamAuthUtils.getPasswordFingerprint(null));
    assertNotNull(IamAuthUtils.getPasswordFingerprint(""));
  }

  @Test
  void testCredentialsIdentityIsShortHexOrNone() {
    final String identity = IamAuthUtils.getCredentialsIdentity(AwsBasicCredentials.create("AKIAEXAMPLEA", "secret"));

    assertEquals(IamAuthUtils.CREDENTIALS_IDENTITY_LENGTH, identity.length());
    assertTrue(identity.matches("[0-9a-f]+"));
    assertEquals(IamAuthUtils.NO_CREDENTIALS_IDENTITY, IamAuthUtils.getCredentialsIdentity((AwsCredentials) null));
  }
}
