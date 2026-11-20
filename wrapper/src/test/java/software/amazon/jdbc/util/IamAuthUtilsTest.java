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
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;

class IamAuthUtilsTest {

  private static final String HOST = "db.cluster-xyz.us-east-2.rds.amazonaws.com";

  @Test
  void testCacheKeyIncludesScopeAndIdentity() {
    assertEquals(
        "iam:us-east-2:" + HOST + ":5432:dbUser:profileA",
        key("iam", new @Nullable String[] {"profileA"}, "dbUser"));
  }

  @Test
  void testCacheKeyDiffersByIdentity() {
    assertNotEquals(
        key("iam", new @Nullable String[] {"profileA"}, "dbUser"),
        key("iam", new @Nullable String[] {"profileB"}, "dbUser"));
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

  private static String key(final String scope, final @Nullable String[] identity, final String user) {
    return IamAuthUtils.getCacheKey(scope, identity, user, HOST, 5432, Region.US_EAST_2);
  }
}
