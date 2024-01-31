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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RdsUtilsTests {

  private RdsUtils target;
  private static final String usEastRegionCluster =
      "database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com";
  private static final String usEastRegionClusterReadOnly =
      "database-test-name.cluster-ro-XYZ.us-east-2.rds.amazonaws.com";
  private static final String usEastRegionInstance =
      "instance-test-name.XYZ.us-east-2.rds.amazonaws.com";
  private static final String usEastRegionProxy =
      "proxy-test-name.proxy-XYZ.us-east-2.rds.amazonaws.com";
  private static final String usEastRegionCustomDomain =
      "custom-test-name.cluster-custom-XYZ.us-east-2.rds.amazonaws.com";

  private static final String chinaRegionCluster =
      "database-test-name.cluster-XYZ.rds.cn-northwest-1.amazonaws.com.cn";
  private static final String chinaRegionClusterReadOnly =
      "database-test-name.cluster-ro-XYZ.rds.cn-northwest-1.amazonaws.com.cn";
  private static final String chinaRegionInstance =
      "instance-test-name.XYZ.rds.cn-northwest-1.amazonaws.com.cn";
  private static final String chinaRegionProxy =
      "proxy-test-name.proxy-XYZ.rds.cn-northwest-1.amazonaws.com.cn";
  private static final String chinaRegionCustomDomain =
      "custom-test-name.cluster-custom-XYZ.rds.cn-northwest-1.amazonaws.com.cn";

  private static final String oldChinaRegionCluster =
      "database-test-name.cluster-XYZ.cn-northwest-1.rds.amazonaws.com.cn";
  private static final String oldChinaRegionClusterReadOnly =
      "database-test-name.cluster-ro-XYZ.cn-northwest-1.rds.amazonaws.com.cn";
  private static final String oldChinaRegionInstance =
      "instance-test-name.XYZ.cn-northwest-1.rds.amazonaws.com.cn";
  private static final String oldChinaRegionProxy =
      "proxy-test-name.proxy-XYZ.cn-northwest-1.rds.amazonaws.com.cn";
  private static final String oldChinaRegionCustomDomain =
      "custom-test-name.cluster-custom-XYZ.cn-northwest-1.rds.amazonaws.com.cn";

  private static final String extraRdsChinaPath =
      "database-test-name.cluster-XYZ.rds.cn-northwest-1.rds.amazonaws.com.cn";
  private static final String missingCnChinaPath =
      "database-test-name.cluster-XYZ.rds.cn-northwest-1.amazonaws.com";
  private static final String missingRegionChinaPath =
      "database-test-name.cluster-XYZ.rds.amazonaws.com.cn";

  private static final String usEastRegionElbUrl =
      "elb-name.elb.us-east-2.amazonaws.com";

  @BeforeEach
  public void setupTests() {
    RdsUtils.clearCache();
    target = new RdsUtils();
  }

  @Test
  public void testIsRdsDns() {
    assertTrue(target.isRdsDns(usEastRegionCluster));
    assertTrue(target.isRdsDns(usEastRegionClusterReadOnly));
    assertTrue(target.isRdsDns(usEastRegionInstance));
    assertTrue(target.isRdsDns(usEastRegionProxy));
    assertTrue(target.isRdsDns(usEastRegionCustomDomain));
    assertFalse(target.isRdsDns(usEastRegionElbUrl));

    assertTrue(target.isRdsDns(chinaRegionCluster));
    assertTrue(target.isRdsDns(chinaRegionClusterReadOnly));
    assertTrue(target.isRdsDns(chinaRegionInstance));
    assertTrue(target.isRdsDns(chinaRegionProxy));
    assertTrue(target.isRdsDns(chinaRegionCustomDomain));

    assertTrue(target.isRdsDns(oldChinaRegionCluster));
    assertTrue(target.isRdsDns(oldChinaRegionClusterReadOnly));
    assertTrue(target.isRdsDns(oldChinaRegionInstance));
    assertTrue(target.isRdsDns(oldChinaRegionProxy));
    assertTrue(target.isRdsDns(oldChinaRegionCustomDomain));
  }

  @Test
  public void testGetRdsInstanceHostPattern() {
    final String expectedHostPattern = "?.XYZ.us-east-2.rds.amazonaws.com";
    assertEquals(expectedHostPattern, target.getRdsInstanceHostPattern(usEastRegionCluster));
    assertEquals(expectedHostPattern, target.getRdsInstanceHostPattern(usEastRegionClusterReadOnly));
    assertEquals(expectedHostPattern, target.getRdsInstanceHostPattern(usEastRegionInstance));
    assertEquals(expectedHostPattern, target.getRdsInstanceHostPattern(usEastRegionProxy));
    assertEquals(expectedHostPattern, target.getRdsInstanceHostPattern(usEastRegionCustomDomain));

    final String chinaExpectedHostPattern = "?.XYZ.rds.cn-northwest-1.amazonaws.com.cn";
    assertEquals(chinaExpectedHostPattern, target.getRdsInstanceHostPattern(chinaRegionCluster));
    assertEquals(chinaExpectedHostPattern, target.getRdsInstanceHostPattern(chinaRegionClusterReadOnly));
    assertEquals(chinaExpectedHostPattern, target.getRdsInstanceHostPattern(chinaRegionInstance));
    assertEquals(chinaExpectedHostPattern, target.getRdsInstanceHostPattern(chinaRegionProxy));
    assertEquals(chinaExpectedHostPattern, target.getRdsInstanceHostPattern(chinaRegionCustomDomain));

    final String oldChinaExpectedHostPattern = "?.XYZ.cn-northwest-1.rds.amazonaws.com.cn";
    assertEquals(oldChinaExpectedHostPattern, target.getRdsInstanceHostPattern(oldChinaRegionCluster));
    assertEquals(oldChinaExpectedHostPattern, target.getRdsInstanceHostPattern(oldChinaRegionClusterReadOnly));
    assertEquals(oldChinaExpectedHostPattern, target.getRdsInstanceHostPattern(oldChinaRegionInstance));
    assertEquals(oldChinaExpectedHostPattern, target.getRdsInstanceHostPattern(oldChinaRegionProxy));
    assertEquals(oldChinaExpectedHostPattern, target.getRdsInstanceHostPattern(oldChinaRegionCustomDomain));
  }

  @Test
  public void testIsRdsClusterDns() {
    assertTrue(target.isRdsClusterDns(usEastRegionCluster));
    assertTrue(target.isRdsClusterDns(usEastRegionClusterReadOnly));
    assertFalse(target.isRdsClusterDns(usEastRegionInstance));
    assertFalse(target.isRdsClusterDns(usEastRegionProxy));
    assertFalse(target.isRdsClusterDns(usEastRegionCustomDomain));
    assertFalse(target.isRdsClusterDns(usEastRegionElbUrl));

    assertTrue(target.isRdsClusterDns(chinaRegionCluster));
    assertTrue(target.isRdsClusterDns(chinaRegionClusterReadOnly));
    assertFalse(target.isRdsClusterDns(chinaRegionInstance));
    assertFalse(target.isRdsClusterDns(chinaRegionProxy));
    assertFalse(target.isRdsClusterDns(chinaRegionCustomDomain));

    assertTrue(target.isRdsClusterDns(oldChinaRegionCluster));
    assertTrue(target.isRdsClusterDns(oldChinaRegionClusterReadOnly));
    assertFalse(target.isRdsClusterDns(oldChinaRegionInstance));
    assertFalse(target.isRdsClusterDns(oldChinaRegionProxy));
    assertFalse(target.isRdsClusterDns(oldChinaRegionCustomDomain));
  }

  @Test
  public void testIsWriterClusterDns() {
    assertTrue(target.isWriterClusterDns(usEastRegionCluster));
    assertFalse(target.isWriterClusterDns(usEastRegionClusterReadOnly));
    assertFalse(target.isWriterClusterDns(usEastRegionInstance));
    assertFalse(target.isWriterClusterDns(usEastRegionProxy));
    assertFalse(target.isWriterClusterDns(usEastRegionCustomDomain));
    assertFalse(target.isWriterClusterDns(usEastRegionElbUrl));

    assertTrue(target.isWriterClusterDns(chinaRegionCluster));
    assertFalse(target.isWriterClusterDns(chinaRegionClusterReadOnly));
    assertFalse(target.isWriterClusterDns(chinaRegionInstance));
    assertFalse(target.isWriterClusterDns(chinaRegionProxy));
    assertFalse(target.isWriterClusterDns(chinaRegionCustomDomain));

    assertTrue(target.isWriterClusterDns(oldChinaRegionCluster));
    assertFalse(target.isWriterClusterDns(oldChinaRegionClusterReadOnly));
    assertFalse(target.isWriterClusterDns(oldChinaRegionInstance));
    assertFalse(target.isWriterClusterDns(oldChinaRegionProxy));
    assertFalse(target.isWriterClusterDns(oldChinaRegionCustomDomain));
  }

  @Test
  public void testIsReaderClusterDns() {
    assertFalse(target.isReaderClusterDns(usEastRegionCluster));
    assertTrue(target.isReaderClusterDns(usEastRegionClusterReadOnly));
    assertFalse(target.isReaderClusterDns(usEastRegionInstance));
    assertFalse(target.isReaderClusterDns(usEastRegionProxy));
    assertFalse(target.isReaderClusterDns(usEastRegionCustomDomain));
    assertFalse(target.isReaderClusterDns(usEastRegionElbUrl));

    assertFalse(target.isReaderClusterDns(chinaRegionCluster));
    assertTrue(target.isReaderClusterDns(chinaRegionClusterReadOnly));
    assertFalse(target.isReaderClusterDns(chinaRegionInstance));
    assertFalse(target.isReaderClusterDns(chinaRegionProxy));
    assertFalse(target.isReaderClusterDns(chinaRegionCustomDomain));

    assertFalse(target.isReaderClusterDns(oldChinaRegionCluster));
    assertTrue(target.isReaderClusterDns(oldChinaRegionClusterReadOnly));
    assertFalse(target.isReaderClusterDns(oldChinaRegionInstance));
    assertFalse(target.isReaderClusterDns(oldChinaRegionProxy));
    assertFalse(target.isReaderClusterDns(oldChinaRegionCustomDomain));
  }

  @Test
  public void testGetRdsRegion() {
    final String expectedHostPattern = "us-east-2";
    assertEquals(expectedHostPattern, target.getRdsRegion(usEastRegionCluster));
    assertEquals(expectedHostPattern, target.getRdsRegion(usEastRegionClusterReadOnly));
    assertEquals(expectedHostPattern, target.getRdsRegion(usEastRegionInstance));
    assertEquals(expectedHostPattern, target.getRdsRegion(usEastRegionProxy));
    assertEquals(expectedHostPattern, target.getRdsRegion(usEastRegionCustomDomain));
    assertEquals(expectedHostPattern, target.getRdsRegion(usEastRegionElbUrl));

    final String chinaExpectedHostPattern = "cn-northwest-1";
    assertEquals(chinaExpectedHostPattern, target.getRdsRegion(chinaRegionCluster));
    assertEquals(chinaExpectedHostPattern, target.getRdsRegion(chinaRegionClusterReadOnly));
    assertEquals(chinaExpectedHostPattern, target.getRdsRegion(chinaRegionInstance));
    assertEquals(chinaExpectedHostPattern, target.getRdsRegion(chinaRegionProxy));
    assertEquals(chinaExpectedHostPattern, target.getRdsRegion(chinaRegionCustomDomain));

    assertEquals(chinaExpectedHostPattern, target.getRdsRegion(oldChinaRegionCluster));
    assertEquals(chinaExpectedHostPattern, target.getRdsRegion(oldChinaRegionClusterReadOnly));
    assertEquals(chinaExpectedHostPattern, target.getRdsRegion(oldChinaRegionInstance));
    assertEquals(chinaExpectedHostPattern, target.getRdsRegion(oldChinaRegionProxy));
    assertEquals(chinaExpectedHostPattern, target.getRdsRegion(oldChinaRegionCustomDomain));
  }

  @Test
  public void testBrokenPathsHostPattern() {
    final String incorrectChinaHostPattern = "?.rds.cn-northwest-1.rds.amazonaws.com.cn";
    assertEquals(incorrectChinaHostPattern, target.getRdsInstanceHostPattern(extraRdsChinaPath));
    assertEquals("?", target.getRdsInstanceHostPattern(missingCnChinaPath));
    assertEquals("?", target.getRdsInstanceHostPattern(missingRegionChinaPath));
  }

  @Test
  public void testBrokenPathsRegion() {
    // Extra rds path returns correct region
    final String chinaExpectedRegion = "cn-northwest-1";
    assertEquals(chinaExpectedRegion, target.getRdsRegion(extraRdsChinaPath));

    assertNull(target.getRdsRegion(missingCnChinaPath));
    assertNull(target.getRdsRegion(missingRegionChinaPath));
  }

  @Test
  public void testBrokenPathsReaderCluster() {
    assertFalse(target.isReaderClusterDns(extraRdsChinaPath));
    assertFalse(target.isReaderClusterDns(missingCnChinaPath));
    assertFalse(target.isReaderClusterDns(missingRegionChinaPath));
  }

  @Test
  public void testBrokenPathsWriterCluster() {
    // Expected to return true with correct cluster paths
    assertFalse(target.isWriterClusterDns(extraRdsChinaPath));
    assertFalse(target.isWriterClusterDns(missingCnChinaPath));
    assertFalse(target.isWriterClusterDns(missingRegionChinaPath));
  }

  @Test
  public void testBrokenPathsRdsDns() {
    // Expected to return true with correct cluster paths
    assertTrue(target.isRdsDns(extraRdsChinaPath));
    assertFalse(target.isRdsDns(missingCnChinaPath));
    assertFalse(target.isRdsDns(missingRegionChinaPath));
  }
}
