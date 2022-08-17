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
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class RdsUtilsTests {

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

  @Test
  public void testIsRdsDns() {
    RdsUtils target = new RdsUtils();

    assertTrue(target.isRdsDns(usEastRegionCluster));
    assertTrue(target.isRdsDns(usEastRegionClusterReadOnly));
    assertTrue(target.isRdsDns(usEastRegionInstance));
    assertTrue(target.isRdsDns(usEastRegionProxy));
    assertTrue(target.isRdsDns(usEastRegionCustomDomain));

    assertTrue(target.isRdsDns(chinaRegionCluster));
    assertTrue(target.isRdsDns(chinaRegionClusterReadOnly));
    assertTrue(target.isRdsDns(chinaRegionInstance));
    assertTrue(target.isRdsDns(chinaRegionProxy));
    assertTrue(target.isRdsDns(chinaRegionCustomDomain));
  }

  @Test
  public void testGetRdsInstanceHostPattern() {
    RdsUtils target = new RdsUtils();

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
  }

  @Test
  public void testIsRdsClusterDns() {
    RdsUtils target = new RdsUtils();

    assertTrue(target.isRdsClusterDns(usEastRegionCluster));
    assertTrue(target.isRdsClusterDns(usEastRegionClusterReadOnly));
    assertFalse(target.isRdsClusterDns(usEastRegionInstance));
    assertFalse(target.isRdsClusterDns(usEastRegionProxy));
    assertFalse(target.isRdsClusterDns(usEastRegionCustomDomain));

    assertTrue(target.isRdsClusterDns(chinaRegionCluster));
    assertTrue(target.isRdsClusterDns(chinaRegionClusterReadOnly));
    assertFalse(target.isRdsClusterDns(chinaRegionInstance));
    assertFalse(target.isRdsClusterDns(chinaRegionProxy));
    assertFalse(target.isRdsClusterDns(chinaRegionCustomDomain));
  }

  @Test
  public void testIsWriterClusterDns() {
    RdsUtils target = new RdsUtils();

    assertTrue(target.isWriterClusterDns(usEastRegionCluster));
    assertFalse(target.isWriterClusterDns(usEastRegionClusterReadOnly));
    assertFalse(target.isWriterClusterDns(usEastRegionInstance));
    assertFalse(target.isWriterClusterDns(usEastRegionProxy));
    assertFalse(target.isWriterClusterDns(usEastRegionCustomDomain));

    assertTrue(target.isWriterClusterDns(chinaRegionCluster));
    assertFalse(target.isWriterClusterDns(chinaRegionClusterReadOnly));
    assertFalse(target.isWriterClusterDns(chinaRegionInstance));
    assertFalse(target.isWriterClusterDns(chinaRegionProxy));
    assertFalse(target.isWriterClusterDns(chinaRegionCustomDomain));
  }

  @Test
  public void testIsReaderClusterDns() {
    RdsUtils target = new RdsUtils();

    assertFalse(target.isReaderClusterDns(usEastRegionCluster));
    assertTrue(target.isReaderClusterDns(usEastRegionClusterReadOnly));
    assertFalse(target.isReaderClusterDns(usEastRegionInstance));
    assertFalse(target.isReaderClusterDns(usEastRegionProxy));
    assertFalse(target.isReaderClusterDns(usEastRegionCustomDomain));

    assertFalse(target.isReaderClusterDns(chinaRegionCluster));
    assertTrue(target.isReaderClusterDns(chinaRegionClusterReadOnly));
    assertFalse(target.isReaderClusterDns(chinaRegionInstance));
    assertFalse(target.isReaderClusterDns(chinaRegionProxy));
    assertFalse(target.isReaderClusterDns(chinaRegionCustomDomain));
  }

  @Test
  public void testGetRdsRegion() {
    RdsUtils target = new RdsUtils();

    final String expectedHostPattern = "us-east-2";
    assertEquals(expectedHostPattern, target.getRdsRegion(usEastRegionCluster));
    assertEquals(expectedHostPattern, target.getRdsRegion(usEastRegionClusterReadOnly));
    assertEquals(expectedHostPattern, target.getRdsRegion(usEastRegionInstance));
    assertEquals(expectedHostPattern, target.getRdsRegion(usEastRegionProxy));
    assertEquals(expectedHostPattern, target.getRdsRegion(usEastRegionCustomDomain));

    final String chinaExpectedHostPattern = "cn-northwest-1";
    assertEquals(chinaExpectedHostPattern, target.getRdsRegion(chinaRegionCluster));
    assertEquals(chinaExpectedHostPattern, target.getRdsRegion(chinaRegionClusterReadOnly));
    assertEquals(chinaExpectedHostPattern, target.getRdsRegion(chinaRegionInstance));
    assertEquals(chinaExpectedHostPattern, target.getRdsRegion(chinaRegionProxy));
    assertEquals(chinaExpectedHostPattern, target.getRdsRegion(chinaRegionCustomDomain));
  }
}
