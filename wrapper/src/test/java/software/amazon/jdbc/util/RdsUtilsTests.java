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
  private static final String usEastRegionClusterTrailingDot =
      "database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com.";
  private static final String usEastRegionClusterReadOnly =
      "database-test-name.cluster-ro-XYZ.us-east-2.rds.amazonaws.com";
  private static final String usEastRegionInstance =
      "instance-test-name.XYZ.us-east-2.rds.amazonaws.com";
  private static final String usEastRegionProxy =
      "proxy-test-name.proxy-XYZ.us-east-2.rds.amazonaws.com";
  private static final String usEastRegionCustomDomain =
      "custom-test-name.cluster-custom-XYZ.us-east-2.rds.amazonaws.com";
  private static final String usEastRegionLimitlessDbShardGroup =
      "database-test-name.shardgrp-XYZ.us-east-2.rds.amazonaws.com";

  private static final String chinaRegionCluster =
      "database-test-name.cluster-XYZ.rds.cn-northwest-1.amazonaws.com.cn";
  private static final String chinaRegionClusterTrailingDot =
      "database-test-name.cluster-XYZ.rds.cn-northwest-1.amazonaws.com.cn.";
  private static final String chinaRegionClusterReadOnly =
      "database-test-name.cluster-ro-XYZ.rds.cn-northwest-1.amazonaws.com.cn";
  private static final String chinaRegionInstance =
      "instance-test-name.XYZ.rds.cn-northwest-1.amazonaws.com.cn";
  private static final String chinaRegionProxy =
      "proxy-test-name.proxy-XYZ.rds.cn-northwest-1.amazonaws.com.cn";
  private static final String chinaRegionCustomDomain =
      "custom-test-name.cluster-custom-XYZ.rds.cn-northwest-1.amazonaws.com.cn";
  private static final String chinaRegionLimitlessDbShardGroup =
      "database-test-name.shardgrp-XYZ.rds.cn-northwest-1.amazonaws.com.cn";

  private static final String oldChinaRegionCluster =
      "database-test-name.cluster-XYZ.cn-northwest-1.rds.amazonaws.com.cn";
  private static final String oldChinaRegionClusterTrailingDot =
      "database-test-name.cluster-XYZ.cn-northwest-1.rds.amazonaws.com.cn.";
  private static final String oldChinaRegionClusterReadOnly =
      "database-test-name.cluster-ro-XYZ.cn-northwest-1.rds.amazonaws.com.cn";
  private static final String oldChinaRegionInstance =
      "instance-test-name.XYZ.cn-northwest-1.rds.amazonaws.com.cn";
  private static final String oldChinaRegionProxy =
      "proxy-test-name.proxy-XYZ.cn-northwest-1.rds.amazonaws.com.cn";
  private static final String oldChinaRegionCustomDomain =
      "custom-test-name.cluster-custom-XYZ.cn-northwest-1.rds.amazonaws.com.cn";
  private static final String oldChinaRegionLimitlessDbShardGroup =
      "database-test-name.shardgrp-XYZ.cn-northwest-1.rds.amazonaws.com.cn";
  private static final String oldChinaRegionLimitlessDbShardGroupTrailingDot =
      "database-test-name.shardgrp-XYZ.cn-northwest-1.rds.amazonaws.com.cn.";

  private static final String extraRdsChinaPath =
      "database-test-name.cluster-XYZ.rds.cn-northwest-1.rds.amazonaws.com.cn";
  private static final String missingCnChinaPath =
      "database-test-name.cluster-XYZ.rds.cn-northwest-1.amazonaws.com";
  private static final String missingRegionChinaPath =
      "database-test-name.cluster-XYZ.rds.amazonaws.com.cn";

  private static final String usEastRegionElbUrl =
      "elb-name.elb.us-east-2.amazonaws.com";
  private static final String usEastRegionElbUrlTrailingDot =
      "elb-name.elb.us-east-2.amazonaws.com.";

  private static final String usIsobEastRegionCluster =
      "database-test-name.cluster-XYZ.rds.us-isob-east-1.sc2s.sgov.gov";
  private static final String usIsobEastRegionClusterReadOnly =
      "database-test-name.cluster-ro-XYZ.rds.us-isob-east-1.sc2s.sgov.gov";
  private static final String usIsobEastRegionInstance =
      "instance-test-name.XYZ.rds.us-isob-east-1.sc2s.sgov.gov";
  private static final String usIsobEastRegionProxy =
      "proxy-test-name.proxy-XYZ.rds.us-isob-east-1.sc2s.sgov.gov";
  private static final String usIsobEastRegionCustomDomain =
      "custom-test-name.cluster-custom-XYZ.rds.us-isob-east-1.sc2s.sgov.gov";
  private static final String usIsobEastRegionLimitlessDbShardGroup =
      "database-test-name.shardgrp-XYZ.rds.us-isob-east-1.sc2s.sgov.gov";

  private static final String usGovEastRegionCluster =
      "database-test-name.cluster-XYZ.rds.us-gov-east-1.amazonaws.com";
  private static final String usIsoEastRegionCluster =
      "database-test-name.cluster-XYZ.rds.us-iso-east-1.c2s.ic.gov";
  private static final String usIsoEastRegionClusterTrailingDot =
      "database-test-name.cluster-XYZ.rds.us-iso-east-1.c2s.ic.gov.";
  private static final String usIsoEastRegionClusterReadOnly =
      "database-test-name.cluster-ro-XYZ.rds.us-iso-east-1.c2s.ic.gov";
  private static final String usIsoEastRegionInstance =
      "instance-test-name.XYZ.rds.us-iso-east-1.c2s.ic.gov";
  private static final String usIsoEastRegionProxy =
      "proxy-test-name.proxy-XYZ.rds.us-iso-east-1.c2s.ic.gov";
  private static final String usIsoEastRegionCustomDomain =
      "custom-test-name.cluster-custom-XYZ.rds.us-iso-east-1.c2s.ic.gov";
  private static final String usIsoEastRegionLimitlessDbShardGroup =
      "database-test-name.shardgrp-XYZ.rds.us-iso-east-1.c2s.ic.gov";

  @BeforeEach
  public void setupTests() {
    RdsUtils.clearCache();
    target = new RdsUtils();
  }

  @Test
  public void testIsRdsDns() {
    assertTrue(target.isRdsDns(usEastRegionCluster));
    assertTrue(target.isRdsDns(usEastRegionClusterTrailingDot));
    assertTrue(target.isRdsDns(usEastRegionClusterReadOnly));
    assertTrue(target.isRdsDns(usEastRegionInstance));
    assertTrue(target.isRdsDns(usEastRegionProxy));
    assertTrue(target.isRdsDns(usEastRegionCustomDomain));
    assertFalse(target.isRdsDns(usEastRegionElbUrl));
    assertFalse(target.isRdsDns(usEastRegionElbUrlTrailingDot));
    assertTrue(target.isRdsDns(usEastRegionLimitlessDbShardGroup));

    assertTrue(target.isRdsDns(chinaRegionCluster));
    assertTrue(target.isRdsDns(chinaRegionClusterTrailingDot));
    assertTrue(target.isRdsDns(chinaRegionClusterReadOnly));
    assertTrue(target.isRdsDns(chinaRegionInstance));
    assertTrue(target.isRdsDns(chinaRegionProxy));
    assertTrue(target.isRdsDns(chinaRegionCustomDomain));
    assertTrue(target.isRdsDns(chinaRegionLimitlessDbShardGroup));

    assertTrue(target.isRdsDns(oldChinaRegionCluster));
    assertTrue(target.isRdsDns(oldChinaRegionClusterTrailingDot));
    assertTrue(target.isRdsDns(oldChinaRegionClusterReadOnly));
    assertTrue(target.isRdsDns(oldChinaRegionInstance));
    assertTrue(target.isRdsDns(oldChinaRegionProxy));
    assertTrue(target.isRdsDns(oldChinaRegionCustomDomain));
    assertTrue(target.isRdsDns(oldChinaRegionLimitlessDbShardGroup));

    assertTrue(target.isRdsDns(usIsobEastRegionCluster));
    assertTrue(target.isRdsDns(usIsobEastRegionClusterReadOnly));
    assertTrue(target.isRdsDns(usIsobEastRegionInstance));
    assertTrue(target.isRdsDns(usIsobEastRegionProxy));
    assertTrue(target.isRdsDns(usIsobEastRegionCustomDomain));
    assertTrue(target.isRdsDns(usIsobEastRegionLimitlessDbShardGroup));

    assertTrue(target.isRdsDns(usIsoEastRegionCluster));
    assertTrue(target.isRdsDns(usIsoEastRegionClusterTrailingDot));
    assertTrue(target.isRdsDns(usIsoEastRegionClusterReadOnly));
    assertTrue(target.isRdsDns(usIsoEastRegionInstance));
    assertTrue(target.isRdsDns(usIsoEastRegionProxy));
    assertTrue(target.isRdsDns(usIsoEastRegionCustomDomain));
    assertTrue(target.isRdsDns(usIsoEastRegionLimitlessDbShardGroup));
  }

  @Test
  public void testGetRdsInstanceHostPattern() {
    final String expectedHostPattern = "?.XYZ.us-east-2.rds.amazonaws.com";
    assertEquals(expectedHostPattern, target.getRdsInstanceHostPattern(usEastRegionCluster));
    assertEquals(expectedHostPattern, target.getRdsInstanceHostPattern(usEastRegionClusterReadOnly));
    assertEquals(expectedHostPattern, target.getRdsInstanceHostPattern(usEastRegionInstance));
    assertEquals(expectedHostPattern, target.getRdsInstanceHostPattern(usEastRegionProxy));
    assertEquals(expectedHostPattern, target.getRdsInstanceHostPattern(usEastRegionCustomDomain));
    assertEquals(expectedHostPattern, target.getRdsInstanceHostPattern(usEastRegionLimitlessDbShardGroup));

    final String govExpectedHostPattern = "?.XYZ.rds.us-gov-east-1.amazonaws.com";
    assertEquals(govExpectedHostPattern, target.getRdsInstanceHostPattern(usGovEastRegionCluster));

    final String isobExpectedHostPattern = "?.XYZ.rds.us-isob-east-1.sc2s.sgov.gov";
    assertEquals(isobExpectedHostPattern, target.getRdsInstanceHostPattern(usIsobEastRegionCluster));
    assertEquals(isobExpectedHostPattern, target.getRdsInstanceHostPattern(usIsobEastRegionClusterReadOnly));
    assertEquals(isobExpectedHostPattern, target.getRdsInstanceHostPattern(usIsobEastRegionInstance));
    assertEquals(isobExpectedHostPattern, target.getRdsInstanceHostPattern(usIsobEastRegionProxy));
    assertEquals(isobExpectedHostPattern, target.getRdsInstanceHostPattern(usIsobEastRegionCustomDomain));
    assertEquals(isobExpectedHostPattern, target.getRdsInstanceHostPattern(usIsobEastRegionLimitlessDbShardGroup));

    final String isoExpectedHostPattern = "?.XYZ.rds.us-iso-east-1.c2s.ic.gov";
    assertEquals(isoExpectedHostPattern, target.getRdsInstanceHostPattern(usIsoEastRegionCluster));
    assertEquals(isoExpectedHostPattern, target.getRdsInstanceHostPattern(usIsoEastRegionClusterReadOnly));
    assertEquals(isoExpectedHostPattern, target.getRdsInstanceHostPattern(usIsoEastRegionInstance));
    assertEquals(isoExpectedHostPattern, target.getRdsInstanceHostPattern(usIsoEastRegionProxy));
    assertEquals(isoExpectedHostPattern, target.getRdsInstanceHostPattern(usIsoEastRegionCustomDomain));
    assertEquals(isoExpectedHostPattern, target.getRdsInstanceHostPattern(usIsoEastRegionLimitlessDbShardGroup));

    final String chinaExpectedHostPattern = "?.XYZ.rds.cn-northwest-1.amazonaws.com.cn";
    assertEquals(chinaExpectedHostPattern, target.getRdsInstanceHostPattern(chinaRegionCluster));
    assertEquals(chinaExpectedHostPattern, target.getRdsInstanceHostPattern(chinaRegionClusterReadOnly));
    assertEquals(chinaExpectedHostPattern, target.getRdsInstanceHostPattern(chinaRegionInstance));
    assertEquals(chinaExpectedHostPattern, target.getRdsInstanceHostPattern(chinaRegionProxy));
    assertEquals(chinaExpectedHostPattern, target.getRdsInstanceHostPattern(chinaRegionCustomDomain));
    assertEquals(chinaExpectedHostPattern, target.getRdsInstanceHostPattern(chinaRegionLimitlessDbShardGroup));

    final String oldChinaExpectedHostPattern = "?.XYZ.cn-northwest-1.rds.amazonaws.com.cn";
    assertEquals(oldChinaExpectedHostPattern, target.getRdsInstanceHostPattern(oldChinaRegionCluster));
    assertEquals(oldChinaExpectedHostPattern, target.getRdsInstanceHostPattern(oldChinaRegionClusterReadOnly));
    assertEquals(oldChinaExpectedHostPattern, target.getRdsInstanceHostPattern(oldChinaRegionInstance));
    assertEquals(oldChinaExpectedHostPattern, target.getRdsInstanceHostPattern(oldChinaRegionProxy));
    assertEquals(oldChinaExpectedHostPattern, target.getRdsInstanceHostPattern(oldChinaRegionCustomDomain));
    assertEquals(oldChinaExpectedHostPattern, target.getRdsInstanceHostPattern(oldChinaRegionLimitlessDbShardGroup));
  }

  @Test
  public void testIsRdsClusterDns() {
    assertTrue(target.isRdsClusterDns(usEastRegionCluster));
    assertTrue(target.isRdsClusterDns(usEastRegionClusterReadOnly));
    assertFalse(target.isRdsClusterDns(usEastRegionInstance));
    assertFalse(target.isRdsClusterDns(usEastRegionProxy));
    assertFalse(target.isRdsClusterDns(usEastRegionCustomDomain));
    assertFalse(target.isRdsClusterDns(usEastRegionElbUrl));
    assertFalse(target.isRdsClusterDns(usEastRegionLimitlessDbShardGroup));

    assertTrue(target.isRdsClusterDns(usIsobEastRegionCluster));
    assertTrue(target.isRdsClusterDns(usIsobEastRegionClusterReadOnly));
    assertFalse(target.isRdsClusterDns(usIsobEastRegionInstance));
    assertFalse(target.isRdsClusterDns(usIsobEastRegionProxy));
    assertFalse(target.isRdsClusterDns(usIsobEastRegionCustomDomain));
    assertFalse(target.isRdsClusterDns(usIsobEastRegionLimitlessDbShardGroup));

    assertTrue(target.isRdsClusterDns(usIsoEastRegionCluster));
    assertTrue(target.isRdsClusterDns(usIsoEastRegionClusterReadOnly));
    assertFalse(target.isRdsClusterDns(usIsoEastRegionInstance));
    assertFalse(target.isRdsClusterDns(usIsoEastRegionProxy));
    assertFalse(target.isRdsClusterDns(usIsoEastRegionCustomDomain));
    assertFalse(target.isRdsClusterDns(usIsoEastRegionLimitlessDbShardGroup));

    assertTrue(target.isRdsClusterDns(chinaRegionCluster));
    assertTrue(target.isRdsClusterDns(chinaRegionClusterReadOnly));
    assertFalse(target.isRdsClusterDns(chinaRegionInstance));
    assertFalse(target.isRdsClusterDns(chinaRegionProxy));
    assertFalse(target.isRdsClusterDns(chinaRegionCustomDomain));
    assertFalse(target.isRdsClusterDns(chinaRegionLimitlessDbShardGroup));

    assertTrue(target.isRdsClusterDns(oldChinaRegionCluster));
    assertTrue(target.isRdsClusterDns(oldChinaRegionClusterReadOnly));
    assertFalse(target.isRdsClusterDns(oldChinaRegionInstance));
    assertFalse(target.isRdsClusterDns(oldChinaRegionProxy));
    assertFalse(target.isRdsClusterDns(oldChinaRegionCustomDomain));
    assertFalse(target.isRdsClusterDns(oldChinaRegionLimitlessDbShardGroup));
  }

  @Test
  public void testIsWriterClusterDns() {
    assertTrue(target.isWriterClusterDns(usEastRegionCluster));
    assertFalse(target.isWriterClusterDns(usEastRegionClusterReadOnly));
    assertFalse(target.isWriterClusterDns(usEastRegionInstance));
    assertFalse(target.isWriterClusterDns(usEastRegionProxy));
    assertFalse(target.isWriterClusterDns(usEastRegionCustomDomain));
    assertFalse(target.isWriterClusterDns(usEastRegionElbUrl));
    assertFalse(target.isWriterClusterDns(usEastRegionLimitlessDbShardGroup));

    assertTrue(target.isWriterClusterDns(usIsobEastRegionCluster));
    assertFalse(target.isWriterClusterDns(usIsobEastRegionClusterReadOnly));
    assertFalse(target.isWriterClusterDns(usIsobEastRegionInstance));
    assertFalse(target.isWriterClusterDns(usIsobEastRegionProxy));
    assertFalse(target.isWriterClusterDns(usIsobEastRegionCustomDomain));
    assertFalse(target.isWriterClusterDns(usIsobEastRegionLimitlessDbShardGroup));

    assertTrue(target.isWriterClusterDns(usIsoEastRegionCluster));
    assertFalse(target.isWriterClusterDns(usIsoEastRegionClusterReadOnly));
    assertFalse(target.isWriterClusterDns(usIsoEastRegionInstance));
    assertFalse(target.isWriterClusterDns(usIsoEastRegionProxy));
    assertFalse(target.isWriterClusterDns(usIsoEastRegionCustomDomain));
    assertFalse(target.isWriterClusterDns(usIsoEastRegionLimitlessDbShardGroup));

    assertTrue(target.isWriterClusterDns(chinaRegionCluster));
    assertFalse(target.isWriterClusterDns(chinaRegionClusterReadOnly));
    assertFalse(target.isWriterClusterDns(chinaRegionInstance));
    assertFalse(target.isWriterClusterDns(chinaRegionProxy));
    assertFalse(target.isWriterClusterDns(chinaRegionCustomDomain));
    assertFalse(target.isWriterClusterDns(chinaRegionLimitlessDbShardGroup));

    assertTrue(target.isWriterClusterDns(oldChinaRegionCluster));
    assertFalse(target.isWriterClusterDns(oldChinaRegionClusterReadOnly));
    assertFalse(target.isWriterClusterDns(oldChinaRegionInstance));
    assertFalse(target.isWriterClusterDns(oldChinaRegionProxy));
    assertFalse(target.isWriterClusterDns(oldChinaRegionCustomDomain));
    assertFalse(target.isWriterClusterDns(oldChinaRegionLimitlessDbShardGroup));
  }

  @Test
  public void testIsReaderClusterDns() {
    assertFalse(target.isReaderClusterDns(usEastRegionCluster));
    assertTrue(target.isReaderClusterDns(usEastRegionClusterReadOnly));
    assertFalse(target.isReaderClusterDns(usEastRegionInstance));
    assertFalse(target.isReaderClusterDns(usEastRegionProxy));
    assertFalse(target.isReaderClusterDns(usEastRegionCustomDomain));
    assertFalse(target.isReaderClusterDns(usEastRegionElbUrl));
    assertFalse(target.isReaderClusterDns(usEastRegionLimitlessDbShardGroup));

    assertFalse(target.isReaderClusterDns(usIsobEastRegionCluster));
    assertTrue(target.isReaderClusterDns(usIsobEastRegionClusterReadOnly));
    assertFalse(target.isReaderClusterDns(usIsobEastRegionInstance));
    assertFalse(target.isReaderClusterDns(usIsobEastRegionProxy));
    assertFalse(target.isReaderClusterDns(usIsobEastRegionCustomDomain));
    assertFalse(target.isReaderClusterDns(usIsobEastRegionLimitlessDbShardGroup));

    assertFalse(target.isReaderClusterDns(usIsoEastRegionCluster));
    assertTrue(target.isReaderClusterDns(usIsoEastRegionClusterReadOnly));
    assertFalse(target.isReaderClusterDns(usIsoEastRegionInstance));
    assertFalse(target.isReaderClusterDns(usIsoEastRegionProxy));
    assertFalse(target.isReaderClusterDns(usIsoEastRegionCustomDomain));
    assertFalse(target.isReaderClusterDns(usIsoEastRegionLimitlessDbShardGroup));

    assertFalse(target.isReaderClusterDns(chinaRegionCluster));
    assertTrue(target.isReaderClusterDns(chinaRegionClusterReadOnly));
    assertFalse(target.isReaderClusterDns(chinaRegionInstance));
    assertFalse(target.isReaderClusterDns(chinaRegionProxy));
    assertFalse(target.isReaderClusterDns(chinaRegionCustomDomain));
    assertFalse(target.isReaderClusterDns(chinaRegionLimitlessDbShardGroup));

    assertFalse(target.isReaderClusterDns(oldChinaRegionCluster));
    assertTrue(target.isReaderClusterDns(oldChinaRegionClusterReadOnly));
    assertFalse(target.isReaderClusterDns(oldChinaRegionInstance));
    assertFalse(target.isReaderClusterDns(oldChinaRegionProxy));
    assertFalse(target.isReaderClusterDns(oldChinaRegionCustomDomain));
    assertFalse(target.isReaderClusterDns(oldChinaRegionLimitlessDbShardGroup));
  }

  @Test
  public void testIsLimitlessDbShardGroupDns() {
    assertFalse(target.isLimitlessDbShardGroupDns(usEastRegionCluster));
    assertFalse(target.isLimitlessDbShardGroupDns(usEastRegionClusterReadOnly));
    assertFalse(target.isLimitlessDbShardGroupDns(usEastRegionInstance));
    assertFalse(target.isLimitlessDbShardGroupDns(usEastRegionProxy));
    assertFalse(target.isLimitlessDbShardGroupDns(usEastRegionCustomDomain));
    assertFalse(target.isLimitlessDbShardGroupDns(usEastRegionElbUrl));
    assertTrue(target.isLimitlessDbShardGroupDns(usEastRegionLimitlessDbShardGroup));

    assertFalse(target.isLimitlessDbShardGroupDns(usIsobEastRegionCluster));
    assertFalse(target.isLimitlessDbShardGroupDns(usIsobEastRegionClusterReadOnly));
    assertFalse(target.isLimitlessDbShardGroupDns(usIsobEastRegionInstance));
    assertFalse(target.isLimitlessDbShardGroupDns(usIsobEastRegionProxy));
    assertFalse(target.isLimitlessDbShardGroupDns(usIsobEastRegionCustomDomain));
    assertTrue(target.isLimitlessDbShardGroupDns(usIsobEastRegionLimitlessDbShardGroup));

    assertFalse(target.isLimitlessDbShardGroupDns(usIsoEastRegionCluster));
    assertFalse(target.isLimitlessDbShardGroupDns(usIsoEastRegionClusterReadOnly));
    assertFalse(target.isLimitlessDbShardGroupDns(usIsoEastRegionInstance));
    assertFalse(target.isLimitlessDbShardGroupDns(usIsoEastRegionProxy));
    assertFalse(target.isLimitlessDbShardGroupDns(usIsoEastRegionCustomDomain));
    assertTrue(target.isLimitlessDbShardGroupDns(usIsoEastRegionLimitlessDbShardGroup));

    assertFalse(target.isLimitlessDbShardGroupDns(chinaRegionCluster));
    assertFalse(target.isLimitlessDbShardGroupDns(chinaRegionClusterReadOnly));
    assertFalse(target.isLimitlessDbShardGroupDns(chinaRegionInstance));
    assertFalse(target.isLimitlessDbShardGroupDns(chinaRegionProxy));
    assertFalse(target.isLimitlessDbShardGroupDns(chinaRegionCustomDomain));
    assertTrue(target.isLimitlessDbShardGroupDns(chinaRegionLimitlessDbShardGroup));

    assertFalse(target.isLimitlessDbShardGroupDns(oldChinaRegionCluster));
    assertFalse(target.isLimitlessDbShardGroupDns(oldChinaRegionClusterReadOnly));
    assertFalse(target.isLimitlessDbShardGroupDns(oldChinaRegionInstance));
    assertFalse(target.isLimitlessDbShardGroupDns(oldChinaRegionProxy));
    assertFalse(target.isLimitlessDbShardGroupDns(oldChinaRegionCustomDomain));
    assertTrue(target.isLimitlessDbShardGroupDns(oldChinaRegionLimitlessDbShardGroup));
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
    assertEquals(expectedHostPattern, target.getRdsRegion(usEastRegionLimitlessDbShardGroup));

    final String govExpectedHostPattern = "us-gov-east-1";
    assertEquals(govExpectedHostPattern, target.getRdsRegion(usGovEastRegionCluster));

    final String isobExpectedHostPattern = "us-isob-east-1";
    assertEquals(isobExpectedHostPattern, target.getRdsRegion(usIsobEastRegionCluster));
    assertEquals(isobExpectedHostPattern, target.getRdsRegion(usIsobEastRegionClusterReadOnly));
    assertEquals(isobExpectedHostPattern, target.getRdsRegion(usIsobEastRegionInstance));
    assertEquals(isobExpectedHostPattern, target.getRdsRegion(usIsobEastRegionProxy));
    assertEquals(isobExpectedHostPattern, target.getRdsRegion(usIsobEastRegionCustomDomain));
    assertEquals(isobExpectedHostPattern, target.getRdsRegion(usIsobEastRegionLimitlessDbShardGroup));

    final String isoExpectedHostPattern = "us-iso-east-1";
    assertEquals(isoExpectedHostPattern, target.getRdsRegion(usIsoEastRegionCluster));
    assertEquals(isoExpectedHostPattern, target.getRdsRegion(usIsoEastRegionClusterReadOnly));
    assertEquals(isoExpectedHostPattern, target.getRdsRegion(usIsoEastRegionInstance));
    assertEquals(isoExpectedHostPattern, target.getRdsRegion(usIsoEastRegionProxy));
    assertEquals(isoExpectedHostPattern, target.getRdsRegion(usIsoEastRegionCustomDomain));
    assertEquals(isoExpectedHostPattern, target.getRdsRegion(usIsoEastRegionLimitlessDbShardGroup));

    final String chinaExpectedHostPattern = "cn-northwest-1";
    assertEquals(chinaExpectedHostPattern, target.getRdsRegion(chinaRegionCluster));
    assertEquals(chinaExpectedHostPattern, target.getRdsRegion(chinaRegionClusterReadOnly));
    assertEquals(chinaExpectedHostPattern, target.getRdsRegion(chinaRegionInstance));
    assertEquals(chinaExpectedHostPattern, target.getRdsRegion(chinaRegionProxy));
    assertEquals(chinaExpectedHostPattern, target.getRdsRegion(chinaRegionCustomDomain));
    assertEquals(chinaExpectedHostPattern, target.getRdsRegion(chinaRegionLimitlessDbShardGroup));

    assertEquals(chinaExpectedHostPattern, target.getRdsRegion(oldChinaRegionCluster));
    assertEquals(chinaExpectedHostPattern, target.getRdsRegion(oldChinaRegionClusterReadOnly));
    assertEquals(chinaExpectedHostPattern, target.getRdsRegion(oldChinaRegionInstance));
    assertEquals(chinaExpectedHostPattern, target.getRdsRegion(oldChinaRegionProxy));
    assertEquals(chinaExpectedHostPattern, target.getRdsRegion(oldChinaRegionCustomDomain));
    assertEquals(chinaExpectedHostPattern, target.getRdsRegion(oldChinaRegionLimitlessDbShardGroup));
  }

  @Test
  public void testBrokenPathsHostPattern() {
    final String incorrectChinaHostPattern = "?.rds.cn-northwest-1.rds.amazonaws.com.cn";
    assertEquals(incorrectChinaHostPattern, target.getRdsInstanceHostPattern(extraRdsChinaPath));
    assertEquals("?", target.getRdsInstanceHostPattern(missingRegionChinaPath));
  }

  @Test
  public void testBrokenPathsRegion() {
    // Extra rds path returns correct region
    final String chinaExpectedRegion = "cn-northwest-1";
    assertEquals(chinaExpectedRegion, target.getRdsRegion(extraRdsChinaPath));

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
    assertFalse(target.isWriterClusterDns(missingRegionChinaPath));
  }

  @Test
  public void testBrokenPathsRdsDns() {
    // Expected to return true with correct cluster paths
    assertTrue(target.isRdsDns(extraRdsChinaPath));
    assertFalse(target.isRdsDns(missingRegionChinaPath));
  }

  @Test
  public void testGreenInstanceHostName() {
    assertFalse(target.isGreenInstance("test-instance"));
    assertFalse(target.isGreenInstance("test-instance-green-12345"));
    assertFalse(target.isGreenInstance("test-instance-green-123456"));
    assertFalse(target.isGreenInstance("test-instance.domain.com"));
    assertFalse(target.isGreenInstance("test-instance-green.domain.com"));
    assertFalse(target.isGreenInstance("test-instance-green-1.domain.com"));
    assertFalse(target.isGreenInstance("test-instance-green-12345.domain.com"));
    assertTrue(target.isGreenInstance("test-instance-green-abcdef.domain.com"));
    assertFalse(target.isGreenInstance("test-instance-green-abcdef-.domain.com"));
    assertFalse(target.isGreenInstance("test-instance-green-abcdef-12345.domain.com"));
    assertFalse(target.isGreenInstance("test-instance-green-abcdef-12345-green.domain.com"));
    assertFalse(target.isGreenInstance("test-instance-green-abcdef-12345-green-00000.domain.com"));
    assertTrue(target.isGreenInstance("test-instance-green-abcdef-12345-green-000000.domain.com"));
  }

  @Test
  public void testNoPrefixInstanceHostName() {
    assertTrue(target.isNoPrefixInstance("test-instance"));
    assertTrue(target.isNoPrefixInstance("test-instance.domain.com"));
    assertTrue(target.isNoPrefixInstance("test-instance-green.domain.com"));
    assertTrue(target.isNoPrefixInstance("test-instance-green-1.domain.com"));
    assertTrue(target.isNoPrefixInstance("test-instance-green-12345.domain.com"));
    assertFalse(target.isNoPrefixInstance("test-instance-green-abcdef.domain.com"));
    assertTrue(target.isNoPrefixInstance("test-instance-green-abcdef-.domain.com"));
    assertTrue(target.isNoPrefixInstance("test-instance-green-abcdef-12345.domain.com"));
    assertTrue(target.isNoPrefixInstance("test-instance-green-abcdef-12345-green.domain.com"));
    assertTrue(target.isNoPrefixInstance("test-instance-green-abcdef-12345-green-00000.domain.com"));
    assertFalse(target.isNoPrefixInstance("test-instance-green-abcdef-12345-green-000000.domain.com"));
  }

  @Test
  public void testRemoveGreenInstancePrefix() {
    assertNull(target.removeGreenInstancePrefix(null));
    assertEquals("", target.removeGreenInstancePrefix(""));
    assertEquals("test-instance",
        target.removeGreenInstancePrefix("test-instance-green-abcdef"));
    assertEquals("test-instance",
        target.removeGreenInstancePrefix("test-instance"));
    assertEquals("test-instance.domain.com",
        target.removeGreenInstancePrefix("test-instance.domain.com"));
    assertEquals("test-instance-green.domain.com",
        target.removeGreenInstancePrefix("test-instance-green.domain.com"));
    assertEquals("test-instance-green-1.domain.com",
        target.removeGreenInstancePrefix("test-instance-green-1.domain.com"));
    assertEquals("test-instance-green-1234.domain.com",
        target.removeGreenInstancePrefix("test-instance-green-1234.domain.com"));
    assertEquals("test-instance-green-12345.domain.com",
        target.removeGreenInstancePrefix("test-instance-green-12345.domain.com"));
    assertEquals("test-instance.domain.com",
        target.removeGreenInstancePrefix("test-instance-green-123456.domain.com"));
    assertEquals("test-instance.domain.com",
        target.removeGreenInstancePrefix("test-instance-green-abcdef.domain.com"));
    assertEquals("test-instance-green-abcdef-.domain.com",
        target.removeGreenInstancePrefix("test-instance-green-abcdef-.domain.com"));
    assertEquals("test-instance-green-abcdef-12345.domain.com",
        target.removeGreenInstancePrefix("test-instance-green-abcdef-12345.domain.com"));
    assertEquals("test-instance-green-abcdef-12345-green.domain.com",
        target.removeGreenInstancePrefix("test-instance-green-abcdef-12345-green.domain.com"));
    assertEquals("test-instance-green-abcdef-12345-green-0000.domain.com",
        target.removeGreenInstancePrefix("test-instance-green-abcdef-12345-green-0000.domain.com"));
    assertEquals("test-instance-green-abcdef-12345-green-00000.domain.com",
        target.removeGreenInstancePrefix("test-instance-green-abcdef-12345-green-00000.domain.com"));
    assertEquals("test-instance-green-abcdef-12345.domain.com",
        target.removeGreenInstancePrefix("test-instance-green-abcdef-12345-green-000000.domain.com"));
    assertEquals("test-instance-green-123456.domain.com",
        target.removeGreenInstancePrefix("test-instance-green-123456-green-123456.domain.com"));
  }

  @Test
  public void testGetRdsClusterId() {
    assertEquals("database-test-name", target.getRdsClusterId(usEastRegionCluster));
    assertEquals("database-test-name", target.getRdsClusterId(usEastRegionClusterReadOnly));
    assertNull(target.getRdsClusterId(usEastRegionInstance));
    assertEquals("proxy-test-name", target.getRdsClusterId(usEastRegionProxy));
    assertEquals("custom-test-name", target.getRdsClusterId(usEastRegionCustomDomain));
    assertEquals("database-test-name", target.getRdsClusterId(usEastRegionLimitlessDbShardGroup));

    assertEquals("database-test-name", target.getRdsClusterId(chinaRegionCluster));
    assertEquals("database-test-name", target.getRdsClusterId(chinaRegionClusterReadOnly));
    assertNull(target.getRdsClusterId(chinaRegionInstance));
    assertEquals("proxy-test-name", target.getRdsClusterId(chinaRegionProxy));
    assertEquals("custom-test-name", target.getRdsClusterId(chinaRegionCustomDomain));
    assertEquals("database-test-name", target.getRdsClusterId(chinaRegionLimitlessDbShardGroup));

    assertEquals("database-test-name", target.getRdsClusterId(oldChinaRegionCluster));
    assertEquals("database-test-name", target.getRdsClusterId(oldChinaRegionClusterReadOnly));
    assertNull(target.getRdsClusterId(oldChinaRegionInstance));
    assertEquals("proxy-test-name", target.getRdsClusterId(oldChinaRegionProxy));
    assertEquals("custom-test-name", target.getRdsClusterId(oldChinaRegionCustomDomain));
    assertEquals("database-test-name", target.getRdsClusterId(oldChinaRegionLimitlessDbShardGroup));

    assertEquals("database-test-name", target.getRdsClusterId(usIsobEastRegionCluster));
    assertEquals("database-test-name", target.getRdsClusterId(usIsobEastRegionClusterReadOnly));
    assertNull(target.getRdsClusterId(usIsobEastRegionInstance));
    assertEquals("proxy-test-name", target.getRdsClusterId(usIsobEastRegionProxy));
    assertEquals("custom-test-name", target.getRdsClusterId(usIsobEastRegionCustomDomain));
    assertEquals("database-test-name", target.getRdsClusterId(usIsobEastRegionLimitlessDbShardGroup));

    assertEquals("database-test-name", target.getRdsClusterId(usGovEastRegionCluster));
    assertEquals("database-test-name", target.getRdsClusterId(usIsoEastRegionCluster));
    assertEquals("database-test-name", target.getRdsClusterId(usIsoEastRegionClusterReadOnly));
    assertNull(target.getRdsClusterId(usIsoEastRegionInstance));
    assertEquals("proxy-test-name", target.getRdsClusterId(usIsoEastRegionProxy));
    assertEquals("custom-test-name", target.getRdsClusterId(usIsoEastRegionCustomDomain));
    assertEquals("database-test-name", target.getRdsClusterId(usIsoEastRegionLimitlessDbShardGroup));
  }

  @Test
  public void testPrepareHostFunction() {

    final String prefix = ".proxied";

    assertFalse(target.isRdsDns(usEastRegionCluster + prefix));
    assertFalse(target.isRdsClusterDns(usEastRegionCluster + prefix));
    assertFalse(target.isWriterClusterDns(usEastRegionCluster + prefix));
    assertFalse(target.isReaderClusterDns(usEastRegionClusterReadOnly + prefix));
    assertFalse(target.isLimitlessDbShardGroupDns(usEastRegionLimitlessDbShardGroup + prefix));
    assertNull(target.getRdsClusterHostUrl(usEastRegionCluster + prefix));
    assertNull(target.getRdsClusterHostUrl(usEastRegionClusterTrailingDot + prefix));
    assertNull(target.getRdsClusterHostUrl(chinaRegionClusterTrailingDot + prefix));
    assertNull(target.getRdsClusterHostUrl(oldChinaRegionClusterTrailingDot + prefix));
    assertNull(target.getRdsClusterHostUrl(oldChinaRegionLimitlessDbShardGroupTrailingDot + prefix));
    assertNull(target.getRdsClusterHostUrl(usIsoEastRegionClusterTrailingDot + prefix));
    assertEquals("?", target.getRdsInstanceHostPattern(usEastRegionCluster + prefix));
    assertNull(target.getRdsRegion(usEastRegionCluster + prefix));

    RdsUtils.setPrepareHostFunc((host) -> {
      if (host.endsWith(prefix)) {
        return host.substring(0, host.length() - prefix.length()); // removes prefix at the end of host
      }
      return host;
    });

    assertTrue(target.isRdsDns(usEastRegionCluster + prefix));
    assertTrue(target.isRdsClusterDns(usEastRegionCluster + prefix));
    assertTrue(target.isWriterClusterDns(usEastRegionCluster + prefix));
    assertTrue(target.isReaderClusterDns(usEastRegionClusterReadOnly + prefix));
    assertTrue(target.isLimitlessDbShardGroupDns(usEastRegionLimitlessDbShardGroup + prefix));
    assertEquals(usEastRegionCluster, target.getRdsClusterHostUrl(usEastRegionCluster + prefix));
    assertEquals(usEastRegionClusterTrailingDot, target.getRdsClusterHostUrl(usEastRegionClusterTrailingDot + prefix));
    assertEquals(chinaRegionClusterTrailingDot, target.getRdsClusterHostUrl(chinaRegionClusterTrailingDot + prefix));
    assertEquals(
        oldChinaRegionClusterTrailingDot, target.getRdsClusterHostUrl(oldChinaRegionClusterTrailingDot + prefix));
    assertEquals(
        oldChinaRegionLimitlessDbShardGroupTrailingDot,
        target.getRdsClusterHostUrl(oldChinaRegionLimitlessDbShardGroupTrailingDot + prefix));
    assertEquals(
        usIsoEastRegionClusterTrailingDot, target.getRdsClusterHostUrl(usIsoEastRegionClusterTrailingDot + prefix));
    assertEquals(
        "?.XYZ.us-east-2.rds.amazonaws.com", target.getRdsInstanceHostPattern(usEastRegionCluster + prefix));
    assertEquals("us-east-2", target.getRdsRegion(usEastRegionCluster + prefix));

    RdsUtils.resetPrepareHostFunc();
    assertFalse(target.isRdsDns(usEastRegionCluster + prefix));
    assertFalse(target.isRdsClusterDns(usEastRegionCluster + prefix));
    assertFalse(target.isWriterClusterDns(usEastRegionCluster + prefix));
    assertFalse(target.isReaderClusterDns(usEastRegionClusterReadOnly + prefix));
    assertFalse(target.isLimitlessDbShardGroupDns(usEastRegionLimitlessDbShardGroup + prefix));
    assertNull(target.getRdsClusterHostUrl(usEastRegionCluster + prefix));
    assertNull(target.getRdsClusterHostUrl(usEastRegionClusterTrailingDot + prefix));
    assertNull(target.getRdsClusterHostUrl(chinaRegionClusterTrailingDot + prefix));
    assertNull(target.getRdsClusterHostUrl(oldChinaRegionClusterTrailingDot + prefix));
    assertNull(target.getRdsClusterHostUrl(oldChinaRegionLimitlessDbShardGroupTrailingDot + prefix));
    assertNull(target.getRdsClusterHostUrl(usIsoEastRegionClusterTrailingDot + prefix));
    assertEquals("?", target.getRdsInstanceHostPattern(usEastRegionCluster + prefix));
    assertNull(target.getRdsRegion(usEastRegionCluster + prefix));
  }
}
