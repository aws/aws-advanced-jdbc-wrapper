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

package software.amazon.jdbc.hostlistprovider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.util.RdsUtils;

class GdbMonitoringConnectionPriorityTest {

  private final RdsUtils rdsUtils = new RdsUtils();

  private HostSpec createHost(String host, HostRole role) {
    return new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host(host)
        .role(role)
        .build();
  }

  @Test
  void testFromValueStrictWriterPrimary() {
    GdbMonitoringConnectionPriority p = GdbMonitoringConnectionPriority.fromValue("strict-writer-primary");
    assertNotNull(p);
    assertEquals(HostRole.WRITER, p.getRequiredRole());
    assertNull(p.getRequiredRegion());
    assertTrue(p.isRequirePrimary());
    assertFalse(p.isRequireSecondary());
  }

  @Test
  void testFromValueStrictReaderPrimary() {
    GdbMonitoringConnectionPriority p = GdbMonitoringConnectionPriority.fromValue("strict-reader-primary");
    assertNotNull(p);
    assertEquals(HostRole.READER, p.getRequiredRole());
    assertNull(p.getRequiredRegion());
    assertTrue(p.isRequirePrimary());
    assertFalse(p.isRequireSecondary());
  }

  @Test
  void testFromValueStrictReaderSecondary() {
    GdbMonitoringConnectionPriority p = GdbMonitoringConnectionPriority.fromValue("strict-reader-secondary");
    assertNotNull(p);
    assertEquals(HostRole.READER, p.getRequiredRole());
    assertNull(p.getRequiredRegion());
    assertFalse(p.isRequirePrimary());
    assertTrue(p.isRequireSecondary());
  }

  @Test
  void testFromValueStrictWriterRegion() {
    GdbMonitoringConnectionPriority p = GdbMonitoringConnectionPriority.fromValue("strict-writer-us-east-1");
    assertNotNull(p);
    assertEquals(HostRole.WRITER, p.getRequiredRole());
    assertEquals("us-east-1", p.getRequiredRegion());
    assertFalse(p.isRequirePrimary());
    assertFalse(p.isRequireSecondary());
  }

  @Test
  void testFromValueStrictReaderRegion() {
    GdbMonitoringConnectionPriority p = GdbMonitoringConnectionPriority.fromValue("strict-reader-us-west-2");
    assertNotNull(p);
    assertEquals(HostRole.READER, p.getRequiredRole());
    assertEquals("us-west-2", p.getRequiredRegion());
    assertFalse(p.isRequirePrimary());
    assertFalse(p.isRequireSecondary());
  }

  @Test
  void testFromValuePlainRegion() {
    GdbMonitoringConnectionPriority p = GdbMonitoringConnectionPriority.fromValue("us-east-1");
    assertNotNull(p);
    assertNull(p.getRequiredRole());
    assertEquals("us-east-1", p.getRequiredRegion());
    assertFalse(p.isRequirePrimary());
    assertFalse(p.isRequireSecondary());
  }

  @Test
  void testFromValueNull() {
    assertNull(GdbMonitoringConnectionPriority.fromValue(null));
    assertNull(GdbMonitoringConnectionPriority.fromValue(""));
    assertNull(GdbMonitoringConnectionPriority.fromValue("  "));
  }

  @Test
  void testFromValueInvalidPrefix() {
    // "strict-writer-" with no suffix
    assertNull(GdbMonitoringConnectionPriority.fromValue("strict-writer-"));
    assertNull(GdbMonitoringConnectionPriority.fromValue("strict-reader-"));
  }

  @Test
  void testFromValueStrictWriterSecondaryRejected() {
    // A writer in a secondary region is impossible for an Aurora Global Database (only the primary region
    // has a writer). It must be rejected rather than treated as a region literal named "secondary".
    assertNull(GdbMonitoringConnectionPriority.fromValue("strict-writer-secondary"));
  }

  @Test
  void testParseListDefault() {
    List<GdbMonitoringConnectionPriority> result = GdbMonitoringConnectionPriority.parseList(null);
    assertEquals(1, result.size());
    assertEquals("strict-writer-primary", result.get(0).toString());
  }

  @Test
  void testParseListMultipleValues() {
    List<GdbMonitoringConnectionPriority> result =
        GdbMonitoringConnectionPriority.parseList("strict-writer-primary,strict-reader-us-east-1,us-west-2");
    assertEquals(3, result.size());
    assertEquals("strict-writer-primary", result.get(0).toString());
    assertEquals("strict-reader-us-east-1", result.get(1).toString());
    assertEquals("us-west-2", result.get(2).toString());
  }

  @Test
  void testParseListSkipsInvalid() {
    List<GdbMonitoringConnectionPriority> result =
        GdbMonitoringConnectionPriority.parseList("strict-writer-,strict-reader-primary");
    assertEquals(1, result.size());
    assertEquals("strict-reader-primary", result.get(0).toString());
  }

  @Test
  void testIsSatisfiedByWriterInPrimaryRegion() {
    GdbMonitoringConnectionPriority p = GdbMonitoringConnectionPriority.fromValue("strict-writer-primary");
    HostSpec writer = createHost("instance1.cluster-xyz.us-east-1.rds.amazonaws.com", HostRole.WRITER);
    assertTrue(p.isSatisfiedBy(writer, "us-east-1", rdsUtils));
    assertFalse(p.isSatisfiedBy(writer, "us-west-2", rdsUtils));
  }

  @Test
  void testIsSatisfiedByReaderInPrimaryRegion() {
    GdbMonitoringConnectionPriority p = GdbMonitoringConnectionPriority.fromValue("strict-reader-primary");
    HostSpec reader = createHost("instance2.cluster-ro-xyz.us-east-1.rds.amazonaws.com", HostRole.READER);
    assertTrue(p.isSatisfiedBy(reader, "us-east-1", rdsUtils));
    assertFalse(p.isSatisfiedBy(reader, "us-west-2", rdsUtils));
  }

  @Test
  void testIsSatisfiedByReaderInSecondaryRegion() {
    GdbMonitoringConnectionPriority p = GdbMonitoringConnectionPriority.fromValue("strict-reader-secondary");
    HostSpec reader = createHost("instance3.cluster-ro-xyz.us-west-2.rds.amazonaws.com", HostRole.READER);
    // Primary is us-east-1, so us-west-2 is secondary
    assertTrue(p.isSatisfiedBy(reader, "us-east-1", rdsUtils));
    // Primary is us-west-2, so us-west-2 is NOT secondary
    assertFalse(p.isSatisfiedBy(reader, "us-west-2", rdsUtils));
  }

  @Test
  void testIsSatisfiedByWriterInSpecificRegion() {
    GdbMonitoringConnectionPriority p = GdbMonitoringConnectionPriority.fromValue("strict-writer-us-east-1");
    HostSpec writer = createHost("instance1.cluster-xyz.us-east-1.rds.amazonaws.com", HostRole.WRITER);
    HostSpec writerOther = createHost("instance1.cluster-xyz.us-west-2.rds.amazonaws.com", HostRole.WRITER);
    assertTrue(p.isSatisfiedBy(writer, "us-east-1", rdsUtils));
    assertFalse(p.isSatisfiedBy(writerOther, "us-east-1", rdsUtils));
  }

  @Test
  void testIsSatisfiedByReaderInSpecificRegion() {
    GdbMonitoringConnectionPriority p = GdbMonitoringConnectionPriority.fromValue("strict-reader-us-west-2");
    HostSpec reader = createHost("instance2.cluster-ro-xyz.us-west-2.rds.amazonaws.com", HostRole.READER);
    HostSpec readerOther = createHost("instance2.cluster-ro-xyz.us-east-1.rds.amazonaws.com", HostRole.READER);
    assertTrue(p.isSatisfiedBy(reader, "us-east-1", rdsUtils));
    assertFalse(p.isSatisfiedBy(readerOther, "us-east-1", rdsUtils));
  }

  @Test
  void testIsSatisfiedByAnyNodeInRegion() {
    GdbMonitoringConnectionPriority p = GdbMonitoringConnectionPriority.fromValue("us-east-1");
    HostSpec writer = createHost("instance1.cluster-xyz.us-east-1.rds.amazonaws.com", HostRole.WRITER);
    HostSpec reader = createHost("instance2.cluster-ro-xyz.us-east-1.rds.amazonaws.com", HostRole.READER);
    HostSpec otherRegion = createHost("instance3.cluster-xyz.us-west-2.rds.amazonaws.com", HostRole.WRITER);
    assertTrue(p.isSatisfiedBy(writer, "us-east-1", rdsUtils));
    assertTrue(p.isSatisfiedBy(reader, "us-east-1", rdsUtils));
    assertFalse(p.isSatisfiedBy(otherRegion, "us-east-1", rdsUtils));
  }

  @Test
  void testIsSatisfiedByRoleCheckRejectsWrongRole() {
    GdbMonitoringConnectionPriority writerPriority = GdbMonitoringConnectionPriority.fromValue("strict-writer-primary");
    HostSpec reader = createHost("instance2.cluster-ro-xyz.us-east-1.rds.amazonaws.com", HostRole.READER);
    assertFalse(writerPriority.isSatisfiedBy(reader, "us-east-1", rdsUtils));
  }

  @Test
  void testFindMatchingHost() {
    GdbMonitoringConnectionPriority p = GdbMonitoringConnectionPriority.fromValue("strict-reader-us-west-2");
    HostSpec writer = createHost("instance1.cluster-xyz.us-east-1.rds.amazonaws.com", HostRole.WRITER);
    HostSpec readerEast = createHost("instance2.cluster-ro-xyz.us-east-1.rds.amazonaws.com", HostRole.READER);
    HostSpec readerWest = createHost("instance3.cluster-ro-xyz.us-west-2.rds.amazonaws.com", HostRole.READER);

    List<HostSpec> hosts = Arrays.asList(writer, readerEast, readerWest);
    HostSpec result = p.findMatchingHost(hosts, "us-east-1", rdsUtils);
    assertNotNull(result);
    assertEquals(readerWest, result);
  }

  @Test
  void testFindMatchingHostReturnsNullWhenNoMatch() {
    GdbMonitoringConnectionPriority p = GdbMonitoringConnectionPriority.fromValue("strict-writer-us-west-2");
    HostSpec writer = createHost("instance1.cluster-xyz.us-east-1.rds.amazonaws.com", HostRole.WRITER);
    HostSpec reader = createHost("instance2.cluster-ro-xyz.us-east-1.rds.amazonaws.com", HostRole.READER);

    List<HostSpec> hosts = Arrays.asList(writer, reader);
    HostSpec result = p.findMatchingHost(hosts, "us-east-1", rdsUtils);
    assertNull(result);
  }
}
