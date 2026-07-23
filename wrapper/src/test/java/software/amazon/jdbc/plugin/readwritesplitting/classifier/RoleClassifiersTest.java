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

package software.amazon.jdbc.plugin.readwritesplitting.classifier;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;

/** Unit tests for the role classifiers. */
public class RoleClassifiersTest {

  private HostSpec host(final String name, final HostRole role) {
    return new HostSpecBuilder(new SimpleHostAvailabilityStrategy()).host(name).port(5432).role(role).build();
  }

  // ---- TopologyRoleClassifier ----

  @Test
  void topology_classifiesByHostRole() {
    final TopologyRoleClassifier classifier = new TopologyRoleClassifier();
    assertTrue(classifier.isWriter(host("w", HostRole.WRITER)));
    assertFalse(classifier.isReader(host("w", HostRole.WRITER)));
    assertTrue(classifier.isReader(host("r", HostRole.READER)));
    assertFalse(classifier.isWriter(host("r", HostRole.READER)));
  }

  @Test
  void topology_nullHost_isNeither() {
    final TopologyRoleClassifier classifier = new TopologyRoleClassifier();
    assertFalse(classifier.isWriter(null));
    assertFalse(classifier.isReader(null));
  }

  // ---- EndpointRoleClassifier ----

  @Test
  void endpoint_matchesByHostString() {
    final EndpointRoleClassifier classifier =
        new EndpointRoleClassifier("writer.endpoint", "reader.endpoint");
    assertTrue(classifier.isWriter(host("writer.endpoint", HostRole.READER)));
    assertTrue(classifier.isReader(host("reader.endpoint", HostRole.WRITER)));
    assertFalse(classifier.isWriter(host("reader.endpoint", HostRole.WRITER)));
    assertFalse(classifier.isReader(host("writer.endpoint", HostRole.READER)));
  }

  @Test
  void endpoint_matchIsCaseInsensitive() {
    final EndpointRoleClassifier classifier =
        new EndpointRoleClassifier("Writer.Endpoint", "Reader.Endpoint");
    assertTrue(classifier.isWriter(host("writer.endpoint", HostRole.WRITER)));
    assertTrue(classifier.isReader(host("reader.endpoint", HostRole.READER)));
  }

  @Test
  void endpoint_matchesByHostAndPort() {
    final EndpointRoleClassifier classifier =
        new EndpointRoleClassifier("writer.endpoint:5432", "reader.endpoint:5432");
    assertTrue(classifier.isWriter(host("writer.endpoint", HostRole.WRITER)));
    assertTrue(classifier.isReader(host("reader.endpoint", HostRole.READER)));
  }

  @Test
  void endpoint_nullHost_isNeither() {
    final EndpointRoleClassifier classifier =
        new EndpointRoleClassifier("writer.endpoint", "reader.endpoint");
    assertFalse(classifier.isWriter(null));
    assertFalse(classifier.isReader(null));
  }
}
