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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class MonitoringConnectionPriorityTest {

  @Test
  void testFromValue() {
    assertEquals(MonitoringConnectionPriority.STRICT_WRITER,
        MonitoringConnectionPriority.fromValue("strict-writer"));
    assertEquals(MonitoringConnectionPriority.STRICT_READER,
        MonitoringConnectionPriority.fromValue("strict-reader"));
    assertEquals(MonitoringConnectionPriority.WRITER_OR_READER,
        MonitoringConnectionPriority.fromValue("writer-or-reader"));
    assertEquals(MonitoringConnectionPriority.STRICT_WRITER,
        MonitoringConnectionPriority.fromValue("STRICT-WRITER"));
    assertNull(MonitoringConnectionPriority.fromValue("invalid"));
    assertNull(MonitoringConnectionPriority.fromValue(null));
  }

  @Test
  void testParseListDefault() {
    List<MonitoringConnectionPriority> result = MonitoringConnectionPriority.parseList(null);
    assertEquals(1, result.size());
    assertEquals(MonitoringConnectionPriority.STRICT_WRITER, result.get(0));
  }

  @Test
  void testParseListEmpty() {
    List<MonitoringConnectionPriority> result = MonitoringConnectionPriority.parseList("");
    assertEquals(1, result.size());
    assertEquals(MonitoringConnectionPriority.STRICT_WRITER, result.get(0));
  }

  @Test
  void testParseListSingleValue() {
    List<MonitoringConnectionPriority> result = MonitoringConnectionPriority.parseList("strict-reader");
    assertEquals(1, result.size());
    assertEquals(MonitoringConnectionPriority.STRICT_READER, result.get(0));
  }

  @Test
  void testParseListMultipleValues() {
    List<MonitoringConnectionPriority> result =
        MonitoringConnectionPriority.parseList("strict-writer,strict-reader,writer-or-reader");
    assertEquals(3, result.size());
    assertEquals(MonitoringConnectionPriority.STRICT_WRITER, result.get(0));
    assertEquals(MonitoringConnectionPriority.STRICT_READER, result.get(1));
    assertEquals(MonitoringConnectionPriority.WRITER_OR_READER, result.get(2));
  }

  @Test
  void testParseListWithSpaces() {
    List<MonitoringConnectionPriority> result =
        MonitoringConnectionPriority.parseList(" strict-reader , writer-or-reader ");
    assertEquals(2, result.size());
    assertEquals(MonitoringConnectionPriority.STRICT_READER, result.get(0));
    assertEquals(MonitoringConnectionPriority.WRITER_OR_READER, result.get(1));
  }

  @Test
  void testParseListIgnoresDuplicates() {
    List<MonitoringConnectionPriority> result =
        MonitoringConnectionPriority.parseList("strict-writer,strict-writer,strict-reader");
    assertEquals(2, result.size());
    assertEquals(MonitoringConnectionPriority.STRICT_WRITER, result.get(0));
    assertEquals(MonitoringConnectionPriority.STRICT_READER, result.get(1));
  }

  @Test
  void testParseListIgnoresInvalidValues() {
    List<MonitoringConnectionPriority> result =
        MonitoringConnectionPriority.parseList("invalid,strict-reader,bad-value");
    assertEquals(1, result.size());
    assertEquals(MonitoringConnectionPriority.STRICT_READER, result.get(0));
  }

  @Test
  void testParseListAllInvalidFallsBackToDefault() {
    List<MonitoringConnectionPriority> result =
        MonitoringConnectionPriority.parseList("invalid,bad-value");
    assertEquals(1, result.size());
    assertEquals(MonitoringConnectionPriority.STRICT_WRITER, result.get(0));
  }

  @Test
  void testIsSatisfiedByStrictWriter() {
    assertTrue(MonitoringConnectionPriority.STRICT_WRITER.isSatisfiedBy(true));
    assertFalse(MonitoringConnectionPriority.STRICT_WRITER.isSatisfiedBy(false));
  }

  @Test
  void testIsSatisfiedByStrictReader() {
    assertFalse(MonitoringConnectionPriority.STRICT_READER.isSatisfiedBy(true));
    assertTrue(MonitoringConnectionPriority.STRICT_READER.isSatisfiedBy(false));
  }

  @Test
  void testIsSatisfiedByWriterOrReader() {
    assertTrue(MonitoringConnectionPriority.WRITER_OR_READER.isSatisfiedBy(true));
    assertTrue(MonitoringConnectionPriority.WRITER_OR_READER.isSatisfiedBy(false));
  }
}
