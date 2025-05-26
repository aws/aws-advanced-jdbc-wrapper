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

package software.amazon.jdbc;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.junit.jupiter.api.Test;

public class JdbcMethodTests {

  @Test
  public void testUniqueIds() {
    Map<Integer, List<JdbcMethod>> groupedJdbcMethods =
        Arrays.stream(JdbcMethod.values()).collect(groupingBy(x -> x.id, toList()));
    List<Integer> duplicateIds = groupedJdbcMethods.entrySet().stream()
        .filter(x -> x.getValue().size() > 1)
        .map(Entry::getKey)
        .collect(toList());
    assertTrue(duplicateIds.isEmpty());
  }

  @Test
  public void testUniqueMethodNames() {
    Map<String, List<JdbcMethod>> groupedJdbcMethods =
        Arrays.stream(JdbcMethod.values()).collect(groupingBy(x -> x.methodName, toList()));
    List<String> duplicateNames = groupedJdbcMethods.entrySet().stream()
        .filter(x -> x.getValue().size() > 1)
        .map(Entry::getKey)
        .collect(toList());
    assertTrue(duplicateNames.isEmpty());
  }
}
