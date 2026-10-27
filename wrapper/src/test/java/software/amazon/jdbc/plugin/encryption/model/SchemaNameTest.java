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

package software.amazon.jdbc.plugin.encryption.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.jdbc.util.Messages;

public class SchemaNameTest {

  @ParameterizedTest
  @ValueSource(strings = {"encrypt", "_a1", "Schema_2"})
  void validNames_accepted(String name) {
    assertEquals(name, SchemaName.of(name).getValue());
    assertTrue(SchemaName.isValidIdentifier(name));
  }

  @Test
  void nullOrEmpty_rejected() {
    final IllegalArgumentException nullName = assertThrows(IllegalArgumentException.class, () -> SchemaName.of(null));
    assertEquals(Messages.get("SchemaName.nullOrEmpty"), nullName.getMessage());
    final IllegalArgumentException empty = assertThrows(IllegalArgumentException.class, () -> SchemaName.of(""));
    assertEquals(Messages.get("SchemaName.nullOrEmpty"), empty.getMessage());
    assertFalse(SchemaName.isValidIdentifier(null));
    assertFalse(SchemaName.isValidIdentifier(""));
  }

  @ParameterizedTest
  @ValueSource(strings = {"1abc", "a-b", "a b", "a\"b", "a.b"})
  void invalidNames_rejected(String name) {
    final IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> SchemaName.of(name));
    assertEquals(Messages.get("SchemaName.invalidName", new Object[] {name
        + "'. Must start with a letter or underscore and contain only"
        + " letters, digits, and underscores."}), exception.getMessage());
    assertFalse(SchemaName.isValidIdentifier(name));
  }
}
