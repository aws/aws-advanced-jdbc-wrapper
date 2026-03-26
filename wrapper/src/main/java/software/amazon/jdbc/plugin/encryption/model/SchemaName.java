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

import java.util.Objects;
import software.amazon.jdbc.util.Messages;

/**
 * A validated schema name that is safe for use in SQL identifier positions.
 * Guarantees that only names matching valid PostgreSQL unquoted identifier rules
 * (letter or underscore start, alphanumeric/underscore body) can be constructed.
 */
public final class SchemaName {

  private static final String VALID_PATTERN = "^[a-zA-Z_][a-zA-Z0-9_]*$";

  private final String value;

  private SchemaName(String value) {
    this.value = value;
  }

  /**
   * Creates a validated SchemaName.
   *
   * @param name the raw schema name string
   * @return a validated SchemaName
   * @throws IllegalArgumentException if the name is null, empty, or contains invalid characters
   */
  public static SchemaName of(String name) {
    if (name == null || name.isEmpty()) {
      throw new IllegalArgumentException(Messages.get("SchemaName.nullOrEmpty"));
    }
    if (!name.matches(VALID_PATTERN)) {
      throw new IllegalArgumentException(
          Messages.get("SchemaName.invalidName", new Object[]{name
              + "'. Must start with a letter or underscore and contain only"
              + " letters, digits, and underscores."}));
    }
    return new SchemaName(name);
  }

  /** Returns the validated schema name string. */
  public String getValue() {
    return value;
  }

  @Override
  public String toString() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    return value.equals(((SchemaName) o).value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }
}
