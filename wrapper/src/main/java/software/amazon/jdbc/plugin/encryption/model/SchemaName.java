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
import java.util.regex.Pattern;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.util.Messages;

/**
 * A validated schema name that is safe for use in SQL identifier positions.
 * Guarantees that only names matching valid PostgreSQL unquoted identifier rules
 * (letter or underscore start, alphanumeric/underscore body) can be constructed.
 */
public final class SchemaName {

  private static final String VALID_PATTERN = "^[a-zA-Z_][a-zA-Z0-9_]*$";

  private static final Pattern VALID = Pattern.compile(VALID_PATTERN);

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
  public static SchemaName of(@Nullable String name) {
    if (name == null || name.isEmpty()) {
      throw new IllegalArgumentException(Messages.get("SchemaName.nullOrEmpty"));
    }
    if (!isValidIdentifier(name)) {
      throw new IllegalArgumentException(
          Messages.get("SchemaName.invalidName", new Object[]{name
              + "'. Must start with a letter or underscore and contain only"
              + " letters, digits, and underscores."}));
    }
    return new SchemaName(name);
  }

  /**
   * Checks a name against the identifier rule used for schema names: a letter or underscore,
   * followed by letters, digits and underscores. No length limit is applied.
   *
   * @param name the name to check
   * @return true if the name is non-null and matches the rule
   */
  public static boolean isValidIdentifier(@Nullable String name) {
    return name != null && VALID.matcher(name).matches();
  }

  /**
   * Returns the validated schema name string.
   *
   * @return the schema name value
   */
  public String getValue() {
    return value;
  }

  @Override
  public String toString() {
    return value;
  }

  @Override
  public boolean equals(@Nullable Object o) {
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
