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

import java.sql.DriverPropertyInfo;
import java.util.Properties;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

public class AwsWrapperProperty extends DriverPropertyInfo {

  public final @Nullable String defaultValue;

  public AwsWrapperProperty(
      final @NonNull String name,
      final @Nullable String defaultValue,
      final String description) {
    this(name, defaultValue, description, false);
  }

  public AwsWrapperProperty(
      final @NonNull String name,
      final @Nullable String defaultValue,
      final String description,
      final boolean required) {
    this(name, defaultValue, description, required, null);
  }

  // DriverPropertyInfo's stub types the constructor 'value' parameter as @NonNull (a null initial
  // value is valid here) and its 'choices' field as @NonNull (a null choices array is valid).
  @SuppressWarnings({"argument", "assignment"})
  public AwsWrapperProperty(
      final @NonNull String name,
      final @Nullable String defaultValue,
      final String description,
      final boolean required,
      final String @Nullable [] choices) {
    super(name, null);
    this.defaultValue = defaultValue;
    this.required = required;
    this.description = description;
    this.choices = choices;
  }

  public @Nullable String getString(final Properties properties) {
    return properties.getProperty(name, defaultValue);
  }

  public boolean getBoolean(final Properties properties) {
    final Object value = properties.get(name);
    if (value instanceof Boolean) {
      return (Boolean) value;
    }
    return Boolean.parseBoolean(properties.getProperty(name, defaultValue));
  }

  public int getInteger(final Properties properties) {
    final Object value = properties.get(name);
    if (value instanceof Integer) {
      return (Integer) value;
    }
    final @Nullable String stringValue = properties.getProperty(name, defaultValue);
    // Integer.parseInt throws NumberFormatException on null, matching prior behavior when the
    // property is unset and has no default value.
    @SuppressWarnings("argument")
    final int result = Integer.parseInt(stringValue);
    return result;
  }

  public long getLong(final Properties properties) {
    final Object value = properties.get(name);
    if (value instanceof Long) {
      return (Long) value;
    }
    final @Nullable String stringValue = properties.getProperty(name, defaultValue);
    // Long.parseLong throws NumberFormatException on null, matching prior behavior when the
    // property is unset and has no default value.
    @SuppressWarnings("argument")
    final long result = Long.parseLong(stringValue);
    return result;
  }

  public void set(final Properties properties, final @Nullable String value) {
    if (value == null) {
      properties.remove(name);
    } else {
      properties.setProperty(name, value);
    }
  }

  public @Nullable String[] getChoices() {
    return this.choices;
  }

  public DriverPropertyInfo toDriverPropertyInfo(final Properties properties) {
    final @Nullable String value = getString(properties);
    // DriverPropertyInfo's stub types the constructor 'value' parameter as @NonNull, but a null
    // value is valid (indicates the property is unset).
    @SuppressWarnings("argument")
    final DriverPropertyInfo propertyInfo = new DriverPropertyInfo(name, value);
    propertyInfo.required = required;
    propertyInfo.description = description;
    propertyInfo.choices = choices;
    return propertyInfo;
  }
}
