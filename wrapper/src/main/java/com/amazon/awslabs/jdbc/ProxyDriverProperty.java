/*
 *
 *     Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License").
 *     You may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.amazon.awslabs.jdbc;

import java.sql.DriverPropertyInfo;
import java.util.Properties;
import org.checkerframework.checker.nullness.qual.Nullable;

public class ProxyDriverProperty extends DriverPropertyInfo {

  public final @Nullable String defaultValue;

  public ProxyDriverProperty(String name, @Nullable String defaultValue, String description) {
    this(name, defaultValue, description, false);
  }

  public ProxyDriverProperty(
      String name, @Nullable String defaultValue, String description, boolean required) {
    this(name, defaultValue, description, required, (String[]) null);
  }

  public ProxyDriverProperty(
      String name,
      @Nullable String defaultValue,
      String description,
      boolean required,
      String @Nullable [] choices) {
    super(name, null);
    this.defaultValue = defaultValue;
    this.required = required;
    this.description = description;
    this.choices = choices;
  }

  public @Nullable String get(Properties properties) {
    return properties.getProperty(name, defaultValue);
  }

  public String getString(Properties properties) {
    return String.valueOf(properties.getProperty(name, defaultValue));
  }

  public boolean getBoolean(Properties properties) {
    Object value = properties.get(name);
    if (value instanceof Boolean) {
      return (Boolean) value;
    }
    return Boolean.parseBoolean(properties.getProperty(name, defaultValue));
  }

  public int getInteger(Properties properties) {
    Object value = properties.get(name);
    if (value instanceof Integer) {
      return (Integer) value;
    }
    return Integer.parseInt(properties.getProperty(name, defaultValue));
  }

  public void set(Properties properties, @Nullable String value) {
    if (value == null) {
      properties.remove(name);
    } else {
      properties.setProperty(name, value);
    }
  }

  public DriverPropertyInfo toDriverPropertyInfo(Properties properties) {
    DriverPropertyInfo propertyInfo = new DriverPropertyInfo(name, get(properties));
    propertyInfo.required = required;
    propertyInfo.description = description;
    propertyInfo.choices = choices;
    return propertyInfo;
  }
}
