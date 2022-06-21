/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver;

import java.sql.DriverPropertyInfo;
import java.util.Properties;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

public class ProxyDriverProperty extends DriverPropertyInfo {

  public final @Nullable String defaultValue;

  ProxyDriverProperty(String name, @Nullable String defaultValue, String description) {
    this(name, defaultValue, description, false);
  }

  ProxyDriverProperty(
      String name, @Nullable String defaultValue, String description, boolean required) {
    this(name, defaultValue, description, required, (String[]) null);
  }

  ProxyDriverProperty(
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

  public boolean getBoolean(Properties properties) {
    return Boolean.parseBoolean(properties.getProperty(name, defaultValue));
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
