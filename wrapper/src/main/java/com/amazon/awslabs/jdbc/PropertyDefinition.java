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

package com.amazon.awslabs.jdbc;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.checkerframework.checker.nullness.qual.Nullable;

public class PropertyDefinition {

  public static final ProxyDriverProperty LOG_UNCLOSED_CONNECTIONS =
      new ProxyDriverProperty(
          "wrapperLogUnclosedConnections", "false",
          "Allows the driver to track a point in the code where connection has been opened and never closed after");

  public static final ProxyDriverProperty LOGGER_LEVEL =
      new ProxyDriverProperty(
          "wrapperLoggerLevel",
          null,
          "Logger level of the driver",
          false,
          new String[] {
              "OFF", "SEVERE", "WARNING", "INFO", "CONFIG", "FINE", "FINER", "FINEST", "ALL"
          });

  public static final ProxyDriverProperty PLUGINS =
      new ProxyDriverProperty(
          "wrapperPlugins", null, "Comma separated list of connection plugin codes");

  public static final ProxyDriverProperty PROFILE_NAME =
      new ProxyDriverProperty(
          "wrapperProfileName", null, "Driver configuration profile name");

  public static final ProxyDriverProperty USER =
      new ProxyDriverProperty(
          "wrapperUser", null, "Driver user name");

  public static final ProxyDriverProperty PASSWORD =
      new ProxyDriverProperty(
          "wrapperPassword", null, "Driver password");

  public static final ProxyDriverProperty DATABASE_NAME =
      new ProxyDriverProperty(
          "wrapperDatabaseName", null, "Driver database name");

  public static final ProxyDriverProperty TARGET_DRIVER_USER_PROPERTY_NAME =
      new ProxyDriverProperty(
          "wrapperTargetDriverUserPropertyName", null, "Target driver user property name");

  public static final ProxyDriverProperty TARGET_DRIVER_PASSWORD_PROPERTY_NAME =
      new ProxyDriverProperty(
          "wrapperTargetDriverPasswordPropertyName",
          null,
          "Target driver password property name");

  private static final Map<String, ProxyDriverProperty> PROPS_BY_NAME =
      new HashMap<String, ProxyDriverProperty>();

  static {
    PROPS_BY_NAME.clear();
    Field[] ff = PropertyDefinition.class.getDeclaredFields();
    Arrays.stream(PropertyDefinition.class.getDeclaredFields())
        .filter(
            f ->
                f.getType() == ProxyDriverProperty.class
                    && Modifier.isPublic(f.getModifiers())
                    && Modifier.isStatic(f.getModifiers()))
        .forEach(
            f -> {
              ProxyDriverProperty prop = null;
              try {
                prop = (ProxyDriverProperty) f.get(ProxyDriverProperty.class);
              } catch (IllegalArgumentException | IllegalAccessException ex) {
                // ignore exception
              }

              if (prop != null) {
                PROPS_BY_NAME.put(prop.name, prop);
              }
            });
  }

  public static @Nullable ProxyDriverProperty byName(String name) {
    return PROPS_BY_NAME.get(name);
  }

  public static Collection<ProxyDriverProperty> allProperties() {
    return PROPS_BY_NAME.values();
  }

  public static void removeAll(Properties props) {
    PROPS_BY_NAME.keySet().forEach((propName) -> props.remove(propName));
  }
}
