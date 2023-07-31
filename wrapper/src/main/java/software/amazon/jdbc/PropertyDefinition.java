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

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

public class PropertyDefinition {

  public static final AwsWrapperProperty LOG_UNCLOSED_CONNECTIONS =
      new AwsWrapperProperty(
          "wrapperLogUnclosedConnections", "false",
          "Allows the driver to track a point in the code where connection has been opened and never closed after");

  public static final AwsWrapperProperty LOGGER_LEVEL =
      new AwsWrapperProperty(
          "wrapperLoggerLevel",
          null,
          "Logger level of the driver",
          false,
          new String[] {
              "OFF", "SEVERE", "WARNING", "INFO", "CONFIG", "FINE", "FINER", "FINEST", "ALL"
          });

  public static final AwsWrapperProperty PLUGINS =
      new AwsWrapperProperty(
          "wrapperPlugins", null, "Comma separated list of connection plugin codes");

  public static final AwsWrapperProperty PRESERVE_PLUGINS_ORDER =
      new AwsWrapperProperty(
          "preserveWrapperPluginsOrder",
          "false",
          "Allows to preserve plugin order provided by user.");

  public static final AwsWrapperProperty PROFILE_NAME =
      new AwsWrapperProperty(
          "wrapperProfileName", null, "Driver configuration profile name");

  public static final AwsWrapperProperty USER =
      new AwsWrapperProperty(
          "user", null, "Driver user name");

  public static final AwsWrapperProperty PASSWORD =
      new AwsWrapperProperty(
          "password", null, "Driver password");

  public static final AwsWrapperProperty DATABASE =
      new AwsWrapperProperty(
          "database", null, "Driver database name");

  private static final Map<String, AwsWrapperProperty> PROPS_BY_NAME =
      new ConcurrentHashMap<>();
  private static final Set<String> KNOWN_PROPS_BY_PREFIX = ConcurrentHashMap.newKeySet();

  static {
    registerProperties(PropertyDefinition.class);
  }

  public static @Nullable AwsWrapperProperty byName(final String name) {
    return PROPS_BY_NAME.get(name);
  }

  public static Collection<AwsWrapperProperty> allProperties() {
    return PROPS_BY_NAME.values();
  }

  public static void registerPluginProperties(final Class<?> pluginClass) {
    registerProperties(pluginClass);
  }

  public static void registerPluginProperties(final @NonNull String propertyNamePrefix) {
    KNOWN_PROPS_BY_PREFIX.add(propertyNamePrefix);
  }

  public static void removeAll(final Properties props) {
    PROPS_BY_NAME.keySet().forEach(props::remove);

    props.stringPropertyNames().stream()
        .filter(propertyName -> KNOWN_PROPS_BY_PREFIX.stream()
            .anyMatch(propertyName::startsWith))
        .forEach(props::remove);
  }

  public static void removeAllExcept(final Properties props, String... propNames) {
    Set<String> propsToDelete = new HashSet<>(PROPS_BY_NAME.keySet());
    Arrays.asList(propNames).forEach(propsToDelete::remove);
    propsToDelete.forEach(props::remove);

    props.stringPropertyNames().stream()
        .filter(propertyName -> KNOWN_PROPS_BY_PREFIX.stream()
            .anyMatch(propertyName::startsWith))
        .forEach(props::remove);
  }

  public static void removeAllExceptCredentials(final Properties props) {
    final String user = props.getProperty(PropertyDefinition.USER.name, null);
    final String password = props.getProperty(PropertyDefinition.PASSWORD.name, null);

    removeAll(props);

    if (user != null) {
      props.setProperty(PropertyDefinition.USER.name, user);
    }

    if (password != null) {
      props.setProperty(PropertyDefinition.PASSWORD.name, password);
    }
  }

  private static void registerProperties(final Class<?> ownerClass) {
    Arrays.stream(ownerClass.getDeclaredFields())
        .filter(
            f ->
                f.getType() == AwsWrapperProperty.class
                    && Modifier.isPublic(f.getModifiers())
                    && Modifier.isStatic(f.getModifiers()))
        .forEach(
            f -> {
              AwsWrapperProperty prop = null;
              try {
                prop = (AwsWrapperProperty) f.get(AwsWrapperProperty.class);
              } catch (final IllegalArgumentException | IllegalAccessException ex) {
                // ignore exception
              }

              if (prop != null) {
                PROPS_BY_NAME.put(prop.name, prop);
              }
            });

  }
}
