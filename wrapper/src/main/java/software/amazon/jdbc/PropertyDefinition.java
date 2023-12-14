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

  public static final AwsWrapperProperty AUTO_SORT_PLUGIN_ORDER =
      new AwsWrapperProperty(
          "autoSortWrapperPluginOrder",
          "true",
          "This flag is enabled by default, meaning that the plugins order will be automatically adjusted."
          + " Disable it at your own risk or if you really need plugins to be executed in a particular order.");

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

  public static final AwsWrapperProperty ENABLE_TELEMETRY =
      new AwsWrapperProperty(
          "enableTelemetry", "false",
          "Enables telemetry and observability of the driver");

  public static final AwsWrapperProperty TELEMETRY_SUBMIT_TOPLEVEL =
      new AwsWrapperProperty(
          "telemetrySubmitToplevel", "false",
          "Force submitting traces related to JDBC calls as top level traces.");

  public static final AwsWrapperProperty TELEMETRY_TRACES_BACKEND =
      new AwsWrapperProperty(
          "telemetryTracesBackend",
          null,
          "Method to export telemetry traces of the driver",
          false,
          new String[] {
              "XRAY", "OTLP", "NONE"
          });

  public static final AwsWrapperProperty TELEMETRY_METRICS_BACKEND =
      new AwsWrapperProperty(
          "telemetryMetricsBackend",
          null,
          "Method to export telemetry metrics of the driver",
          false,
          new String[] {
              "OTLP", "NONE"
          });

  public static final AwsWrapperProperty AWS_PROFILE =
      new AwsWrapperProperty(
          "awsProfile", null, "Name of the AWS Profile to use for IAM/SecretsManager auth.");

  public static final AwsWrapperProperty LOGIN_TIMEOUT =
      new AwsWrapperProperty(
          "loginTimeout", null, "Login timeout in msec.");

  public static final AwsWrapperProperty CONNECT_TIMEOUT =
      new AwsWrapperProperty(
          "connectTimeout", null, "Socket connect timeout in msec.");
  public static final AwsWrapperProperty SOCKET_TIMEOUT =
      new AwsWrapperProperty(
          "socketTimeout", null, "Socket timeout in msec.");

  public static final AwsWrapperProperty TCP_KEEP_ALIVE =
      new AwsWrapperProperty(
          "tcpKeepAlive",
          "false",
          "Enable or disable TCP keep-alive probe.",
          false,
          new String[] {
              "true", "false"
          });

  public static final AwsWrapperProperty TRANSFER_SESSION_STATE_ON_SWITCH =
      new AwsWrapperProperty(
          "transferSessionStateOnSwitch",
          "true",
          "Enables session state transfer to a new connection.",
          false,
          new String[] {
              "true", "false"
          });

  public static final AwsWrapperProperty RESET_SESSION_STATE_ON_CLOSE =
      new AwsWrapperProperty(
          "resetSessionStateOnClose",
          "true",
          "Enables to reset connection session state before closing it.",
          false,
          new String[] {
              "true", "false"
          });

  public static final AwsWrapperProperty ROLLBACK_ON_SWITCH =
      new AwsWrapperProperty(
          "rollbackOnSwitch",
          "true",
          "Enables to rollback a current transaction being in progress when switching to a new connection.",
          false,
          new String[] {
              "true", "false"
          });

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
