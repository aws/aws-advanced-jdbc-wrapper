/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.checkerframework.checker.nullness.qual.Nullable;

public class PropertyDefinition {

  public static final ProxyDriverProperty PLUGIN_FACTORIES =
      new ProxyDriverProperty(
          "proxyDriverPluginFactories", null, "Coma separated list of connection plugin factories");

  public static final ProxyDriverProperty LOGGER_LEVEL =
      new ProxyDriverProperty(
          "proxyDriverLoggerLevel",
          null,
          "Logger level of the driver",
          false,
          new String[]{
              "OFF", "SEVERE", "WARNING", "INFO", "CONFIG", "FINE", "FINER", "FINEST", "ALL"
          });

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
}
