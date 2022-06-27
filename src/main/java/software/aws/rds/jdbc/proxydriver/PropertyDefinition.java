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

  public static final ProxyDriverProperty CLUSTER_INSTANCE_HOST_PATTERN =
      new ProxyDriverProperty(
          "clusterInstanceHostPattern",
          null,
          "The cluster instance DNS pattern that will be used to build a complete instance endpoint. "
              + "A \"?\" character in this pattern should be used as a placeholder for cluster instance names. "
              + "This pattern is required to be specified for IP address or custom domain connections to AWS RDS "
              + "clusters. Otherwise, if unspecified, the pattern will be automatically created for AWS RDS clusters.");

  public static final ProxyDriverProperty ENABLE_CLUSTER_AWARE_FAILOVER =
      new ProxyDriverProperty(
          "enableClusterAwareFailover", "true",
          "Enable/disable cluster-aware failover logic");

  public static final ProxyDriverProperty LOG_UNCLOSED_CONNECTIONS =
      new ProxyDriverProperty(
          "proxyDriverLogUnclosedConnections", "false",
          "Allows the driver to track a point in the code where connection has been opened and never closed after");

  public static final ProxyDriverProperty LOGGER_LEVEL =
      new ProxyDriverProperty(
          "proxyDriverLoggerLevel",
          null,
          "Logger level of the driver",
          false,
          new String[] {
              "OFF", "SEVERE", "WARNING", "INFO", "CONFIG", "FINE", "FINER", "FINEST", "ALL"
          });

  public static final ProxyDriverProperty PLUGINS =
      new ProxyDriverProperty(
          "proxyDriverPlugins", null, "Comma separated list of connection plugin codes");

  public static final ProxyDriverProperty PROFILE_NAME =
      new ProxyDriverProperty(
          "proxyDriverProfileName", null, "Driver configuration profile name");

  public static final ProxyDriverProperty USE_AWS_IAM =
      new ProxyDriverProperty(
          "useAwsIam", "false", "Set to true to use AWS IAM database authentication");


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
