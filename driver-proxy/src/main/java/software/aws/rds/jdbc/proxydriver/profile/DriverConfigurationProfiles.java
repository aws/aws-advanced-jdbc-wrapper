/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.profile;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.aws.rds.jdbc.proxydriver.ConnectionPluginFactory;

public class DriverConfigurationProfiles {

  private static final Map<String, List<Class<? extends ConnectionPluginFactory>>> profiles =
      new ConcurrentHashMap<>();

  public static void clear() {
    profiles.clear();
  }

  public static void addOrReplaceProfile(
      @NonNull String profileName,
      @NonNull List<Class<? extends ConnectionPluginFactory>> pluginFactories) {
    profiles.put(profileName, pluginFactories);
  }

  public static void remove(@NonNull String profileName) {
    profiles.remove(profileName);
  }

  public static boolean contains(@NonNull String profileName) {
    return profiles.containsKey(profileName);
  }

  public static List<Class<? extends ConnectionPluginFactory>> getPluginFactories(
      @NonNull String profileName) {
    return profiles.get(profileName);
  }
}
