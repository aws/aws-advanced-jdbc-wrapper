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

package software.amazon.jdbc.util;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PropertyDefinition;

public class LogUtils {

  private static final Logger DRIVER_PARENT_LOGGER = Logger.getLogger("software.amazon.jdbc");

  /**
   * Applies the logger level requested through the {@code wrapperLoggerLevel} connection property,
   * if it's specified. The level is set on the driver's parent logger
   * ({@code software.amazon.jdbc}), so loggers that have their own level explicitly configured (for
   * instance, through a {@code logging.properties} file) are not affected and keep their configured
   * level.
   *
   * <p>This method should be called as early as possible while establishing a connection so that
   * the driver's own log messages are governed by the requested level.
   *
   * @param props the connection properties to get the requested logger level from
   * @throws IllegalArgumentException if the specified logger level isn't a valid {@link Level}
   */
  public static void applyLoggerLevel(final Properties props) {
    final String logLevelStr = PropertyDefinition.LOGGER_LEVEL.getString(props);
    if (StringUtils.isNullOrEmpty(logLevelStr)) {
      return;
    }

    final Level logLevel = Level.parse(logLevelStr.toUpperCase());
    final Logger rootLogger = Logger.getLogger("");
    for (final Handler handler : rootLogger.getHandlers()) {
      if (handler instanceof ConsoleHandler) {
        if (handler.getLevel().intValue() > logLevel.intValue()) {
          // Set higher (more detailed) level as requested
          handler.setLevel(logLevel);
        }
      }
    }
    DRIVER_PARENT_LOGGER.setLevel(logLevel);
  }

  public static String logTopology(final @Nullable List<HostSpec> hosts) {
    return logTopology(hosts, null);
  }

  public static String logTopology(
      final @Nullable List<HostSpec> hosts,
      final @Nullable String messagePrefix) {

    final StringBuilder msg = new StringBuilder();
    if (hosts == null) {
      msg.append("<null>");
    } else if (hosts.isEmpty()) {
      msg.append("<empty>");
    } else {
      for (final HostSpec host : hosts) {
        if (msg.length() > 0) {
          msg.append("\n");
        }
        msg.append("   ").append(host == null ? "<null>" : host);
      }
    }

    return Messages.get("Utils.topology",
        new Object[] {messagePrefix == null ? "Topology:" : messagePrefix, msg.toString()});
  }

  public static String toLogString(Map<String, HostSpec> map) {
    return map.entrySet().stream()
        .map(x -> String.format("\t[%s] -> %s", x.getKey(), x.getValue().getHostAndPort()))
        .collect(Collectors.joining("\n"));
  }
}
