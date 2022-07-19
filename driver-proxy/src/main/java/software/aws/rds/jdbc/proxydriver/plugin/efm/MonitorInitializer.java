/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.plugin.efm;

import java.util.Properties;
import software.aws.rds.jdbc.proxydriver.HostSpec;

/** Interface for initialize a new {@link MonitorImpl}. */
@FunctionalInterface
public interface MonitorInitializer {
  Monitor createMonitor(HostSpec hostSpec, Properties properties, MonitorService monitorService);
}
