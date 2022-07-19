/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.plugin.efm;

import java.util.concurrent.ExecutorService;

/**
 * Interface for passing a specific {@link ExecutorService} to use by the {@link
 * MonitorThreadContainer}.
 */
@FunctionalInterface
public interface ExecutorServiceInitializer {
  ExecutorService createExecutorService();
}
