/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package integration.container.standard.postgres;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

@Disabled
public class LogLevelTests {

  @Test
  public void testJavaUtilLogging() {
    final Logger logger = Logger.getLogger(StandardPostgresIntegrationTest.class.getName());
    logger.log(Level.FINEST, "StandardPostgresIntegrationTest FINEST");
    logger.log(Level.FINER, "StandardPostgresIntegrationTest FINER");
    logger.log(Level.FINE, "StandardPostgresIntegrationTest FINE");
    logger.log(Level.WARNING, "StandardPostgresIntegrationTest WARNING");
    logger.log(Level.CONFIG, "StandardPostgresIntegrationTest CONFIG");
    logger.log(Level.INFO, "StandardPostgresIntegrationTest INFO");
    logger.log(Level.SEVERE, "StandardPostgresIntegrationTest SEVERE");
  }

  @Test
  public void testStandardStreams() {
    System.out.println("StandardPostgresIntegrationTest [OUT]");
    System.err.println("StandardPostgresIntegrationTest [ERR]");
  }

  @Test
  public void testSlf4j() throws IOException {
    org.slf4j.Logger logger =
        LoggerFactory.getLogger("integration.container.standard.postgres.StandardPostgresIntegrationTest");

    logger.debug("StandardPostgresIntegrationTest debug");
    logger.trace("StandardPostgresIntegrationTest trace");
    logger.info("StandardPostgresIntegrationTest info");
    logger.warn("StandardPostgresIntegrationTest warn");
    logger.error("StandardPostgresIntegrationTest error");
  }
}
