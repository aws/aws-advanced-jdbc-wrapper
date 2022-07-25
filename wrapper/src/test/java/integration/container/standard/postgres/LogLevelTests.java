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
        LoggerFactory.getLogger(
            "integration.container.standard.postgres.StandardPostgresIntegrationTest");

    logger.debug("StandardPostgresIntegrationTest debug");
    logger.trace("StandardPostgresIntegrationTest trace");
    logger.info("StandardPostgresIntegrationTest info");
    logger.warn("StandardPostgresIntegrationTest warn");
    logger.error("StandardPostgresIntegrationTest error");
  }
}
