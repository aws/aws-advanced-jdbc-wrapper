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

package software.amazon.jdbc.targetdriverdialect;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import software.amazon.jdbc.util.Messages;

/**
 * Converts the wrapper timeout properties, which are always in milliseconds, to the whole seconds
 * that some target drivers accept (the PostgreSQL JDBC driver (pgjdbc), and the MariaDB
 * Connector/J data source login timeout). The conversion truncates, as the PostgreSQL path always
 * has, to stay backward compatible. A value from 1 to 999 therefore becomes 0, which these drivers
 * treat as no timeout. That case is logged once per target driver, property and value.
 *
 * <p>This class must not reference target driver classes, so that the dialects can load without
 * those drivers on the classpath.
 */
final class TimeoutConverter {

  static final String PGJDBC = "PostgreSQL JDBC driver (pgjdbc)";
  static final String MARIADB_CONNECTOR_J = "MariaDB Connector/J";

  private static final Logger LOGGER = Logger.getLogger(TimeoutConverter.class.getName());

  // "driver:property=value" entries already reported, so the warning is logged once per JVM per
  // distinct value rather than on every (pooled or monitoring) connection. Bounded: at most
  // 2 drivers x 3 properties x 999 values.
  private static final Set<String> reportedTruncatedValues = ConcurrentHashMap.newKeySet();

  private TimeoutConverter() {
  }

  /**
   * Converts a wrapper timeout in milliseconds to whole seconds, truncating.
   *
   * @param targetDriverName the target driver name, used in the warning.
   * @param propertyName     the wrapper property name, used in the warning.
   * @param millis           the timeout in milliseconds.
   * @return the timeout in whole seconds.
   */
  static int millisToSeconds(final String targetDriverName, final String propertyName, final int millis) {
    if (millis > 0 && millis < 1000
        && reportedTruncatedValues.add(targetDriverName + ":" + propertyName + "=" + millis)) {
      LOGGER.warning(() -> Messages.get(
          "TimeoutConverter.timeoutTruncatedToZero",
          new Object[] {propertyName, String.valueOf(millis), targetDriverName}));
    }
    return (int) TimeUnit.MILLISECONDS.toSeconds(millis);
  }

  /**
   * Forgets the reported values so the once-per-JVM warning can be tested in isolation. Intended
   * for tests.
   */
  static void clearReportedTruncatedValues() {
    reportedTruncatedValues.clear();
  }
}
