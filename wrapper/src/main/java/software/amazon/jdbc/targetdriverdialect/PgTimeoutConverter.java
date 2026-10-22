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
 * Converts the wrapper timeout properties (milliseconds) to the whole seconds expected by the
 * PostgreSQL JDBC driver. The conversion truncates, as it always has, to stay backward compatible.
 * A value from 1 to 999 therefore becomes 0, which pgjdbc treats as "no timeout"; that case is
 * reported with a warning.
 *
 * <p>This class must not reference pgjdbc classes, so that {@link PgTargetDriverDialect} can load
 * without the PostgreSQL driver on the classpath.
 */
final class PgTimeoutConverter {

  private static final Logger LOGGER = Logger.getLogger(PgTimeoutConverter.class.getName());

  // "property=value" pairs already reported, so the warning is logged once per JVM per distinct
  // value rather than on every (pooled or monitoring) connection. Bounded: at most 3 x 999 entries.
  private static final Set<String> reportedTruncatedValues = ConcurrentHashMap.newKeySet();

  private PgTimeoutConverter() {
  }

  /**
   * Converts a wrapper timeout in milliseconds to pgjdbc seconds, truncating exactly as before.
   *
   * @param propertyName the wrapper property name, used in the warning.
   * @param millis       the timeout in milliseconds.
   * @return the timeout in whole seconds.
   */
  static int millisToPgSeconds(final String propertyName, final int millis) {
    if (millis > 0 && millis < 1000 && reportedTruncatedValues.add(propertyName + "=" + millis)) {
      LOGGER.warning(() -> Messages.get(
          "PgTimeoutConverter.timeoutTruncatedToZero",
          new Object[] {propertyName, String.valueOf(millis)}));
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
