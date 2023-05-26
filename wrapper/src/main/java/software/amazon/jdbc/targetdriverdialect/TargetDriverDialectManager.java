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

import java.sql.Driver;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.function.Function;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.StringUtils;

public class TargetDriverDialectManager implements TargetDriverDialectProvider {

  private static final Logger LOGGER = Logger.getLogger(TargetDriverDialectManager.class.getName());

  protected static TargetDriverDialect customDialect;

  public static final AwsWrapperProperty TARGET_DRIVER_DIALECT = new AwsWrapperProperty(
      "wrapperTargetDriverDialect", "",
      "A unique identifier for the target driver dialect.");

  /**
   * Every Dialect implementation SHOULD BE stateless!!!
   * Dialect objects are shared between different connections.
   * The order of entries in this map is the order in which dialects are called/verified.
   */
  protected static final Map<String, TargetDriverDialect> knownDialectsByCode =
      new HashMap<String, TargetDriverDialect>() {
        {
          put(TargetDriverDialectCodes.PG_JDBC, new PgTargetDriverDialect());
          put(TargetDriverDialectCodes.MYSQL_CONNECTOR_J, new MysqlConnectorJTargetDriverDialect());
          put(TargetDriverDialectCodes.MARIADB_CONNECTOR_J_VER_3, new MariadbTargetDriverDialect());
          put(TargetDriverDialectCodes.GENERIC, new GenericTargetDriverDialect());
        }
      };

  public static void setCustomDialect(final @NonNull TargetDriverDialect targetDriverDialect) {
    customDialect = targetDriverDialect;
  }

  public static void resetCustomDialect() {
    customDialect = null;
  }

  @Override
  public TargetDriverDialect getDialect(
      final @NonNull Driver driver,
      final @NonNull Properties props) throws SQLException {

    return this.getDialect(props, (targetDriverDialect -> targetDriverDialect.isDialect(driver)));
  }

  @Override
  public TargetDriverDialect getDialect(
      final @NonNull String dataSourceClass,
      final @NonNull Properties props) throws SQLException {

    LOGGER.finest(() -> "Try to identify target driver dialect for data source class: " + dataSourceClass);
    return this.getDialect(props, (targetDriverDialect -> targetDriverDialect.isDialect(dataSourceClass)));
  }

  private TargetDriverDialect getDialect(
      final @NonNull Properties props,
      Function<TargetDriverDialect, Boolean> checkFunc) throws SQLException {

    if (customDialect != null) {
      if (checkFunc.apply(customDialect)) {
        this.logDialect("custom", customDialect);
        return customDialect;
      } else {
        LOGGER.warning(() -> Messages.get("TargetDriverDialectManager.customDialectNotSupported"));
      }
    }

    TargetDriverDialect result;
    String dialectCode = TARGET_DRIVER_DIALECT.getString(props);
    if (!StringUtils.isNullOrEmpty(dialectCode)) {
      result = knownDialectsByCode.get(dialectCode);
      if (result == null) {
        throw new SQLException(Messages.get(
            "TargetDriverDialectManager.unknownDialectCode",
            new Object[] {dialectCode}));
      }
      this.logDialect(dialectCode, result);
      return result;
    }

    for (Entry<String, TargetDriverDialect> entry : knownDialectsByCode.entrySet()) {
      if (checkFunc.apply(entry.getValue())) {
        this.logDialect(entry.getKey(), entry.getValue());
        return entry.getValue();
      }
    }

    result = knownDialectsByCode.get(TargetDriverDialectCodes.GENERIC);
    this.logDialect(TargetDriverDialectCodes.GENERIC, result);
    return result;
  }

  private void logDialect(final String dialectCode, final TargetDriverDialect targetDriverDialect) {
    LOGGER.finest(() -> Messages.get(
        "TargetDriverDialectManager.useDialect",
        new Object[] {dialectCode, targetDriverDialect}));
  }
}
