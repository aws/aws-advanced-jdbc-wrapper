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

import static software.amazon.jdbc.util.ConnectionUrlBuilder.buildUrl;

import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.mariadb.jdbc.MariaDbDataSource;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.Messages;

public class MariadbDataSourceHelper {

  private static final Logger LOGGER =
      Logger.getLogger(MariadbDataSourceHelper.class.getName());

  private static final String LOGIN_TIMEOUT = "loginTimeout";
  private static final String DS_CLASS_NAME = MariaDbDataSource.class.getName();

  public void prepareDataSource(
      final @NonNull DataSource dataSource,
      final @NonNull String protocol,
      final @NonNull HostSpec hostSpec,
      final @NonNull Properties props) throws SQLException {

    if (!(dataSource instanceof MariaDbDataSource)) {
      throw new SQLException(Messages.get(
          "TargetDriverDialectManager.unexpectedClass",
          new Object[] { DS_CLASS_NAME, dataSource.getClass().getName() }));
    }

    final MariaDbDataSource mariaDbDataSource = (MariaDbDataSource) dataSource;

    mariaDbDataSource.setUser(PropertyDefinition.USER.getString(props));
    mariaDbDataSource.setPassword(PropertyDefinition.PASSWORD.getString(props));

    final String loginTimeoutValue = props.getProperty(LOGIN_TIMEOUT, null);
    if (loginTimeoutValue != null) {
      mariaDbDataSource.setLoginTimeout(Integer.parseInt(loginTimeoutValue));
      props.remove(LOGIN_TIMEOUT);
    }

    // keep unknown properties (the ones that don't belong to AWS Wrapper Driver)
    // and include them to connect URL.
    PropertyDefinition.removeAllExcept(props, PropertyDefinition.DATABASE.name);

    String finalUrl = buildUrl(protocol, hostSpec, props);
    LOGGER.finest(() -> "Connecting to " + finalUrl);
    mariaDbDataSource.setUrl(finalUrl);
  }
}
