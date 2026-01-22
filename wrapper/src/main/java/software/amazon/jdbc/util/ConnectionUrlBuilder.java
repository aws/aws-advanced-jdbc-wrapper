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

import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Properties;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PropertyDefinition;

public class ConnectionUrlBuilder {

  // Builds a connection URL of the generic format: "protocol//[hosts][:port]/[database]"
  public static String buildUrl(
      final @NonNull String jdbcProtocol,
      final @NonNull String serverName,
      final int port,
      final @Nullable String databaseName) throws SQLException {

    if (StringUtils.isNullOrEmpty(jdbcProtocol) || StringUtils.isNullOrEmpty(serverName)) {
      throw new SQLException(Messages.get("ConnectionUrlBuilder.missingJdbcProtocol"));
    }

    final StringBuilder urlBuilder = new StringBuilder();
    urlBuilder.append(jdbcProtocol);

    if (!jdbcProtocol.endsWith("//")) {
      urlBuilder.append("//");
    }

    urlBuilder.append(serverName);

    if (port > 0) {
      urlBuilder.append(":").append(port);
    }

    urlBuilder.append("/");

    if (!StringUtils.isNullOrEmpty(databaseName)) {
      urlBuilder.append(databaseName);
    }

    return urlBuilder.toString();
  }

  // Builds a connection URL of the generic format: "protocol//[hosts][/database][?properties]"
  public static String buildUrl(final String jdbcProtocol,
      final HostSpec hostSpec,
      final Properties props) throws SQLException {

    if (StringUtils.isNullOrEmpty(jdbcProtocol) || hostSpec == null) {
      throw new SQLException(Messages.get("ConnectionUrlBuilder.missingJdbcProtocol"));
    }

    final Properties copy = PropertyUtils.copyProperties(props);
    final StringBuilder urlBuilder = new StringBuilder();
    urlBuilder.append(jdbcProtocol);

    if (!jdbcProtocol.endsWith("//")) {
      urlBuilder.append("//");
    }

    urlBuilder.append(hostSpec.getUrl());

    if (!StringUtils.isNullOrEmpty(PropertyDefinition.DATABASE.getString(copy))) {
      urlBuilder.append(PropertyDefinition.DATABASE.getString(copy));
      copy.remove(PropertyDefinition.DATABASE.name);
    }

    final StringBuilder queryBuilder = new StringBuilder();
    final Enumeration<?> propertyNames = copy.propertyNames();
    while (propertyNames.hasMoreElements()) {
      final String propertyName = propertyNames.nextElement().toString();
      if (propertyName != null && !propertyName.trim().equals("")) {
        if (queryBuilder.length() != 0) {
          queryBuilder.append("&");
        }
        final String propertyValue = copy.getProperty(propertyName);
        queryBuilder
            .append(propertyName)
            .append("=")
            .append(propertyValue);
      }
    }

    if (queryBuilder.length() != 0) {
      urlBuilder.append("?").append(queryBuilder);
    }

    return urlBuilder.toString();
  }
}
