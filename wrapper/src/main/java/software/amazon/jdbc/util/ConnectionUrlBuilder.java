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

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Properties;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PropertyDefinition;

public class ConnectionUrlBuilder {

  // Builds a connection URL of the generic format: "protocol//[hosts][/database][?properties]"
  public static String buildUrl(String jdbcProtocol,
      HostSpec hostSpec,
      String serverPropertyName,
      String portPropertyName,
      String databasePropertyName,
      Properties props) throws SQLException {
    if (StringUtils.isNullOrEmpty(jdbcProtocol)
        || ((StringUtils.isNullOrEmpty(serverPropertyName)
        || StringUtils.isNullOrEmpty(
        props.getProperty(serverPropertyName)))
        && hostSpec == null)) {
      throw new SQLException(Messages.get("ConnectionUrlBuilder.missingJdbcProtocol"));
    }

    final Properties copy = PropertyUtils.copyProperties(props);
    final StringBuilder urlBuilder = new StringBuilder();
    urlBuilder.append(jdbcProtocol);

    if (!jdbcProtocol.contains("//")) {
      urlBuilder.append("//");
    }

    if (hostSpec != null) {
      urlBuilder.append(hostSpec.getUrl());
    } else {
      urlBuilder.append(copy.get(serverPropertyName));

      if (!StringUtils.isNullOrEmpty(portPropertyName) && !StringUtils.isNullOrEmpty(
          copy.getProperty(portPropertyName))) {
        urlBuilder.append(":").append(copy.get(portPropertyName));
      }

      urlBuilder.append("/");
    }

    if (!StringUtils.isNullOrEmpty(PropertyDefinition.DATABASE.getString(copy))) {
      urlBuilder.append(PropertyDefinition.DATABASE.getString(copy));
      copy.remove(PropertyDefinition.DATABASE.name);
    }

    removeProperty(serverPropertyName, copy);
    removeProperty(portPropertyName, copy);
    removeProperty(databasePropertyName, copy);

    final StringBuilder queryBuilder = new StringBuilder();
    final Enumeration<?> propertyNames = copy.propertyNames();
    while (propertyNames.hasMoreElements()) {
      String propertyName = propertyNames.nextElement().toString();
      if (queryBuilder.length() != 0) {
        queryBuilder.append("&");
      }

      if (!StringUtils.isNullOrEmpty(propertyName)) {
        final String propertyValue = copy.getProperty(propertyName);
        try {
          queryBuilder
              .append(propertyName)
              .append("=")
              .append(URLEncoder.encode(propertyValue, StandardCharsets.UTF_8.toString()));
        } catch (UnsupportedEncodingException e) {
          throw new SQLException(
              Messages.get("ConnectionUrlBuilder.failureEncodingConnectionUrl"),
              e);
        }
      }
    }

    if (queryBuilder.length() != 0) {
      urlBuilder.append("?").append(queryBuilder);
    }

    return urlBuilder.toString();
  }

  private static void removeProperty(String propertyKey, Properties props) {
    if (!StringUtils.isNullOrEmpty(propertyKey)
        && !StringUtils.isNullOrEmpty(props.getProperty(propertyKey))) {
      props.remove(propertyKey);
    }
  }
}
