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

package software.amazon.jdbc.ds;

import java.sql.SQLException;
import java.util.Properties;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.util.ConnectionUrlBuilder;
import software.amazon.jdbc.util.ConnectionUrlParser;
import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.StringUtils;

/**
 * Shared configuration-resolution logic for the wrapper's data sources
 * ({@link AwsWrapperDataSource} and {@link AwsWrapperXADataSource}). Resolves the final target JDBC
 * URL from either a configured JDBC URL or the server/port/database properties, and applies database
 * and credential overrides to {@code props}. The behavior mirrors the logic that previously lived
 * inline in {@link AwsWrapperDataSource#getConnection} so both entry points resolve configuration
 * identically.
 */
public final class DataSourceConfigHelper {

  private static final String SERVER_NAME = "serverName";
  private static final String SERVER_PORT = "serverPort";

  private DataSourceConfigHelper() {
  }

  /**
   * Resolves the final (non-wrapper) target URL and applies database/credential overrides to
   * {@code props}.
   *
   * @param props          the connection properties to update in place.
   * @param protocolPrefix the wrapper protocol prefix to strip from a configured JDBC URL
   *                       (e.g. {@code jdbc:aws-wrapper:}).
   * @param jdbcUrl        the configured JDBC URL, or null.
   * @param serverName     the configured server name, or null.
   * @param serverPort     the configured server port, or null.
   * @param database       the configured database name, or null.
   * @param jdbcProtocol   the configured JDBC protocol (required when {@code jdbcUrl} is not set).
   * @param user           the user to apply, or null.
   * @param password       the password to apply, or null.
   * @return the resolved final target URL.
   * @throws SQLException if neither a URL nor a server name is provided, or the protocol is missing.
   */
  public static String resolveFinalUrl(
      final Properties props,
      final String protocolPrefix,
      final @Nullable String jdbcUrl,
      final @Nullable String serverName,
      final @Nullable String serverPort,
      final @Nullable String database,
      final @Nullable String jdbcProtocol,
      final @Nullable String user,
      final @Nullable String password) throws SQLException {

    final String finalUrl;
    if (!StringUtils.isNullOrEmpty(jdbcUrl)) {
      finalUrl = jdbcUrl.replaceFirst(protocolPrefix, "jdbc:");

      ConnectionUrlParser.parsePropertiesFromUrl(jdbcUrl, props);
      setDatabasePropertyFromUrl(props, jdbcUrl);

      // Override credentials with the ones provided through the data source property.
      setCredentialProperties(props, user, password);

      // Override database with the one provided through the data source property.
      if (!StringUtils.isNullOrEmpty(database)) {
        PropertyDefinition.DATABASE.set(props, database);
      }

    } else {
      final String effectiveServerName = !StringUtils.isNullOrEmpty(serverName)
          ? serverName
          : props.getProperty(SERVER_NAME);
      final String effectiveServerPort = !StringUtils.isNullOrEmpty(serverPort)
          ? serverPort
          : props.getProperty(SERVER_PORT);
      final String databaseName = !StringUtils.isNullOrEmpty(database)
          ? database
          : PropertyDefinition.DATABASE.getString(props);

      if (StringUtils.isNullOrEmpty(effectiveServerName)) {
        throw new SQLException(Messages.get("AwsWrapperDataSource.missingTarget"));
      }
      if (StringUtils.isNullOrEmpty(jdbcProtocol)) {
        throw new SQLException(Messages.get("AwsWrapperDataSource.missingJdbcProtocol"));
      }

      int port = HostSpec.NO_PORT;
      if (!StringUtils.isNullOrEmpty(effectiveServerPort)) {
        port = Integer.parseInt(effectiveServerPort);
      }

      finalUrl = ConnectionUrlBuilder.buildUrl(jdbcProtocol, effectiveServerName, port, databaseName);

      // Override credentials with the ones provided through the data source property.
      setCredentialProperties(props, user, password);

      // Override database with the one provided through the data source property.
      if (!StringUtils.isNullOrEmpty(databaseName)) {
        PropertyDefinition.DATABASE.set(props, databaseName);
      }
    }

    return finalUrl;
  }

  /**
   * Returns {@code url} with any AWS Wrapper-specific query parameters removed, keeping only
   * target-driver parameters. This prevents wrapper parameters (e.g. {@code wrapperPlugins},
   * {@code wrapperDialect}, plugin-specific properties) from leaking into the URL handed to the
   * target driver, which some drivers reject as unknown parameters. Recognition uses the same
   * registry as {@link PropertyDefinition#removeAll(Properties)} (named and prefix-registered
   * wrapper properties), so it is only complete once the relevant plugin classes have been loaded.
   *
   * @param url the URL to sanitize (may be null or have no query string).
   * @return the URL with wrapper query parameters removed.
   */
  public static @Nullable String removeWrapperPropertiesFromUrl(final @Nullable String url) {
    if (StringUtils.isNullOrEmpty(url)) {
      return url;
    }
    final int queryStart = url.indexOf('?');
    if (queryStart < 0) {
      return url;
    }
    final String base = url.substring(0, queryStart);
    final String query = url.substring(queryStart + 1);
    if (StringUtils.isNullOrEmpty(query)) {
      return base;
    }

    // Collect the parameter names, then let PropertyDefinition.removeAll decide which are AWS
    // Wrapper properties; whatever it leaves behind are the target-driver parameters to keep.
    final Properties names = new Properties();
    for (final String token : query.split("&")) {
      if (StringUtils.isNullOrEmpty(token)) {
        continue;
      }
      final int eq = token.indexOf('=');
      names.setProperty(eq < 0 ? token : token.substring(0, eq), "");
    }
    PropertyDefinition.removeAll(names);

    final StringBuilder kept = new StringBuilder();
    for (final String token : query.split("&")) {
      if (StringUtils.isNullOrEmpty(token)) {
        continue;
      }
      final int eq = token.indexOf('=');
      final String name = eq < 0 ? token : token.substring(0, eq);
      if (!names.containsKey(name)) {
        continue; // recognized AWS Wrapper parameter; drop it from the target URL
      }
      if (kept.length() > 0) {
        kept.append('&');
      }
      kept.append(token); // preserve the original (encoded) token
    }
    return kept.length() == 0 ? base : base + "?" + kept.toString();
  }

  private static void setDatabasePropertyFromUrl(final Properties props, final @Nullable String jdbcUrl) {
    if (StringUtils.isNullOrEmpty(jdbcUrl)) {
      return;
    }
    final String databaseName = ConnectionUrlParser.parseDatabaseFromUrl(jdbcUrl);
    if (!StringUtils.isNullOrEmpty(databaseName)) {
      PropertyDefinition.DATABASE.set(props, databaseName);
    }
  }

  private static void setCredentialProperties(
      final Properties props, final @Nullable String user, final @Nullable String password) {
    if (!StringUtils.isNullOrEmpty(user)) {
      PropertyDefinition.USER.set(props, user);
    }
    if (!StringUtils.isNullOrEmpty(password)) {
      PropertyDefinition.PASSWORD.set(props, password);
    }
  }
}
