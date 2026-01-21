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

package software.amazon.jdbc.plugin.encryption.parser;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

/**
 * Detects SQL dialect from JDBC connection or URL.
 */
public final class DialectDetector {

  private DialectDetector() {
    // Utility class
  }

  /**
   * Detect dialect from JDBC connection.
   *
   * @param connection JDBC connection
   * @return Detected dialect
   */
  public static JSQLParserAnalyzer.Dialect detectFromConnection(Connection connection) {
    if (connection == null) {
      return JSQLParserAnalyzer.Dialect.POSTGRESQL; // Default
    }

    try {
      DatabaseMetaData metaData = connection.getMetaData();
      String productName = metaData.getDatabaseProductName().toLowerCase();
      return detectFromProductName(productName);
    } catch (SQLException e) {
      return JSQLParserAnalyzer.Dialect.POSTGRESQL; // Default on error
    }
  }

  /**
   * Detect dialect from JDBC URL.
   *
   * @param jdbcUrl JDBC connection URL
   * @return Detected dialect
   */
  public static JSQLParserAnalyzer.Dialect detectFromUrl(String jdbcUrl) {
    if (jdbcUrl == null || jdbcUrl.isEmpty()) {
      return JSQLParserAnalyzer.Dialect.POSTGRESQL; // Default
    }

    String url = jdbcUrl.toLowerCase();

    if (url.contains("mysql")) {
      return JSQLParserAnalyzer.Dialect.MYSQL;
    } else if (url.contains("mariadb")) {
      return JSQLParserAnalyzer.Dialect.MARIADB;
    } else if (url.contains("postgresql") || url.contains("postgres")) {
      return JSQLParserAnalyzer.Dialect.POSTGRESQL;
    }

    return JSQLParserAnalyzer.Dialect.POSTGRESQL; // Default
  }

  /**
   * Detect dialect from database product name.
   *
   * @param productName Database product name from metadata
   * @return Detected dialect
   */
  public static JSQLParserAnalyzer.Dialect detectFromProductName(String productName) {
    if (productName == null || productName.isEmpty()) {
      return JSQLParserAnalyzer.Dialect.POSTGRESQL; // Default
    }

    String name = productName.toLowerCase();

    if (name.contains("mysql")) {
      return JSQLParserAnalyzer.Dialect.MYSQL;
    } else if (name.contains("mariadb")) {
      return JSQLParserAnalyzer.Dialect.MARIADB;
    } else if (name.contains("postgresql") || name.contains("postgres")) {
      return JSQLParserAnalyzer.Dialect.POSTGRESQL;
    }

    return JSQLParserAnalyzer.Dialect.POSTGRESQL; // Default
  }
}
