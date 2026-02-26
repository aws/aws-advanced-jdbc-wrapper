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

package software.amazon.jdbc.plugin.encryption.model;

import java.util.Objects;
import java.util.Properties;

/**
 * Immutable data class that holds connection parameters extracted from a database connection. These
 * parameters can be used to create independent connections to the same database.
 */
public class ConnectionParameters {
  private final String jdbcUrl;
  private final String username;
  private final String password;
  private final Properties connectionProperties;
  private final String driverClassName;
  private final String catalog;
  private final String schema;

  private ConnectionParameters(Builder builder) {
    this.jdbcUrl = builder.jdbcUrl;
    this.username = builder.username;
    this.password = builder.password;
    this.connectionProperties = new Properties();
    if (builder.connectionProperties != null) {
      this.connectionProperties.putAll(builder.connectionProperties);
    }
    this.driverClassName = builder.driverClassName;
    this.catalog = builder.catalog;
    this.schema = builder.schema;
  }

  /**
   * Gets the JDBC URL for the database connection.
   *
   * @return the JDBC URL, never null
   */
  public String getJdbcUrl() {
    return jdbcUrl;
  }

  /**
   * Gets the username for database authentication.
   *
   * @return the username, may be null if using other authentication methods
   */
  public String getUsername() {
    return username;
  }

  /**
   * Gets the password for database authentication.
   *
   * @return the password, may be null if using other authentication methods
   */
  public String getPassword() {
    return password;
  }

  /**
   * Gets additional connection properties.
   *
   * @return a copy of the connection properties, never null
   */
  public Properties getConnectionProperties() {
    return new Properties(connectionProperties);
  }

  /**
   * Gets the JDBC driver class name.
   *
   * @return the driver class name, may be null if not specified
   */
  public String getDriverClassName() {
    return driverClassName;
  }

  /**
   * Gets the database catalog name.
   *
   * @return the catalog name, may be null
   */
  public String getCatalog() {
    return catalog;
  }

  /**
   * Gets the database schema name.
   *
   * @return the schema name, may be null
   */
  public String getSchema() {
    return schema;
  }

  /**
   * Checks if this connection uses username/password authentication.
   *
   * @return true if both username and password are present, false otherwise
   */
  public boolean hasCredentials() {
    return username != null && password != null;
  }

  /**
   * Creates a new Builder instance for constructing ConnectionParameters.
   *
   * @return a new Builder instance
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Creates a new Builder instance initialized with values from this instance.
   *
   * @return a new Builder instance with copied values
   */
  public Builder toBuilder() {
    return new Builder()
        .jdbcUrl(this.jdbcUrl)
        .username(this.username)
        .password(this.password)
        .connectionProperties(this.connectionProperties)
        .driverClassName(this.driverClassName)
        .catalog(this.catalog)
        .schema(this.schema);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ConnectionParameters that = (ConnectionParameters) o;
    return Objects.equals(jdbcUrl, that.jdbcUrl)
        && Objects.equals(username, that.username)
        && Objects.equals(password, that.password)
        && Objects.equals(connectionProperties, that.connectionProperties)
        && Objects.equals(driverClassName, that.driverClassName)
        && Objects.equals(catalog, that.catalog)
        && Objects.equals(schema, that.schema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        jdbcUrl, username, password, connectionProperties, driverClassName, catalog, schema);
  }

  @Override
  public String toString() {
    return "ConnectionParameters{"
        + "jdbcUrl='"
        + jdbcUrl
        + '\''
        + ", username='"
        + username
        + '\''
        + ", password='[REDACTED]'"
        + ", connectionProperties="
        + connectionProperties
        + ", driverClassName='"
        + driverClassName
        + '\''
        + ", catalog='"
        + catalog
        + '\''
        + ", schema='"
        + schema
        + '\''
        + '}';
  }

  /** Builder class for constructing ConnectionParameters instances. */
  public static class Builder {
    private String jdbcUrl;
    private String username;
    private String password;
    private Properties connectionProperties;
    private String driverClassName;
    private String catalog;
    private String schema;

    private Builder() {}

    /**
     * Sets the JDBC URL.
     *
     * @param jdbcUrl the JDBC URL, must not be null or empty
     * @return this Builder instance for method chaining
     * @throws IllegalArgumentException if jdbcUrl is null or empty
     */
    public Builder jdbcUrl(String jdbcUrl) {
      if (jdbcUrl == null || jdbcUrl.trim().isEmpty()) {
        throw new IllegalArgumentException("JDBC URL cannot be null or empty");
      }
      this.jdbcUrl = jdbcUrl.trim();
      return this;
    }

    /**
     * Sets the username for authentication.
     *
     * @param username the username, may be null
     * @return this Builder instance for method chaining
     */
    public Builder username(String username) {
      this.username = username;
      return this;
    }

    /**
     * Sets the password for authentication.
     *
     * @param password the password, may be null
     * @return this Builder instance for method chaining
     */
    public Builder password(String password) {
      this.password = password;
      return this;
    }

    /**
     * Sets the connection properties.
     *
     * @param connectionProperties the connection properties, may be null
     * @return this Builder instance for method chaining
     */
    public Builder connectionProperties(Properties connectionProperties) {
      this.connectionProperties = connectionProperties;
      return this;
    }

    /**
     * Sets the JDBC driver class name.
     *
     * @param driverClassName the driver class name, may be null
     * @return this Builder instance for method chaining
     */
    public Builder driverClassName(String driverClassName) {
      this.driverClassName = driverClassName;
      return this;
    }

    /**
     * Sets the database catalog name.
     *
     * @param catalog the catalog name, may be null
     * @return this Builder instance for method chaining
     */
    public Builder catalog(String catalog) {
      this.catalog = catalog;
      return this;
    }

    /**
     * Sets the database schema name.
     *
     * @param schema the schema name, may be null
     * @return this Builder instance for method chaining
     */
    public Builder schema(String schema) {
      this.schema = schema;
      return this;
    }

    /**
     * Builds a new ConnectionParameters instance.
     *
     * @return a new ConnectionParameters instance
     * @throws IllegalStateException if required fields are not set
     */
    public ConnectionParameters build() {
      if (jdbcUrl == null || jdbcUrl.trim().isEmpty()) {
        throw new IllegalStateException("JDBC URL is required");
      }
      return new ConnectionParameters(this);
    }
  }
}
