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

package software.amazon.jdbc.plugin.encryption;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;
import software.amazon.jdbc.ConnectionPlugin;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.NodeChangeOptions;
import software.amazon.jdbc.OldConnectionSuggestedAction;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostlistprovider.HostListProviderService;

/**
 * ConnectionPlugin implementation that integrates KmsEncryptionPlugin with AWS JDBC Wrapper. This
 * class acts as a bridge between the AWS JDBC Wrapper plugin system and our encryption
 * functionality.
 */
public class KmsEncryptionConnectionPlugin implements ConnectionPlugin {

  private static final Logger LOGGER =
      Logger.getLogger(KmsEncryptionConnectionPlugin.class.getName());

  private final KmsEncryptionPlugin encryptionPlugin;
  private final PluginService pluginService;

  public static final String KMS_ENCRYPTION_PLUGIN_CODE = "kmsEncryption";

  /**
   * Constructor that creates the encryption plugin with PluginService.
   *
   * @param pluginService The PluginService instance from AWS JDBC Wrapper
   * @param properties Configuration properties
   */
  public KmsEncryptionConnectionPlugin(PluginService pluginService, Properties properties) {
    this.pluginService = pluginService;
    this.encryptionPlugin = new KmsEncryptionPlugin(pluginService);

    try {
      this.encryptionPlugin.initialize(properties);
      LOGGER.info(() -> "KmsEncryptionConnectionPlugin initialized successfully");
    } catch (SQLException e) {
      LOGGER.severe(
          () ->
              String.format(
                  "Failed to initialize KmsEncryptionConnectionPlugin %s", e.getMessage()));
      throw new RuntimeException("Failed to initialize encryption plugin", e);
    }
  }

  /**
   * Returns the underlying encryption plugin.
   *
   * @return KmsEncryptionPlugin instance
   */
  public KmsEncryptionPlugin getEncryptionPlugin() {
    return encryptionPlugin;
  }

  /**
   * Executes JDBC method calls and applies encryption/decryption wrapping when needed.
   *
   * @param <T> Return type
   * @param <E> Exception type
   * @param methodClass Method class
   * @param methodReturnType Return type class
   * @param methodInvokeOn Object to invoke method on
   * @param methodName Method name
   * @param jdbcCallable Callable to execute
   * @param args Method arguments
   * @return Method result, potentially wrapped with encryption/decryption
   * @throws E if method execution fails
   */
  @Override
  public <T, E extends Exception> T execute(
      Class<T> methodClass,
      Class<E> methodReturnType,
      Object methodInvokeOn,
      String methodName,
      JdbcCallable<T, E> jdbcCallable,
      Object... args)
      throws E {
    // Execute the original method first
    T result = jdbcCallable.call();

    try {
      // Apply encryption/decryption wrapping if needed
      if (result instanceof java.sql.PreparedStatement
          && args.length > 0
          && args[0] instanceof String) {
        String sql = (String) args[0];
        @SuppressWarnings("unchecked")
        T wrappedResult =
            (T) encryptionPlugin.wrapPreparedStatement((java.sql.PreparedStatement) result, sql);
        return wrappedResult;
      } else if (result instanceof java.sql.ResultSet) {
        @SuppressWarnings("unchecked")
        T wrappedResult = (T) encryptionPlugin.wrapResultSet((java.sql.ResultSet) result);
        return wrappedResult;
      }
    } catch (SQLException e) {
      // If E is SQLException or a superclass, we can throw it
      if (methodReturnType.isAssignableFrom(SQLException.class)) {
        @SuppressWarnings("unchecked")
        E exception = (E) e;
        throw exception;
      } else {
        // Otherwise wrap in RuntimeException
        throw new RuntimeException("Failed to wrap JDBC object with encryption", e);
      }
    }

    return result;
  }

  /**
   * Delegates connection creation to the original function.
   *
   * @param driverProtocol Driver protocol
   * @param hostSpec Host specification
   * @param props Connection properties
   * @param isInitialConnection Whether this is initial connection
   * @param connectFunc Connection function
   * @return Database connection
   * @throws SQLException if connection fails
   */
  @Override
  public Connection connect(
      String driverProtocol,
      HostSpec hostSpec,
      Properties props,
      boolean isInitialConnection,
      JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {
    // Delegate to the original connection function
    return connectFunc.call();
  }

  /**
   * Returns the set of JDBC methods this plugin subscribes to.
   *
   * @return Set of method names to intercept
   */
  @Override
  public Set<String> getSubscribedMethods() {
    // Subscribe to PreparedStatement and ResultSet creation methods
    return new HashSet<>(
        Arrays.asList(
            "Connection.prepareStatement",
            "Connection.prepareCall",
            "Statement.executeQuery",
            "PreparedStatement.executeQuery"));
  }

  /**
   * Delegates host provider initialization to the original function.
   *
   * @param driverProtocol Driver protocol
   * @param initialUrl Initial URL
   * @param props Properties
   * @param hostListProviderService Host list provider service
   * @param initFunc Initialization function
   * @throws SQLException if initialization fails
   */
  @Override
  public void initHostProvider(
      String driverProtocol,
      String initialUrl,
      Properties props,
      HostListProviderService hostListProviderService,
      JdbcCallable<Void, SQLException> initFunc)
      throws SQLException {
    // Delegate to the original initialization
    initFunc.call();
  }

  /**
   * Handles node list change notifications (no action needed for encryption).
   *
   * @param changes Map of node changes
   */
  @Override
  public void notifyNodeListChanged(Map<String, EnumSet<NodeChangeOptions>> changes) {
    // No action needed for encryption plugin
  }

  /**
   * Accepts all strategies since encryption is transparent.
   *
   * @param role Host role
   * @param strategy Strategy name
   * @return Always true
   */
  @Override
  public boolean acceptsStrategy(HostRole role, String strategy) {
    // Accept all strategies - encryption is transparent
    return true;
  }

  /**
   * Not supported - encryption plugin does not provide host selection.
   *
   * @param role Host role
   * @param strategy Strategy name
   * @return Never returns
   * @throws SQLException Always throws UnsupportedOperationException
   */
  @Override
  public HostSpec getHostSpecByStrategy(HostRole role, String strategy) throws SQLException {
    throw new UnsupportedOperationException("Encryption plugin does not provide host selection");
  }

  /**
   * Not supported - encryption plugin does not provide host selection.
   *
   * @param hosts List of host specs
   * @param role Host role
   * @param strategy Strategy name
   * @return Never returns
   * @throws SQLException Always throws UnsupportedOperationException
   */
  public HostSpec getHostSpecByStrategy(List<HostSpec> hosts, HostRole role, String strategy)
      throws SQLException {
    throw new UnsupportedOperationException("Encryption plugin does not provide host selection");
  }

  /**
   * Forces connection creation by delegating to the original function.
   *
   * @param driverProtocol Driver protocol
   * @param hostSpec Host specification
   * @param props Connection properties
   * @param isInitialConnection Whether this is initial connection
   * @param connectFunc Connection function
   * @return Database connection
   * @throws SQLException if connection fails
   */
  @Override
  public Connection forceConnect(
      String driverProtocol,
      HostSpec hostSpec,
      Properties props,
      boolean isInitialConnection,
      JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {
    // Delegate to the original connection function
    return connectFunc.call();
  }

  /**
   * Handles connection change notifications (no special action needed).
   *
   * @param changes Set of node change options
   * @return NO_OPINION - no special action required
   */
  @Override
  public OldConnectionSuggestedAction notifyConnectionChanged(EnumSet<NodeChangeOptions> changes) {
    // No special action needed for connection changes
    return OldConnectionSuggestedAction.NO_OPINION;
  }
}
