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

package software.amazon.jdbc.plugin.readwritesplitting;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.hostlistprovider.HostListProviderService;
import software.amazon.jdbc.util.SqlState;

/**
 * Narrow interface through which read/write splitting helpers read and drive the unified plugin's
 * connection state. Helpers never touch the plugin's private fields directly; all state
 * transitions and logging/exception helpers are funneled through this context so that behavior
 * (and message keys) stay consistent with the legacy plugins.
 */
public interface RwSplitContext {

  PluginService pluginService();

  @Nullable HostListProviderService hostListProviderService();

  Properties properties();

  @Nullable Connection currentConnection();

  @Nullable HostSpec currentHostSpec();

  @Nullable Connection writerConnection();

  @Nullable Connection readerConnection();

  @Nullable HostSpec writerHostSpec();

  @Nullable HostSpec readerHostSpec();

  /**
   * Records the writer connection and host, preserving the legacy {@code setWriterConnection} log.
   *
   * @param conn the writer connection to record
   * @param host the host the writer connection is established to
   * @throws SQLException if the writer connection cannot be recorded
   */
  void bindWriter(Connection conn, HostSpec host) throws SQLException;

  /**
   * Records the resolved writer host (without a connection), e.g. after a topology refresh.
   *
   * @param host the resolved writer host
   */
  void setWriterHostSpec(HostSpec host);

  /**
   * Records (caches) the reader connection and host, preserving the legacy
   * {@code setReaderConnection} log.
   *
   * @param conn the reader connection to cache
   * @param host the host the reader connection is established to
   */
  void bindReader(Connection conn, HostSpec host);

  /**
   * Switches the wrapper's current connection to the given connection/host.
   *
   * @param conn the connection to switch to
   * @param host the host the connection is established to
   * @throws SQLException if the current connection cannot be switched
   */
  void switchCurrentConnectionTo(Connection conn, HostSpec host) throws SQLException;

  /**
   * Opens a connection to the given host via the plugin service (using this plugin as the caller).
   *
   * @param host the host to connect to
   * @param props the connection properties to connect with
   * @return the newly opened connection
   * @throws SQLException if the connection cannot be opened
   */
  Connection connect(HostSpec host, Properties props) throws SQLException;

  /** Marks that the plugin is actively read/write splitting (sets {@code inReadWriteSplit}). */
  void enterReadWriteSplit();

  boolean isInReadWriteSplit();

  boolean isConnectionUsable(@Nullable Connection conn) throws SQLException;

  void closeReaderConnectionIfIdle();

  void closeWriterConnectionIfIdle();

  void markReaderFromPool(boolean fromPool);

  void markWriterFromPool(boolean fromPool);

  boolean isReaderFromPool();

  boolean isWriterFromPool();

  /**
   * Logs the message at SEVERE and throws a {@link ReadWriteSplittingSQLException}.
   *
   * @param message the message to log and to report in the exception
   * @throws SQLException always, as a {@link ReadWriteSplittingSQLException}
   */
  void logAndThrow(String message) throws SQLException;

  /**
   * Logs the message at SEVERE and throws a {@link ReadWriteSplittingSQLException} with the SQL
   * state.
   *
   * @param message the message to log and to report in the exception
   * @param sqlState the SQL state to report in the exception
   * @throws SQLException always, as a {@link ReadWriteSplittingSQLException}
   */
  void logAndThrow(String message, SqlState sqlState) throws SQLException;
}
