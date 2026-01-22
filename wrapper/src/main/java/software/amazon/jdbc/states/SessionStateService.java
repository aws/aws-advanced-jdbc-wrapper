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

package software.amazon.jdbc.states;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;

public interface SessionStateService {

  // auto commit
  Optional<Boolean> getAutoCommit() throws SQLException;

  void setAutoCommit(final boolean autoCommit) throws SQLException;

  void setupPristineAutoCommit() throws SQLException;

  void setupPristineAutoCommit(final boolean autoCommit) throws SQLException;

  // read-only
  Optional<Boolean> getReadOnly() throws SQLException;

  void setReadOnly(boolean readOnly) throws SQLException;

  void setupPristineReadOnly() throws SQLException;

  void setupPristineReadOnly(final boolean readOnly) throws SQLException;

  // catalog

  Optional<String> getCatalog() throws SQLException;

  void setCatalog(final String catalog) throws SQLException;

  void setupPristineCatalog() throws SQLException;

  void setupPristineCatalog(final String catalog) throws SQLException;

  // holdability

  Optional<Integer> getHoldability() throws SQLException;

  void setHoldability(final int holdability) throws SQLException;

  void setupPristineHoldability() throws SQLException;

  void setupPristineHoldability(final int holdability) throws SQLException;

  // network timeout

  Optional<Integer> getNetworkTimeout() throws SQLException;

  void setNetworkTimeout(final int milliseconds) throws SQLException;

  void setupPristineNetworkTimeout() throws SQLException;

  void setupPristineNetworkTimeout(final int milliseconds) throws SQLException;

  // schema

  Optional<String> getSchema() throws SQLException;

  void setSchema(final String schema) throws SQLException;

  void setupPristineSchema() throws SQLException;

  void setupPristineSchema(final String schema) throws SQLException;

  // transaction isolation

  Optional<Integer> getTransactionIsolation() throws SQLException;

  void setTransactionIsolation(final int level) throws SQLException;

  void setupPristineTransactionIsolation() throws SQLException;

  void setupPristineTransactionIsolation(final int level) throws SQLException;

  // type map

  Optional<Map<String, Class<?>>> getTypeMap() throws SQLException;

  void setTypeMap(final Map<String, Class<?>> map) throws SQLException;

  void setupPristineTypeMap() throws SQLException;

  void setupPristineTypeMap(final Map<String, Class<?>> map) throws SQLException;

  void reset();

  // Begin session transfer process
  void begin() throws SQLException;

  // Complete session transfer process. This method should be called despite whether
  // session transfer is successful or not.
  void complete();

  // Apply current session state (of the current connection) to a new connection.
  void applyCurrentSessionState(final Connection newConnection) throws SQLException;

  // Apply pristine values to the provided connection (practically resetting the connection to its original state).
  void applyPristineSessionState(final Connection connection) throws SQLException;
}
