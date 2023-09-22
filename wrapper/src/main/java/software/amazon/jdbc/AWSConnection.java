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

package software.amazon.jdbc;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;



public class AWSConnection implements Connection {

  private static final int READONLY_DIRTY_BIT   = 0b000001;
  private static final int AUTO_COMMIT_DIRTY = 0b000010;
  private static final int ISOLATION_DIRTY_BIT  = 0b000100;
  private static final int CATALOG_DIRTY_BIT    = 0b001000;
  private static final int NET_TIMEOUT_DIRTY_BIT = 0b010000;
  private static final int SCHEMA_DIRTY_BIT     = 0b100000;
  private static final int TYPE_MAP_DIRTY_BIT   = 0b1000000;
  private static final int HOLDABILITY_DIRTY_BIT = 0b10000000;

  private boolean inTransaction = true;
  private int dirtyBits = 0;
  private final Connection delegate;
  private  Executor executor;

  public AWSConnection(Connection delegate) {
    this.delegate = delegate;
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    if (delegate.getAutoCommit() != autoCommit) {
      dirtyBits |= AUTO_COMMIT_DIRTY;
    }
    delegate.setAutoCommit(autoCommit);
    this.inTransaction = !autoCommit;
  }

  @Override
  public void commit() throws SQLException {
    // commit resets autocommit so we need to remove the flag
    if ((dirtyBits & AUTO_COMMIT_DIRTY) != 0 && inTransaction) {
      dirtyBits ^= AUTO_COMMIT_DIRTY;
      inTransaction = false;
    }
    delegate.commit();
  }

  @Override
  public void rollback() throws SQLException {
    // rollback resets autocommit so we need to remove the flag
    if ((dirtyBits & AUTO_COMMIT_DIRTY) != 0 && inTransaction) {
      dirtyBits ^= AUTO_COMMIT_DIRTY;
      inTransaction = false;
    }
    delegate.rollback();
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    delegate.rollback(savepoint);
  }

  @Override
  public void close() throws SQLException {
    delegate.close();
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    dirtyBits |= READONLY_DIRTY_BIT;
    delegate.setReadOnly(readOnly);
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    dirtyBits |= CATALOG_DIRTY_BIT;
    delegate.setCatalog(catalog);
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    dirtyBits |= ISOLATION_DIRTY_BIT;
    delegate.setTransactionIsolation(level);
  }

  @Override
  public void clearWarnings() throws SQLException {
    delegate.clearWarnings();
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    delegate.releaseSavepoint(savepoint);
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    dirtyBits |= TYPE_MAP_DIRTY_BIT;
    delegate.setTypeMap(map);
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    dirtyBits |= HOLDABILITY_DIRTY_BIT;
    delegate.setHoldability(holdability);
  }

  @Override
  public boolean isClosed() throws SQLException {
    return delegate.isClosed();
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return delegate.isReadOnly();
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    return delegate.getAutoCommit();
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    return delegate.nativeSQL(sql);
  }

  @Override
  public String getCatalog() throws SQLException {
    return delegate.getCatalog();
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    return delegate.getTransactionIsolation();
  }

  @Override
  public int getHoldability() throws SQLException {
    return delegate.getHoldability();
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    return delegate.getMetaData();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return delegate.getWarnings();
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    return delegate.getTypeMap();
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    return delegate.setSavepoint();
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    return delegate.setSavepoint(name);
  }

  @Override
  public Statement createStatement() throws SQLException {
    return delegate.createStatement();
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
    return delegate.createStatement(resultSetType, resultSetConcurrency);
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    return delegate.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    return delegate.prepareStatement(sql);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    return delegate.prepareStatement(sql, resultSetType, resultSetConcurrency);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    return delegate.prepareStatement(sql, resultSetType, resultSetConcurrency);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    return delegate.prepareStatement(sql, autoGeneratedKeys);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    return delegate.prepareStatement(sql, columnIndexes);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    return delegate.prepareStatement(sql, columnNames);
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    return delegate.prepareCall(sql);
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    return delegate.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    return delegate.prepareCall(sql, resultSetType, resultSetConcurrency);
  }

  @Override
  public Clob createClob() throws SQLException {
    return delegate.createClob();
  }

  @Override
  public Blob createBlob() throws SQLException {
    return delegate.createBlob();
  }

  @Override
  public NClob createNClob() throws SQLException {
    return delegate.createNClob();
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    return delegate.createSQLXML();
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    return delegate.isValid(timeout);
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    delegate.setClientInfo(name, value);
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    delegate.setClientInfo(properties);
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    return delegate.getClientInfo(name);
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    return delegate.getClientInfo();
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    return delegate.createArrayOf(typeName, elements);
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    return delegate.createStruct(typeName, attributes);
  }

  @Override
  public void setSchema(String schema) throws SQLException {
    dirtyBits |= SCHEMA_DIRTY_BIT;
    delegate.setSchema(schema);
  }

  @Override
  public String getSchema() throws SQLException {
    return delegate.getSchema();
  }

  @Override
  public void abort(Executor executor) throws SQLException {
    delegate.abort(executor);
  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    this.executor = executor;
    dirtyBits |= NET_TIMEOUT_DIRTY_BIT;
    delegate.setNetworkTimeout(executor, milliseconds);
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    return delegate.getNetworkTimeout();
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return delegate.unwrap(iface);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return delegate.isWrapperFor(iface);
  }

  public void updateConnection(AWSConnection connection) throws SQLException {

    if ((dirtyBits & READONLY_DIRTY_BIT)  != 0) {
      connection.setReadOnly(this.isReadOnly());
    }
    if ((dirtyBits & AUTO_COMMIT_DIRTY)  != 0) {
      connection.setAutoCommit(this.getAutoCommit());
    }
    if ((dirtyBits & ISOLATION_DIRTY_BIT)  != 0) {
      connection.setTransactionIsolation(this.getTransactionIsolation());
    }
    if ((dirtyBits & CATALOG_DIRTY_BIT) != 0) {
      connection.setCatalog(this.getCatalog());
    }
    if ((dirtyBits & NET_TIMEOUT_DIRTY_BIT)  != 0) {
      connection.setNetworkTimeout(executor, this.getNetworkTimeout());
    }
    if ((dirtyBits & SCHEMA_DIRTY_BIT)  != 0) {
      connection.setSchema(this.getSchema());
    }
    if ((dirtyBits & TYPE_MAP_DIRTY_BIT) != 0) {
      connection.setTypeMap(this.getTypeMap());
    }
    if ((dirtyBits & HOLDABILITY_DIRTY_BIT) != 0) {
      connection.setHoldability(this.getHoldability());
    }
  }
}
