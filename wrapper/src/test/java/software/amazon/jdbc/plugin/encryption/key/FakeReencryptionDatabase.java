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

package software.amazon.jdbc.plugin.encryption.key;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Predicate;

/**
 * In-memory fake of the JDBC calls made by {@link KeyManagementUtility} and {@link KeyManager}
 * during data key rotation. It answers by SQL shape: the key_storage and encryption_metadata
 * queries, the existence probe and keyset page of one data table, the conditional row update,
 * PostgreSQL set_config and MySQL user variables. Writes made with auto-commit off are staged and
 * applied on commit, discarded on rollback.
 */
final class FakeReencryptionDatabase {

  static final class KeyRow {
    final int id;
    final String name;
    final String masterKeyArn;
    final String encryptedDataKey;
    final byte[] hmacKey;

    KeyRow(int id, String name, String masterKeyArn, String encryptedDataKey, byte[] hmacKey) {
      this.id = id;
      this.name = name;
      this.masterKeyArn = masterKeyArn;
      this.encryptedDataKey = encryptedDataKey;
      this.hmacKey = hmacKey;
    }
  }

  private final boolean postgres;
  private final String metadataTableName;
  private final String metadataColumnName;
  private final Map<Integer, KeyRow> keys = new TreeMap<>();
  private final TreeMap<Long, byte[]> rows = new TreeMap<>();
  private final List<Runnable> staged = new ArrayList<>();
  private final List<String> executedSql = new ArrayList<>();
  private final List<String[]> setConfigCalls = new ArrayList<>();
  private final List<String[]> metadataLookups = new ArrayList<>();
  private final Map<String, String> sessionSettings = new HashMap<>();
  private final Map<String, String> settingUndo = new HashMap<>();
  private final Map<String, byte[]> userVariables = new HashMap<>();
  private final List<String> userVariableCalls = new ArrayList<>();
  private final List<Long> rowUpdatePks = new ArrayList<>();
  private final Connection connection;

  private String storedTableName;
  private String storedColumnName;
  private boolean caseInsensitiveMetadataMatch;
  private String physicalSchema;
  private String physicalTable;
  private String physicalPrimaryKey = "id";
  private String physicalColumn;

  private int metadataKeyId = -1;
  private String algorithm = "AES-256-GCM";
  private int nextKeyId = 1;
  private int lastInsertedKeyId = -1;
  private boolean autoCommit = true;
  private int commitCount;
  private int rollbackCount;
  private int executeBatchCount;
  private int failOnExecuteBatchNumber = -1;
  private int failOnCommitNumber = -1;
  private boolean applyBeforeCommitFailure;
  private boolean forceSwitchConflict;
  private boolean failOnSwitchUpdate;
  private boolean failOnKeyDelete;
  private boolean failOnSetConfig;
  private boolean pluginConnection;

  /**
   * Creates the fake. A schema prefix in {@code tableName} ({@code app.users}) is the physical
   * schema of the data table; the metadata stores the bare table name.
   */
  FakeReencryptionDatabase(boolean postgres, String tableName, String columnName) {
    this.postgres = postgres;
    final int dot = tableName.lastIndexOf('.');
    this.physicalSchema = dot < 0 ? null : tableName.substring(0, dot);
    this.metadataTableName = tableName.substring(dot + 1);
    this.metadataColumnName = columnName;
    this.storedTableName = metadataTableName;
    this.storedColumnName = columnName;
    this.physicalTable = metadataTableName;
    this.physicalColumn = columnName;
    this.connection = (Connection) Proxy.newProxyInstance(
        getClass().getClassLoader(), new Class<?>[] {Connection.class}, this::handleConnection);
  }

  Connection getConnection() {
    return connection;
  }

  // ---- test setup and inspection ------------------------------------------------------------

  int addKey(String name, String masterKeyArn, String encryptedDataKey, byte[] hmacKey) {
    final int id = nextKeyId++;
    keys.put(id, new KeyRow(id, name, masterKeyArn, encryptedDataKey, hmacKey.clone()));
    return id;
  }

  void setMetadataKeyId(int keyId) {
    this.metadataKeyId = keyId;
  }

  int getMetadataKeyId() {
    return metadataKeyId;
  }

  /** Names returned in the table_name and column_name columns of the metadata lookup. */
  void setStoredNames(String table, String column) {
    this.storedTableName = table;
    this.storedColumnName = column;
  }

  /** Simulates a case-insensitive collation of encryption_metadata (MySQL default). */
  void setCaseInsensitiveMetadataMatch(boolean caseInsensitive) {
    this.caseInsensitiveMetadataMatch = caseInsensitive;
  }

  /** Names that exist in the data table; the existence probe fails for any other spelling. */
  void setPhysicalNames(String schema, String table, String primaryKey, String column) {
    this.physicalSchema = schema;
    this.physicalTable = table;
    this.physicalPrimaryKey = primaryKey;
    this.physicalColumn = column;
  }

  List<String[]> getMetadataLookups() {
    return Collections.unmodifiableList(metadataLookups);
  }

  KeyRow getKey(int id) {
    return keys.get(id);
  }

  int getKeyCount() {
    return keys.size();
  }

  int getLastInsertedKeyId() {
    return lastInsertedKeyId;
  }

  void putRow(long pk, byte[] value) {
    rows.put(pk, value.clone());
  }

  byte[] getRow(long pk) {
    final byte[] value = rows.get(pk);
    return value == null ? null : value.clone();
  }

  Map<Long, byte[]> getRows() {
    final Map<Long, byte[]> copy = new TreeMap<>();
    for (Map.Entry<Long, byte[]> e : rows.entrySet()) {
      copy.put(e.getKey(), e.getValue() == null ? null : e.getValue().clone());
    }
    return copy;
  }

  List<String> getExecutedSql() {
    return Collections.unmodifiableList(executedSql);
  }

  /** Number of prepared statements whose SQL matches. */
  long countPrepared(Predicate<String> sqlMatcher) {
    return executedSql.stream().filter(sqlMatcher).count();
  }

  List<String[]> getSetConfigCalls() {
    return Collections.unmodifiableList(setConfigCalls);
  }

  /** The PostgreSQL session setting as the session sees it now (committed or not). */
  String getSessionSetting(String name) {
    return sessionSettings.get(name);
  }

  /** The MySQL user variable, name with or without the leading {@code @}, case-insensitive. */
  byte[] getUserVariable(String name) {
    final byte[] value = userVariables.get(normalizeVariable(name));
    return value == null ? null : value.clone();
  }

  List<String> getUserVariableCalls() {
    return Collections.unmodifiableList(userVariableCalls);
  }

  List<Long> getRowUpdatePks() {
    return Collections.unmodifiableList(rowUpdatePks);
  }

  int getCommitCount() {
    return commitCount;
  }

  int getRollbackCount() {
    return rollbackCount;
  }

  boolean isAutoCommit() {
    return autoCommit;
  }

  void failOnExecuteBatchNumber(int n) {
    this.failOnExecuteBatchNumber = n;
  }

  /** Commit number {@code n} fails; with {@code applyBeforeFailing} its changes are applied first. */
  void failOnCommitNumber(int n, boolean applyBeforeFailing) {
    this.failOnCommitNumber = n;
    this.applyBeforeCommitFailure = applyBeforeFailing;
  }

  void forceSwitchConflict() {
    this.forceSwitchConflict = true;
  }

  void failOnSwitchUpdate() {
    this.failOnSwitchUpdate = true;
  }

  void failOnKeyDelete() {
    this.failOnKeyDelete = true;
  }

  void failOnSetConfig() {
    this.failOnSetConfig = true;
  }

  void setPluginConnection(boolean pluginConnection) {
    this.pluginConnection = pluginConnection;
  }

  // ---- JDBC fakes ----------------------------------------------------------------------------

  private void write(Runnable change) {
    if (autoCommit) {
      change.run();
    } else {
      staged.add(change);
    }
  }

  private Object handleConnection(Object proxy, java.lang.reflect.Method method, Object[] args)
      throws Throwable {
    switch (method.getName()) {
      case "prepareStatement":
        executedSql.add((String) args[0]);
        return newStatement((String) args[0]);
      case "getAutoCommit":
        return autoCommit;
      case "setAutoCommit":
        if (!autoCommit && (Boolean) args[0]) {
          applyStaged();
        }
        autoCommit = (Boolean) args[0];
        return null;
      case "commit":
        commitCount++;
        if (commitCount == failOnCommitNumber) {
          if (applyBeforeCommitFailure) {
            applyStaged();
          } else {
            discardStaged();
          }
          throw new SQLException("Injected failure on commit " + commitCount);
        }
        applyStaged();
        return null;
      case "rollback":
        rollbackCount++;
        discardStaged();
        return null;
      case "isWrapperFor":
        return pluginConnection;
      case "close":
        return null;
      case "isClosed":
        return false;
      case "toString":
        return "FakeReencryptionDatabaseConnection";
      case "hashCode":
        return System.identityHashCode(proxy);
      case "equals":
        return proxy == args[0];
      default:
        throw new UnsupportedOperationException(method.getName());
    }
  }

  private void applyStaged() {
    for (Runnable change : staged) {
      change.run();
    }
    staged.clear();
    settingUndo.clear();
  }

  private void discardStaged() {
    staged.clear();
    for (Map.Entry<String, String> e : settingUndo.entrySet()) {
      if (e.getValue() == null) {
        sessionSettings.remove(e.getKey());
      } else {
        sessionSettings.put(e.getKey(), e.getValue());
      }
    }
    settingUndo.clear();
  }

  private PreparedStatement newStatement(String sql) {
    final Map<Integer, Object> params = new HashMap<>();
    final List<Map<Integer, Object>> batch = new ArrayList<>();
    final ResultSet[] generatedKeys = new ResultSet[1];
    final InvocationHandler handler = (proxy, method, args) -> {
      final String name = method.getName();
      if (name.startsWith("set") && args != null && args.length >= 2 && args[0] instanceof Integer) {
        params.put((Integer) args[0], args[1] instanceof byte[] ? ((byte[]) args[1]).clone() : args[1]);
        return null;
      }
      switch (name) {
        case "executeQuery":
          return executeQuery(sql, params);
        case "executeUpdate":
          final int[] result = new int[1];
          final ResultSet keysRs = executeUpdate(sql, params, result);
          generatedKeys[0] = keysRs;
          return result[0];
        case "getGeneratedKeys":
          return generatedKeys[0];
        case "addBatch":
          batch.add(new HashMap<>(params));
          return null;
        case "clearBatch":
          batch.clear();
          return null;
        case "executeBatch":
          return executeBatch(sql, batch);
        case "clearParameters":
          params.clear();
          return null;
        case "close":
          return null;
        case "toString":
          return "FakeStatement[" + sql + "]";
        case "hashCode":
          return System.identityHashCode(proxy);
        case "equals":
          return proxy == args[0];
        default:
          throw new UnsupportedOperationException(name);
      }
    };
    return (PreparedStatement) Proxy.newProxyInstance(
        getClass().getClassLoader(), new Class<?>[] {PreparedStatement.class}, handler);
  }

  private boolean metadataMatches(Object table, Object column) {
    if (caseInsensitiveMetadataMatch) {
      return metadataTableName.equalsIgnoreCase((String) table)
          && metadataColumnName.equalsIgnoreCase((String) column);
    }
    return metadataTableName.equals(table) && metadataColumnName.equals(column);
  }

  private String quote(String identifier) {
    return postgres ? "\"" + identifier + "\"" : "`" + identifier + "`";
  }

  private String probeSql() {
    final String table = physicalSchema == null
        ? quote(physicalTable) : quote(physicalSchema) + "." + quote(physicalTable);
    return "SELECT " + quote(physicalPrimaryKey) + ", " + quote(physicalColumn) + " FROM " + table + " WHERE 1=0";
  }

  private ResultSet executeQuery(String sql, Map<Integer, Object> params) throws SQLException {
    if (sql.contains("set_config")) {
      final String name = (String) params.get(1);
      final String value = (String) params.get(2);
      setConfigCalls.add(new String[] {name, value});
      if (failOnSetConfig) {
        throw new SQLException("Injected failure on set_config");
      }
      if (!autoCommit && !settingUndo.containsKey(name)) {
        settingUndo.put(name, sessionSettings.get(name));
      }
      sessionSettings.put(name, value);
      return resultSet(new String[] {"set_config"}, Collections.singletonList(new Object[] {value}));
    }
    if (sql.startsWith("SELECT em.key_id")) {
      metadataLookups.add(new String[] {(String) params.get(1), (String) params.get(2)});
      if (!metadataMatches(params.get(1), params.get(2)) || metadataKeyId < 0) {
        return resultSet(new String[0], Collections.emptyList());
      }
      final KeyRow key = keys.get(metadataKeyId);
      return resultSet(
          new String[] {"key_id", "encryption_algorithm", "table_name", "column_name", "master_key_arn",
              "encrypted_data_key", "hmac_key"},
          Collections.singletonList(new Object[] {
              key.id, algorithm, storedTableName, storedColumnName, key.masterKeyArn, key.encryptedDataKey,
              key.hmacKey.clone()}));
    }
    if (sql.startsWith("SELECT key_id FROM") && sql.contains("encryption_metadata")) {
      if (!metadataMatches(params.get(1), params.get(2)) || metadataKeyId < 0) {
        return resultSet(new String[0], Collections.emptyList());
      }
      return resultSet(new String[] {"key_id"}, Collections.singletonList(new Object[] {metadataKeyId}));
    }
    if (sql.startsWith("SELECT id, master_key_arn")) {
      final KeyRow key = keys.get((Integer) params.get(1));
      if (key == null) {
        return resultSet(new String[0], Collections.emptyList());
      }
      return resultSet(
          new String[] {"id", "master_key_arn", "encrypted_data_key", "hmac_key"},
          Collections.singletonList(new Object[] {
              key.id, key.masterKeyArn, key.encryptedDataKey, key.hmacKey.clone()}));
    }
    if (sql.startsWith("INSERT INTO") && sql.contains("key_storage") && postgres) {
      final int id = insertKey(params);
      return resultSet(new String[] {"id"}, Collections.singletonList(new Object[] {id}));
    }
    if (sql.endsWith(" WHERE 1=0")) {
      if (!sql.equals(probeSql())) {
        throw new SQLException("relation or column does not exist: " + sql);
      }
      return resultSet(new String[] {"pk", "value"}, Collections.emptyList());
    }
    if (sql.startsWith("SELECT ") && sql.contains(" LIMIT ?")) {
      final boolean hasLastPk = sql.contains(" > ?");
      final long lastPk = hasLastPk ? ((Number) params.get(1)).longValue() : Long.MIN_VALUE;
      final int limit = (Integer) params.get(hasLastPk ? 2 : 1);
      final List<Object[]> page = new ArrayList<>();
      for (Map.Entry<Long, byte[]> e : rows.tailMap(lastPk, !hasLastPk).entrySet()) {
        if (page.size() >= limit) {
          break;
        }
        if (e.getValue() != null) {
          page.add(new Object[] {e.getKey(), e.getValue().clone()});
        }
      }
      return resultSet(new String[] {"pk", "value"}, page);
    }
    throw new SQLException("Unexpected query: " + sql);
  }

  private ResultSet executeUpdate(String sql, Map<Integer, Object> params, int[] result) throws SQLException {
    if (sql.startsWith("INSERT INTO") && sql.contains("key_storage") && !postgres) {
      final int id = insertKey(params);
      result[0] = 1;
      return resultSet(new String[] {"GENERATED_KEY"}, Collections.singletonList(new Object[] {id}));
    }
    if (sql.startsWith("UPDATE") && sql.contains("encryption_metadata")) {
      final int newKeyId = (Integer) params.get(1);
      final boolean matches = metadataMatches(params.get(3), params.get(4));
      final boolean compareAndSet = sql.contains("AND key_id = ?");
      if (compareAndSet && failOnSwitchUpdate) {
        throw new SQLException("Injected failure on the switch update");
      }
      final boolean casOk = !compareAndSet
          || (!forceSwitchConflict && metadataKeyId == (Integer) params.get(5));
      if (matches && casOk) {
        write(() -> metadataKeyId = newKeyId);
        result[0] = 1;
      } else {
        result[0] = 0;
      }
      return null;
    }
    if (sql.startsWith("DELETE FROM") && sql.contains("key_storage") && sql.endsWith("WHERE id = ?")) {
      if (failOnKeyDelete) {
        throw new SQLException("Injected failure on the key delete");
      }
      final int id = (Integer) params.get(1);
      result[0] = keys.containsKey(id) ? 1 : 0;
      write(() -> keys.remove(id));
      return null;
    }
    if (sql.startsWith("SET @") && sql.endsWith(" = ?")) {
      final String name = sql.substring("SET ".length(), sql.length() - " = ?".length());
      userVariableCalls.add(name);
      userVariables.put(normalizeVariable(name), ((byte[]) params.get(1)).clone());
      result[0] = 0;
      return null;
    }
    throw new SQLException("Unexpected update: " + sql);
  }

  private static String normalizeVariable(String name) {
    final String bare = name.startsWith("@") ? name.substring(1) : name;
    return bare.toLowerCase(Locale.ROOT);
  }

  private int insertKey(Map<Integer, Object> params) {
    final int id = nextKeyId++;
    keys.put(id, new KeyRow(id, (String) params.get(1), (String) params.get(2), (String) params.get(3),
        ((byte[]) params.get(4)).clone()));
    lastInsertedKeyId = id;
    return id;
  }

  private int[] executeBatch(String sql, List<Map<Integer, Object>> batch) throws SQLException {
    executeBatchCount++;
    if (executeBatchCount == failOnExecuteBatchNumber) {
      batch.clear();
      throw new SQLException("Injected failure on executeBatch " + executeBatchCount);
    }
    if (!sql.startsWith("UPDATE") || !sql.contains(" SET ")) {
      throw new SQLException("Unexpected batch: " + sql);
    }
    final int[] counts = new int[batch.size()];
    for (int i = 0; i < batch.size(); i++) {
      final Map<Integer, Object> p = batch.get(i);
      final byte[] newValue = (byte[]) p.get(1);
      final long pk = ((Number) p.get(2)).longValue();
      final byte[] expected = (byte[]) p.get(3);
      if (rows.containsKey(pk) && Arrays.equals(rows.get(pk), expected)) {
        counts[i] = 1;
        rowUpdatePks.add(pk);
        write(() -> rows.put(pk, newValue));
      } else {
        counts[i] = 0;
      }
    }
    batch.clear();
    return counts;
  }

  private static ResultSet resultSet(String[] labels, List<Object[]> data) {
    final int[] cursor = {-1};
    final InvocationHandler handler = (proxy, method, args) -> {
      switch (method.getName()) {
        case "next":
          cursor[0]++;
          return cursor[0] < data.size();
        case "getObject":
        case "getBytes":
        case "getString":
          return column(labels, data.get(cursor[0]), args[0]);
        case "getInt":
          final Object value = column(labels, data.get(cursor[0]), args[0]);
          return value == null ? 0 : ((Number) value).intValue();
        case "close":
          return null;
        case "toString":
          return "FakeResultSet";
        case "hashCode":
          return System.identityHashCode(proxy);
        case "equals":
          return proxy == args[0];
        default:
          throw new UnsupportedOperationException(method.getName());
      }
    };
    return (ResultSet) Proxy.newProxyInstance(
        FakeReencryptionDatabase.class.getClassLoader(), new Class<?>[] {ResultSet.class}, handler);
  }

  private static Object column(String[] labels, Object[] row, Object indexOrLabel) throws SQLException {
    if (indexOrLabel instanceof Integer) {
      return row[(Integer) indexOrLabel - 1];
    }
    for (int i = 0; i < labels.length; i++) {
      if (labels[i].equals(indexOrLabel)) {
        return row[i];
      }
    }
    throw new SQLException("Unknown column " + indexOrLabel);
  }
}
