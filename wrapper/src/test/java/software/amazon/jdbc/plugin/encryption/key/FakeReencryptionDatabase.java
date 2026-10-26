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
import java.util.Map;
import java.util.TreeMap;

/**
 * In-memory fake of the JDBC calls made by {@link KeyManagementUtility} and {@link KeyManager}
 * during data key rotation. It answers by SQL shape: the key_storage and encryption_metadata
 * queries, the keyset page of one data table, the conditional row update, and set_config. Writes
 * made with auto-commit off are staged and applied on commit, discarded on rollback.
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
  private final String tableName;
  private final String columnName;
  private final Map<Integer, KeyRow> keys = new TreeMap<>();
  private final TreeMap<Long, byte[]> rows = new TreeMap<>();
  private final List<Runnable> staged = new ArrayList<>();
  private final List<String> executedSql = new ArrayList<>();
  private final List<String[]> setConfigCalls = new ArrayList<>();
  private final List<Long> rowUpdatePks = new ArrayList<>();
  private final Connection connection;

  private int metadataKeyId = -1;
  private String algorithm = "AES-256-GCM";
  private int nextKeyId = 1;
  private boolean autoCommit = true;
  private int commitCount;
  private int rollbackCount;
  private int executeBatchCount;
  private int failOnExecuteBatchNumber = -1;
  private boolean forceSwitchConflict;
  private boolean pluginConnection;

  FakeReencryptionDatabase(boolean postgres, String tableName, String columnName) {
    this.postgres = postgres;
    this.tableName = tableName;
    this.columnName = columnName;
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

  KeyRow getKey(int id) {
    return keys.get(id);
  }

  int getKeyCount() {
    return keys.size();
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

  List<String[]> getSetConfigCalls() {
    return Collections.unmodifiableList(setConfigCalls);
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

  void forceSwitchConflict() {
    this.forceSwitchConflict = true;
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
        applyStaged();
        return null;
      case "rollback":
        rollbackCount++;
        staged.clear();
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

  private ResultSet executeQuery(String sql, Map<Integer, Object> params) throws SQLException {
    if (sql.contains("set_config")) {
      setConfigCalls.add(new String[] {(String) params.get(1), (String) params.get(2)});
      return resultSet(new String[] {"set_config"}, Collections.singletonList(new Object[] {params.get(2)}));
    }
    if (sql.startsWith("SELECT em.key_id")) {
      if (!tableName.equals(params.get(1)) || !columnName.equals(params.get(2)) || metadataKeyId < 0) {
        return resultSet(new String[0], Collections.emptyList());
      }
      final KeyRow key = keys.get(metadataKeyId);
      return resultSet(
          new String[] {"key_id", "encryption_algorithm", "master_key_arn", "encrypted_data_key", "hmac_key"},
          Collections.singletonList(new Object[] {
              key.id, algorithm, key.masterKeyArn, key.encryptedDataKey, key.hmacKey.clone()}));
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
      final boolean matches = tableName.equals(params.get(3)) && columnName.equals(params.get(4));
      final boolean compareAndSet = sql.contains("AND key_id = ?");
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
    throw new SQLException("Unexpected update: " + sql);
  }

  private int insertKey(Map<Integer, Object> params) {
    final int id = nextKeyId++;
    keys.put(id, new KeyRow(id, (String) params.get(1), (String) params.get(2), (String) params.get(3),
        ((byte[]) params.get(4)).clone()));
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
