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

package software.amazon.jdbc.wrapper;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.ConnectionPluginManager;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.util.WrapperUtils;

public class ArrayWrapper implements Array {

  protected final Array array;
  protected final ConnectionWrapper connectionWrapper;
  protected final ConnectionPluginManager pluginManager;

  public ArrayWrapper(
      @NonNull Array array,
      @NonNull ConnectionWrapper connectionWrapper,
      @NonNull ConnectionPluginManager pluginManager) {
    this.array = array;
    this.connectionWrapper = connectionWrapper;
    this.pluginManager = pluginManager;
  }

  @Override
  public String getBaseTypeName() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.ARRAY_GETBASETYPENAME)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.array,
          JdbcMethod.ARRAY_GETBASETYPENAME,
          this.array::getBaseTypeName);
    } else {
      return this.array.getBaseTypeName();
    }
  }

  @Override
  public int getBaseType() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.ARRAY_GETBASETYPE)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.array,
          JdbcMethod.ARRAY_GETBASETYPE,
          this.array::getBaseType);
    } else {
      return this.array.getBaseType();
    }
  }

  @Override
  public Object getArray() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.ARRAY_GETARRAY)) {
      return WrapperUtils.executeWithPlugins(
          Object.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.array,
          JdbcMethod.ARRAY_GETARRAY,
          this.array::getArray);
    } else {
      return this.array.getArray();
    }
  }

  @Override
  public Object getArray(Map<String, Class<?>> map) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.ARRAY_GETARRAY)) {
      return WrapperUtils.executeWithPlugins(
          Object.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.array,
          JdbcMethod.ARRAY_GETARRAY,
          () -> this.array.getArray(map),
          map);
    } else {
      return this.array.getArray(map);
    }
  }

  @Override
  public Object getArray(long index, int count) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.ARRAY_GETARRAY)) {
      return WrapperUtils.executeWithPlugins(
          Object.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.array,
          JdbcMethod.ARRAY_GETARRAY,
          () -> this.array.getArray(index, count),
          index,
          count);
    } else {
      return this.array.getArray(index, count);
    }
  }

  @Override
  public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.ARRAY_GETARRAY)) {
      return WrapperUtils.executeWithPlugins(
          Object.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.array,
          JdbcMethod.ARRAY_GETARRAY,
          () -> this.array.getArray(index, count, map),
          index,
          count,
          map);
    } else {
      return this.array.getArray(index, count, map);
    }
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
         this.connectionWrapper,
          this.pluginManager,
        this.array,
        JdbcMethod.ARRAY_GETRESULTSET,
        this.array::getResultSet);
  }

  @Override
  public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
         this.connectionWrapper,
          this.pluginManager,
        this.array,
        JdbcMethod.ARRAY_GETRESULTSET,
        () -> this.array.getResultSet(map),
        map);
  }

  @Override
  public ResultSet getResultSet(long index, int count) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
         this.connectionWrapper,
          this.pluginManager,
        this.array,
        JdbcMethod.ARRAY_GETRESULTSET,
        () -> this.array.getResultSet(index, count),
        index,
        count);
  }

  @Override
  public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map)
      throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
         this.connectionWrapper,
          this.pluginManager,
        this.array,
        JdbcMethod.ARRAY_GETRESULTSET,
        () -> this.array.getResultSet(index, count, map),
        index,
        count,
        map);
  }

  @Override
  public void free() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.ARRAY_FREE)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.array,
          JdbcMethod.ARRAY_FREE,
          this.array::free);
    } else {
      this.array.free();
    }
  }

  @Override
  public String toString() {
    // This implementation is different from other wrapper classes toString(). The reason is
    // that PG JDBC driver has a strong functional dependency on this method and uses it to
    // serialize array content. So wrapper class should preserve this logic.
    // More details can be found at
    // https://github.com/pgjdbc/pgjdbc/blob/f61fbfe7b72ccf2ca0ac2e2c366230fdb93260e5/pgjdbc/src/main/java/org/postgresql/jdbc/PgArray.java
    return this.array.toString();
  }
}
