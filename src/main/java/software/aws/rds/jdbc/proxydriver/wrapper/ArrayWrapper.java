/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.wrapper;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.aws.rds.jdbc.proxydriver.ConnectionPluginManager;
import software.aws.rds.jdbc.proxydriver.util.WrapperUtils;

public class ArrayWrapper implements Array {

  protected Array array;
  protected ConnectionPluginManager pluginManager;

  public ArrayWrapper(@NonNull Array array, @NonNull ConnectionPluginManager pluginManager) {
    this.array = array;
    this.pluginManager = pluginManager;
  }

  @Override
  public String getBaseTypeName() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        String.class,
        SQLException.class,
        this.pluginManager,
        this.array,
        "Array.getBaseTypeName",
        () -> this.array.getBaseTypeName());
  }

  @Override
  public int getBaseType() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.array,
        "Array.getBaseType",
        () -> this.array.getBaseType());
  }

  @Override
  public Object getArray() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Object.class,
        SQLException.class,
        this.pluginManager,
        this.array,
        "Array.getArray",
        () -> this.array.getArray());
  }

  @Override
  public Object getArray(Map<String, Class<?>> map) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Object.class,
        SQLException.class,
        this.pluginManager,
        this.array,
        "Array.getArray",
        () -> this.array.getArray());
  }

  @Override
  public Object getArray(long index, int count) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Object.class,
        SQLException.class,
        this.pluginManager,
        this.array,
        "Array.getArray",
        () -> this.array.getArray());
  }

  @Override
  public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Object.class,
        SQLException.class,
        this.pluginManager,
        this.array,
        "Array.getArray",
        () -> this.array.getArray());
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
        this.pluginManager,
        this.array,
        "Array.getResultSet",
        () -> this.array.getResultSet());
  }

  @Override
  public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
        this.pluginManager,
        this.array,
        "Array.getResultSet",
        () -> this.array.getResultSet());
  }

  @Override
  public ResultSet getResultSet(long index, int count) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
        this.pluginManager,
        this.array,
        "Array.getResultSet",
        () -> this.array.getResultSet());
  }

  @Override
  public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map)
      throws SQLException {
    return WrapperUtils.executeWithPlugins(
        ResultSet.class,
        SQLException.class,
        this.pluginManager,
        this.array,
        "Array.getResultSet",
        () -> this.array.getResultSet(index, count, map),
        index,
        count,
        map);
  }

  @Override
  public void free() throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class, this.pluginManager, this.array, "Array.free", () -> this.array.free());
  }
}
