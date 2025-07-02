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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.ConnectionPluginManager;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.util.WrapperUtils;

public class ResultSetMetaDataWrapper implements ResultSetMetaData {

  protected ResultSetMetaData resultSetMetaData;
  protected ConnectionPluginManager pluginManager;

  public ResultSetMetaDataWrapper(
      @NonNull ResultSetMetaData resultSetMetaData,
      @NonNull ConnectionPluginManager pluginManager) {
    this.resultSetMetaData = resultSetMetaData;
    this.pluginManager = pluginManager;
  }

  @Override
  public int getColumnCount() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSETMETADATA_GETCOLUMNCOUNT)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.resultSetMetaData,
          JdbcMethod.RESULTSETMETADATA_GETCOLUMNCOUNT,
          () -> this.resultSetMetaData.getColumnCount());
    } else {
      return this.resultSetMetaData.getColumnCount();
    }
  }

  @Override
  public boolean isAutoIncrement(int column) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSETMETADATA_ISAUTOINCREMENT)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.resultSetMetaData,
          JdbcMethod.RESULTSETMETADATA_ISAUTOINCREMENT,
          () -> this.resultSetMetaData.isAutoIncrement(column),
          column);
    } else {
      return this.resultSetMetaData.isAutoIncrement(column);
    }
  }

  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSETMETADATA_ISCASESENSITIVE)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.resultSetMetaData,
          JdbcMethod.RESULTSETMETADATA_ISCASESENSITIVE,
          () -> this.resultSetMetaData.isCaseSensitive(column),
          column);
    } else {
      return this.resultSetMetaData.isCaseSensitive(column);
    }
  }

  @Override
  public boolean isSearchable(int column) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSETMETADATA_ISSEARCHABLE)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.resultSetMetaData,
          JdbcMethod.RESULTSETMETADATA_ISSEARCHABLE,
          () -> this.resultSetMetaData.isSearchable(column),
          column);
    } else {
      return this.resultSetMetaData.isSearchable(column);
    }
  }

  @Override
  public boolean isCurrency(int column) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSETMETADATA_ISCURRENCY)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.resultSetMetaData,
          JdbcMethod.RESULTSETMETADATA_ISCURRENCY,
          () -> this.resultSetMetaData.isCurrency(column),
          column);
    } else {
      return this.resultSetMetaData.isCurrency(column);
    }
  }

  @Override
  public int isNullable(int column) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSETMETADATA_ISNULLABLE)) {
      //noinspection MagicConstant
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.resultSetMetaData,
          JdbcMethod.RESULTSETMETADATA_ISNULLABLE,
          () -> this.resultSetMetaData.isNullable(column),
          column);
    } else {
      return this.resultSetMetaData.isNullable(column);
    }
  }

  @Override
  public boolean isSigned(int column) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSETMETADATA_ISSIGNED)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.resultSetMetaData,
          JdbcMethod.RESULTSETMETADATA_ISSIGNED,
          () -> this.resultSetMetaData.isSigned(column),
          column);
    } else {
      return this.resultSetMetaData.isSigned(column);
    }
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSETMETADATA_GETCOLUMNDISPLAYSIZE)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.resultSetMetaData,
          JdbcMethod.RESULTSETMETADATA_GETCOLUMNDISPLAYSIZE,
          () -> this.resultSetMetaData.getColumnDisplaySize(column),
          column);
    } else {
      return this.resultSetMetaData.getColumnDisplaySize(column);
    }
  }

  @Override
  public String getColumnLabel(int column) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSETMETADATA_GETCOLUMNLABEL)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.pluginManager,
          this.resultSetMetaData,
          JdbcMethod.RESULTSETMETADATA_GETCOLUMNLABEL,
          () -> this.resultSetMetaData.getColumnLabel(column),
          column);
    } else {
      return this.resultSetMetaData.getColumnLabel(column);
    }
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSETMETADATA_GETCOLUMNNAME)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.pluginManager,
          this.resultSetMetaData,
          JdbcMethod.RESULTSETMETADATA_GETCOLUMNNAME,
          () -> this.resultSetMetaData.getColumnName(column),
          column);
    } else {
      return this.resultSetMetaData.getColumnName(column);
    }
  }

  @Override
  public String getSchemaName(int column) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSETMETADATA_GETSCHEMANAME)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.pluginManager,
          this.resultSetMetaData,
          JdbcMethod.RESULTSETMETADATA_GETSCHEMANAME,
          () -> this.resultSetMetaData.getSchemaName(column),
          column);
    } else {
      return this.resultSetMetaData.getSchemaName(column);
    }
  }

  @Override
  public int getPrecision(int column) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSETMETADATA_GETPRECISION)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.resultSetMetaData,
          JdbcMethod.RESULTSETMETADATA_GETPRECISION,
          () -> this.resultSetMetaData.getPrecision(column),
          column);
    } else {
      return this.resultSetMetaData.getPrecision(column);
    }
  }

  @Override
  public int getScale(int column) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSETMETADATA_GETSCALE)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.resultSetMetaData,
          JdbcMethod.RESULTSETMETADATA_GETSCALE,
          () -> this.resultSetMetaData.getScale(column),
          column);
    } else {
      return this.resultSetMetaData.getScale(column);
    }
  }

  @Override
  public String getTableName(int column) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSETMETADATA_GETTABLENAME)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.pluginManager,
          this.resultSetMetaData,
          JdbcMethod.RESULTSETMETADATA_GETTABLENAME,
          () -> this.resultSetMetaData.getTableName(column),
          column);
    } else {
      return this.resultSetMetaData.getTableName(column);
    }
  }

  @Override
  public String getCatalogName(int column) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSETMETADATA_GETCATALOGNAME)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.pluginManager,
          this.resultSetMetaData,
          JdbcMethod.RESULTSETMETADATA_GETCATALOGNAME,
          () -> this.resultSetMetaData.getCatalogName(column),
          column);
    } else {
      return this.resultSetMetaData.getCatalogName(column);
    }
  }

  @Override
  public int getColumnType(int column) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSETMETADATA_GETCOLUMNTYPE)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.resultSetMetaData,
          JdbcMethod.RESULTSETMETADATA_GETCOLUMNTYPE,
          () -> this.resultSetMetaData.getColumnType(column),
          column);
    } else {
      return this.resultSetMetaData.getColumnType(column);
    }
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSETMETADATA_GETCOLUMNTYPENAME)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.pluginManager,
          this.resultSetMetaData,
          JdbcMethod.RESULTSETMETADATA_GETCOLUMNTYPENAME,
          () -> this.resultSetMetaData.getColumnTypeName(column),
          column);
    } else {
      return this.resultSetMetaData.getColumnTypeName(column);
    }
  }

  @Override
  public boolean isReadOnly(int column) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSETMETADATA_ISREADONLY)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.resultSetMetaData,
          JdbcMethod.RESULTSETMETADATA_ISREADONLY,
          () -> this.resultSetMetaData.isReadOnly(column),
          column);
    } else {
      return this.resultSetMetaData.isReadOnly(column);
    }
  }

  @Override
  public boolean isWritable(int column) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSETMETADATA_ISWRITABLE)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.resultSetMetaData,
          JdbcMethod.RESULTSETMETADATA_ISWRITABLE,
          () -> this.resultSetMetaData.isWritable(column),
          column);
    } else {
      return this.resultSetMetaData.isWritable(column);
    }
  }

  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSETMETADATA_ISDEFINITELYWRITABLE)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.pluginManager,
          this.resultSetMetaData,
          JdbcMethod.RESULTSETMETADATA_ISDEFINITELYWRITABLE,
          () -> this.resultSetMetaData.isDefinitelyWritable(column),
          column);
    } else {
      return this.resultSetMetaData.isDefinitelyWritable(column);
    }
  }

  @Override
  public String getColumnClassName(int column) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.RESULTSETMETADATA_GETCOLUMNCLASSNAME)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.pluginManager,
          this.resultSetMetaData,
          JdbcMethod.RESULTSETMETADATA_GETCOLUMNCLASSNAME,
          () -> this.resultSetMetaData.getColumnClassName(column),
          column);
    } else {
      return this.resultSetMetaData.getColumnClassName(column);
    }
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return this.resultSetMetaData.unwrap(iface);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return this.resultSetMetaData.isWrapperFor(iface);
  }

  @Override
  public String toString() {
    return super.toString() + " - " + this.resultSetMetaData;
  }
}
