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

package software.amazon.jdbc.plugin.cache;

import java.io.Serializable;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

class CachedResultSetMetaData implements ResultSetMetaData, Serializable {
  protected final Field[] columns;

  protected static class Field implements Serializable {
    String catalog;
    String className;
    String label;
    String name;
    String typeName;
    int type;
    int displaySize;
    int precision;
    String tableName;
    int scale;
    String schemaName;
    boolean isAutoIncrement;
    boolean isCaseSensitive;
    boolean isCurrency;
    boolean isDefinitelyWritable;
    int isNullable;
    boolean isReadOnly;
    boolean isSearchable;
    boolean isSigned;
    boolean isWritable;

    protected Field(final ResultSetMetaData srcMetadata, int column) throws SQLException {
      catalog = srcMetadata.getCatalogName(column);
      className = srcMetadata.getColumnClassName(column);
      label = srcMetadata.getColumnLabel(column);
      name = srcMetadata.getColumnName(column);
      typeName = srcMetadata.getColumnTypeName(column);
      type = srcMetadata.getColumnType(column);
      displaySize = srcMetadata.getColumnDisplaySize(column);
      precision = srcMetadata.getPrecision(column);
      tableName = srcMetadata.getTableName(column);
      scale = srcMetadata.getScale(column);
      schemaName = srcMetadata.getSchemaName(column);
      isAutoIncrement = srcMetadata.isAutoIncrement(column);
      isCaseSensitive = srcMetadata.isCaseSensitive(column);
      isCurrency = srcMetadata.isCurrency(column);
      isDefinitelyWritable = srcMetadata.isDefinitelyWritable(column);
      isNullable = srcMetadata.isNullable(column);
      isReadOnly = srcMetadata.isReadOnly(column);
      isSearchable = srcMetadata.isSearchable(column);
      isSigned = srcMetadata.isSigned(column);
      isWritable = srcMetadata.isWritable(column);
    }
  }

  CachedResultSetMetaData(Field[] columns) {
    this.columns = columns;
  }

  @Override
  public int getColumnCount() throws SQLException {
    return columns.length;
  }

  private Field getColumns(final int column) throws SQLException {
    if (column == 0 || column > columns.length)
      throw new SQLException("Wrong column number: " + column);
    return columns[column - 1];
  }

  @Override
  public boolean isAutoIncrement(int column) throws SQLException {
    return getColumns(column).isAutoIncrement;
  }

  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    return getColumns(column).isCaseSensitive;
  }

  @Override
  public boolean isSearchable(int column) throws SQLException {
    return getColumns(column).isSearchable;
  }

  @Override
  public boolean isCurrency(int column) throws SQLException {
    return getColumns(column).isCurrency;
  }

  @Override
  public int isNullable(int column) throws SQLException {
    return getColumns(column).isNullable;
  }

  @Override
  public boolean isSigned(int column) throws SQLException {
    return getColumns(column).isSigned;
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    return getColumns(column).displaySize;
  }

  @Override
  public String getColumnLabel(int column) throws SQLException {
    return getColumns(column).label;
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    return getColumns(column).name;
  }

  @Override
  public String getSchemaName(int column) throws SQLException {
    return getColumns(column).schemaName;
  }

  @Override
  public int getPrecision(int column) throws SQLException {
    return getColumns(column).precision;
  }

  @Override
  public int getScale(int column) throws SQLException {
    return getColumns(column).scale;
  }

  @Override
  public String getTableName(int column) throws SQLException {
    return getColumns(column).tableName;
  }

  @Override
  public String getCatalogName(int column) throws SQLException {
    return getColumns(column).catalog;
  }

  @Override
  public int getColumnType(int column) throws SQLException {
    return getColumns(column).type;
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException {
    return getColumns(column).typeName;
  }

  @Override
  public boolean isReadOnly(int column) throws SQLException {
    return getColumns(column).isReadOnly;
  }

  @Override
  public boolean isWritable(int column) throws SQLException {
    return getColumns(column).isWritable;
  }

  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException {
    return getColumns(column).isDefinitelyWritable;
  }

  @Override
  public String getColumnClassName(int column) throws SQLException {
    return getColumns(column).className;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new UnsupportedOperationException();
  }
}
