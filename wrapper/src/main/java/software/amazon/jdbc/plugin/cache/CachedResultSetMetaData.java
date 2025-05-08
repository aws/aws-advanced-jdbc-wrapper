package software.amazon.jdbc.plugin.cache;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class CachedResultSetMetaData implements ResultSetMetaData {

  public static class Field {
    // TODO: support binary format
    private final String columnLabel;
    private final String columnName;
    public Field(String columnLabel, String columnName) {
      this.columnLabel = columnLabel;
      this.columnName = columnName;
    }

    public String getColumnLabel() {
      return columnLabel;
    }

    public String getColumnName() {
      return columnName;
    }
  }

  protected Field[] fields;

  public CachedResultSetMetaData(Field[] fields) {
    this.fields = fields;
  }

  @Override
  public int getColumnCount() throws SQLException {
    return this.fields.length;
  }

  @Override
  public boolean isAutoIncrement(int column) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isSearchable(int column) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isCurrency(int column) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int isNullable(int column) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isSigned(int column) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getColumnLabel(int column) throws SQLException {
    return fields[column-1].getColumnLabel();
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    return fields[column-1].getColumnName();
  }

  // TODO
  @Override
  public String getSchemaName(int column) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getPrecision(int column) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getScale(int column) throws SQLException {
    throw new UnsupportedOperationException();
  }

  // TODO
  @Override
  public String getTableName(int column) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getCatalogName(int column) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getColumnType(int column) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isReadOnly(int column) throws SQLException {
    return true;
  }

  @Override
  public boolean isWritable(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException {
    return false;
  }

  @Override
  public String getColumnClassName(int column) throws SQLException {
    throw new UnsupportedOperationException();
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
