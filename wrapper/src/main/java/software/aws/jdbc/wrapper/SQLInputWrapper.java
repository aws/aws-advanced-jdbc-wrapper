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

package software.aws.jdbc.wrapper;

import software.aws.jdbc.ConnectionPluginManager;
import software.aws.jdbc.util.WrapperUtils;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import org.checkerframework.checker.nullness.qual.NonNull;

public class SQLInputWrapper implements SQLInput {

  protected SQLInput sqlInput;
  protected ConnectionPluginManager pluginManager;

  public SQLInputWrapper(
      @NonNull SQLInput sqlInput, @NonNull ConnectionPluginManager pluginManager) {
    this.sqlInput = sqlInput;
    this.pluginManager = pluginManager;
  }

  @Override
  public String readString() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        String.class,
        SQLException.class,
        this.pluginManager,
        this.sqlInput,
        "SQLInput.readString",
        () -> this.sqlInput.readString());
  }

  @Override
  public boolean readBoolean() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.sqlInput,
        "SQLInput.readBoolean",
        () -> this.sqlInput.readBoolean());
  }

  @Override
  public byte readByte() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        byte.class,
        SQLException.class,
        this.pluginManager,
        this.sqlInput,
        "SQLInput.readByte",
        () -> this.sqlInput.readByte());
  }

  @Override
  public short readShort() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        short.class,
        SQLException.class,
        this.pluginManager,
        this.sqlInput,
        "SQLInput.readShort",
        () -> this.sqlInput.readShort());
  }

  @Override
  public int readInt() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.sqlInput,
        "SQLInput.readInt",
        () -> this.sqlInput.readInt());
  }

  @Override
  public long readLong() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        long.class,
        SQLException.class,
        this.pluginManager,
        this.sqlInput,
        "SQLInput.readLong",
        () -> this.sqlInput.readLong());
  }

  @Override
  public float readFloat() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        float.class,
        SQLException.class,
        this.pluginManager,
        this.sqlInput,
        "SQLInput.readFloat",
        () -> this.sqlInput.readFloat());
  }

  @Override
  public double readDouble() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        double.class,
        SQLException.class,
        this.pluginManager,
        this.sqlInput,
        "SQLInput.readDouble",
        () -> this.sqlInput.readDouble());
  }

  @Override
  public BigDecimal readBigDecimal() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        BigDecimal.class,
        SQLException.class,
        this.pluginManager,
        this.sqlInput,
        "SQLInput.readBigDecimal",
        () -> this.sqlInput.readBigDecimal());
  }

  @Override
  public byte[] readBytes() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        byte[].class,
        SQLException.class,
        this.pluginManager,
        this.sqlInput,
        "SQLInput.readBytes",
        () -> this.sqlInput.readBytes());
  }

  @Override
  public Date readDate() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Date.class,
        SQLException.class,
        this.pluginManager,
        this.sqlInput,
        "SQLInput.readDate",
        () -> this.sqlInput.readDate());
  }

  @Override
  public Time readTime() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Time.class,
        SQLException.class,
        this.pluginManager,
        this.sqlInput,
        "SQLInput.readTime",
        () -> this.sqlInput.readTime());
  }

  @Override
  public Timestamp readTimestamp() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Timestamp.class,
        SQLException.class,
        this.pluginManager,
        this.sqlInput,
        "SQLInput.readTimestamp",
        () -> this.sqlInput.readTimestamp());
  }

  @Override
  public Reader readCharacterStream() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Reader.class,
        SQLException.class,
        this.pluginManager,
        this.sqlInput,
        "SQLInput.readCharacterStream",
        () -> this.sqlInput.readCharacterStream());
  }

  @Override
  public InputStream readAsciiStream() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        InputStream.class,
        SQLException.class,
        this.pluginManager,
        this.sqlInput,
        "SQLInput.readAsciiStream",
        () -> this.sqlInput.readAsciiStream());
  }

  @Override
  public InputStream readBinaryStream() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        InputStream.class,
        SQLException.class,
        this.pluginManager,
        this.sqlInput,
        "SQLInput.readBinaryStream",
        () -> this.sqlInput.readBinaryStream());
  }

  @Override
  public Object readObject() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Object.class,
        SQLException.class,
        this.pluginManager,
        this.sqlInput,
        "SQLInput.readObject",
        () -> this.sqlInput.readObject());
  }

  @Override
  public <T> T readObject(Class<T> type) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        type,
        SQLException.class,
        this.pluginManager,
        this.sqlInput,
        "SQLInput.readString",
        () -> this.sqlInput.readObject(type),
        type);
  }

  @Override
  public Ref readRef() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Ref.class,
        SQLException.class,
        this.pluginManager,
        this.sqlInput,
        "SQLInput.readRef",
        () -> this.sqlInput.readRef());
  }

  @Override
  public Blob readBlob() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Blob.class,
        SQLException.class,
        this.pluginManager,
        this.sqlInput,
        "SQLInput.readBlob",
        () -> this.sqlInput.readBlob());
  }

  @Override
  public Clob readClob() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Clob.class,
        SQLException.class,
        this.pluginManager,
        this.sqlInput,
        "SQLInput.readClob",
        () -> this.sqlInput.readClob());
  }

  @Override
  public Array readArray() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Array.class,
        SQLException.class,
        this.pluginManager,
        this.sqlInput,
        "SQLInput.readArray",
        () -> this.sqlInput.readArray());
  }

  @Override
  public boolean wasNull() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        boolean.class,
        SQLException.class,
        this.pluginManager,
        this.sqlInput,
        "SQLInput.wasNull",
        () -> this.sqlInput.wasNull());
  }

  @Override
  public URL readURL() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        URL.class,
        SQLException.class,
        this.pluginManager,
        this.sqlInput,
        "SQLInput.readURL",
        () -> this.sqlInput.readURL());
  }

  @Override
  public NClob readNClob() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        NClob.class,
        SQLException.class,
        this.pluginManager,
        this.sqlInput,
        "SQLInput.readNClob",
        () -> this.sqlInput.readNClob());
  }

  @Override
  public String readNString() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        String.class,
        SQLException.class,
        this.pluginManager,
        this.sqlInput,
        "SQLInput.readNString",
        () -> this.sqlInput.readNString());
  }

  @Override
  public SQLXML readSQLXML() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        SQLXML.class,
        SQLException.class,
        this.pluginManager,
        this.sqlInput,
        "SQLInput.readSQLXML",
        () -> this.sqlInput.readSQLXML());
  }

  @Override
  public RowId readRowId() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        RowId.class,
        SQLException.class,
        this.pluginManager,
        this.sqlInput,
        "SQLInput.readRowId",
        () -> this.sqlInput.readRowId());
  }
}
