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
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLOutput;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import org.checkerframework.checker.nullness.qual.NonNull;

public class SQLOutputWrapper implements SQLOutput {

  protected SQLOutput sqlOutput;
  protected ConnectionPluginManager pluginManager;

  public SQLOutputWrapper(
      @NonNull SQLOutput sqlOutput, @NonNull ConnectionPluginManager pluginManager) {
    this.sqlOutput = sqlOutput;
    this.pluginManager = pluginManager;
  }

  @Override
  public void writeString(String x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.sqlOutput,
        "SQLOutput.writeString",
        () -> this.sqlOutput.writeString(x),
        x);
  }

  @Override
  public void writeBoolean(boolean x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.sqlOutput,
        "SQLOutput.writeBoolean",
        () -> this.sqlOutput.writeBoolean(x),
        x);
  }

  @Override
  public void writeByte(byte x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.sqlOutput,
        "SQLOutput.writeByte",
        () -> this.sqlOutput.writeByte(x),
        x);
  }

  @Override
  public void writeShort(short x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.sqlOutput,
        "SQLOutput.writeShort",
        () -> this.sqlOutput.writeShort(x),
        x);
  }

  @Override
  public void writeInt(int x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.sqlOutput,
        "SQLOutput.writeInt",
        () -> this.sqlOutput.writeInt(x),
        x);
  }

  @Override
  public void writeLong(long x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.sqlOutput,
        "SQLOutput.writeLong",
        () -> this.sqlOutput.writeLong(x),
        x);
  }

  @Override
  public void writeFloat(float x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.sqlOutput,
        "SQLOutput.writeFloat",
        () -> this.sqlOutput.writeFloat(x),
        x);
  }

  @Override
  public void writeDouble(double x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.sqlOutput,
        "SQLOutput.writeDouble",
        () -> this.sqlOutput.writeDouble(x),
        x);
  }

  @Override
  public void writeBigDecimal(BigDecimal x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.sqlOutput,
        "SQLOutput.writeBigDecimal",
        () -> this.sqlOutput.writeBigDecimal(x),
        x);
  }

  @Override
  public void writeBytes(byte[] x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.sqlOutput,
        "SQLOutput.writeBytes",
        () -> this.sqlOutput.writeBytes(x),
        x);
  }

  @Override
  public void writeDate(Date x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.sqlOutput,
        "SQLOutput.writeDate",
        () -> this.sqlOutput.writeDate(x),
        x);
  }

  @Override
  public void writeTime(Time x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.sqlOutput,
        "SQLOutput.writeTime",
        () -> this.sqlOutput.writeTime(x),
        x);
  }

  @Override
  public void writeTimestamp(Timestamp x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.sqlOutput,
        "SQLOutput.writeTimestamp",
        () -> this.sqlOutput.writeTimestamp(x),
        x);
  }

  @Override
  public void writeCharacterStream(Reader x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.sqlOutput,
        "SQLOutput.writeCharacterStream",
        () -> this.sqlOutput.writeCharacterStream(x),
        x);
  }

  @Override
  public void writeAsciiStream(InputStream x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.sqlOutput,
        "SQLOutput.writeAsciiStream",
        () -> this.sqlOutput.writeAsciiStream(x),
        x);
  }

  @Override
  public void writeBinaryStream(InputStream x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.sqlOutput,
        "SQLOutput.writeBinaryStream",
        () -> this.sqlOutput.writeBinaryStream(x),
        x);
  }

  @Override
  public void writeObject(SQLData x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.sqlOutput,
        "SQLOutput.writeObject",
        () -> this.sqlOutput.writeObject(x),
        x);
  }

  @Override
  public void writeObject(Object x, SQLType targetSqlType) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.sqlOutput,
        "SQLOutput.writeObject",
        () -> this.sqlOutput.writeObject(x, targetSqlType),
        x,
        targetSqlType);
  }

  @Override
  public void writeRef(Ref x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.sqlOutput,
        "SQLOutput.writeRef",
        () -> this.sqlOutput.writeRef(x),
        x);
  }

  @Override
  public void writeBlob(Blob x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.sqlOutput,
        "SQLOutput.writeBlob",
        () -> this.sqlOutput.writeBlob(x),
        x);
  }

  @Override
  public void writeClob(Clob x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.sqlOutput,
        "SQLOutput.writeClob",
        () -> this.sqlOutput.writeClob(x),
        x);
  }

  @Override
  public void writeStruct(Struct x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.sqlOutput,
        "SQLOutput.writeStruct",
        () -> this.sqlOutput.writeStruct(x),
        x);
  }

  @Override
  public void writeArray(Array x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.sqlOutput,
        "SQLOutput.writeArray",
        () -> this.sqlOutput.writeArray(x),
        x);
  }

  @Override
  public void writeURL(URL x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.sqlOutput,
        "SQLOutput.writeURL",
        () -> this.sqlOutput.writeURL(x),
        x);
  }

  @Override
  public void writeNString(String x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.sqlOutput,
        "SQLOutput.writeNString",
        () -> this.sqlOutput.writeNString(x),
        x);
  }

  @Override
  public void writeNClob(NClob x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.sqlOutput,
        "SQLOutput.writeNClob",
        () -> this.sqlOutput.writeNClob(x),
        x);
  }

  @Override
  public void writeRowId(RowId x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.sqlOutput,
        "SQLOutput.writeRowId",
        () -> this.sqlOutput.writeRowId(x),
        x);
  }

  @Override
  public void writeSQLXML(SQLXML x) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.sqlOutput,
        "SQLOutput.writeSQLXML",
        () -> this.sqlOutput.writeSQLXML(x),
        x);
  }
}
