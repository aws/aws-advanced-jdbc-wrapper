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
import software.amazon.jdbc.ConnectionPluginManager;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.util.WrapperUtils;

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
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLOUTPUT_WRITESTRING)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.sqlOutput,
          JdbcMethod.SQLOUTPUT_WRITESTRING,
          () -> this.sqlOutput.writeString(x),
          x);
    } else {
      this.sqlOutput.writeString(x);
    }
  }

  @Override
  public void writeBoolean(boolean x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLOUTPUT_WRITEBOOLEAN)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.sqlOutput,
          JdbcMethod.SQLOUTPUT_WRITEBOOLEAN,
          () -> this.sqlOutput.writeBoolean(x),
          x);
    } else {
      this.sqlOutput.writeBoolean(x);
    }
  }

  @Override
  public void writeByte(byte x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLOUTPUT_WRITEBYTE)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.sqlOutput,
          JdbcMethod.SQLOUTPUT_WRITEBYTE,
          () -> this.sqlOutput.writeByte(x),
          x);
    } else {
      this.sqlOutput.writeByte(x);
    }
  }

  @Override
  public void writeShort(short x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLOUTPUT_WRITESHORT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.sqlOutput,
          JdbcMethod.SQLOUTPUT_WRITESHORT,
          () -> this.sqlOutput.writeShort(x),
          x);
    } else {
      this.sqlOutput.writeShort(x);
    }
  }

  @Override
  public void writeInt(int x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLOUTPUT_WRITEINT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.sqlOutput,
          JdbcMethod.SQLOUTPUT_WRITEINT,
          () -> this.sqlOutput.writeInt(x),
          x);
    } else {
      this.sqlOutput.writeInt(x);
    }
  }

  @Override
  public void writeLong(long x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLOUTPUT_WRITELONG)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.sqlOutput,
          JdbcMethod.SQLOUTPUT_WRITELONG,
          () -> this.sqlOutput.writeLong(x),
          x);
    } else {
      this.sqlOutput.writeLong(x);
    }
  }

  @Override
  public void writeFloat(float x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLOUTPUT_WRITEFLOAT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.sqlOutput,
          JdbcMethod.SQLOUTPUT_WRITEFLOAT,
          () -> this.sqlOutput.writeFloat(x),
          x);
    } else {
      this.sqlOutput.writeFloat(x);
    }
  }

  @Override
  public void writeDouble(double x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLOUTPUT_WRITEDOUBLE)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.sqlOutput,
          JdbcMethod.SQLOUTPUT_WRITEDOUBLE,
          () -> this.sqlOutput.writeDouble(x),
          x);
    } else {
      this.sqlOutput.writeDouble(x);
    }
  }

  @Override
  public void writeBigDecimal(BigDecimal x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLOUTPUT_WRITEBIGDECIMAL)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.sqlOutput,
          JdbcMethod.SQLOUTPUT_WRITEBIGDECIMAL,
          () -> this.sqlOutput.writeBigDecimal(x),
          x);
    } else {
      this.sqlOutput.writeBigDecimal(x);
    }
  }

  @Override
  public void writeBytes(byte[] x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLOUTPUT_WRITEBYTES)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.sqlOutput,
          JdbcMethod.SQLOUTPUT_WRITEBYTES,
          () -> this.sqlOutput.writeBytes(x),
          x);
    } else {
      this.sqlOutput.writeBytes(x);
    }
  }

  @Override
  public void writeDate(Date x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLOUTPUT_WRITEDATE)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.sqlOutput,
          JdbcMethod.SQLOUTPUT_WRITEDATE,
          () -> this.sqlOutput.writeDate(x),
          x);
    } else {
      this.sqlOutput.writeDate(x);
    }
  }

  @Override
  public void writeTime(Time x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLOUTPUT_WRITETIME)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.sqlOutput,
          JdbcMethod.SQLOUTPUT_WRITETIME,
          () -> this.sqlOutput.writeTime(x),
          x);
    } else {
      this.sqlOutput.writeTime(x);
    }
  }

  @Override
  public void writeTimestamp(Timestamp x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLOUTPUT_WRITETIMESTAMP)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.sqlOutput,
          JdbcMethod.SQLOUTPUT_WRITETIMESTAMP,
          () -> this.sqlOutput.writeTimestamp(x),
          x);
    } else {
      this.sqlOutput.writeTimestamp(x);
    }
  }

  @Override
  public void writeCharacterStream(Reader x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLOUTPUT_WRITECHARACTERSTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.sqlOutput,
          JdbcMethod.SQLOUTPUT_WRITECHARACTERSTREAM,
          () -> this.sqlOutput.writeCharacterStream(x),
          x);
    } else {
      this.sqlOutput.writeCharacterStream(x);
    }
  }

  @Override
  public void writeAsciiStream(InputStream x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLOUTPUT_WRITEASCIISTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.sqlOutput,
          JdbcMethod.SQLOUTPUT_WRITEASCIISTREAM,
          () -> this.sqlOutput.writeAsciiStream(x),
          x);
    } else {
      this.sqlOutput.writeAsciiStream(x);
    }
  }

  @Override
  public void writeBinaryStream(InputStream x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLOUTPUT_WRITEBINARYSTREAM)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.sqlOutput,
          JdbcMethod.SQLOUTPUT_WRITEBINARYSTREAM,
          () -> this.sqlOutput.writeBinaryStream(x),
          x);
    } else {
      this.sqlOutput.writeBinaryStream(x);
    }
  }

  @Override
  public void writeObject(SQLData x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLOUTPUT_WRITEOBJECT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.sqlOutput,
          JdbcMethod.SQLOUTPUT_WRITEOBJECT,
          () -> this.sqlOutput.writeObject(x),
          x);
    } else {
      this.sqlOutput.writeObject(x);
    }
  }

  @Override
  public void writeObject(Object x, SQLType targetSqlType) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLOUTPUT_WRITEOBJECT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.sqlOutput,
          JdbcMethod.SQLOUTPUT_WRITEOBJECT,
          () -> this.sqlOutput.writeObject(x, targetSqlType),
          x,
          targetSqlType);
    } else {
      this.sqlOutput.writeObject(x, targetSqlType);
    }
  }

  @Override
  public void writeRef(Ref x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLOUTPUT_WRITEREF)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.sqlOutput,
          JdbcMethod.SQLOUTPUT_WRITEREF,
          () -> this.sqlOutput.writeRef(x),
          x);
    } else {
      this.sqlOutput.writeRef(x);
    }
  }

  @Override
  public void writeBlob(Blob x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLOUTPUT_WRITEBLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.sqlOutput,
          JdbcMethod.SQLOUTPUT_WRITEBLOB,
          () -> this.sqlOutput.writeBlob(x),
          x);
    } else {
      this.sqlOutput.writeBlob(x);
    }
  }

  @Override
  public void writeClob(Clob x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLOUTPUT_WRITECLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.sqlOutput,
          JdbcMethod.SQLOUTPUT_WRITECLOB,
          () -> this.sqlOutput.writeClob(x),
          x);
    } else {
      this.sqlOutput.writeClob(x);
    }
  }

  @Override
  public void writeStruct(Struct x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLOUTPUT_WRITESTRUCT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.sqlOutput,
          JdbcMethod.SQLOUTPUT_WRITESTRUCT,
          () -> this.sqlOutput.writeStruct(x),
          x);
    } else {
      this.sqlOutput.writeStruct(x);
    }
  }

  @Override
  public void writeArray(Array x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLOUTPUT_WRITEARRAY)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.sqlOutput,
          JdbcMethod.SQLOUTPUT_WRITEARRAY,
          () -> this.sqlOutput.writeArray(x),
          x);
    } else {
      this.sqlOutput.writeArray(x);
    }
  }

  @Override
  public void writeURL(URL x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLOUTPUT_WRITEURL)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.sqlOutput,
          JdbcMethod.SQLOUTPUT_WRITEURL,
          () -> this.sqlOutput.writeURL(x),
          x);
    } else {
      this.sqlOutput.writeURL(x);
    }
  }

  @Override
  public void writeNString(String x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLOUTPUT_WRITENSTRING)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.sqlOutput,
          JdbcMethod.SQLOUTPUT_WRITENSTRING,
          () -> this.sqlOutput.writeNString(x),
          x);
    } else {
      this.sqlOutput.writeNString(x);
    }
  }

  @Override
  public void writeNClob(NClob x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLOUTPUT_WRITENCLOB)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.sqlOutput,
          JdbcMethod.SQLOUTPUT_WRITENCLOB,
          () -> this.sqlOutput.writeNClob(x),
          x);
    } else {
      this.sqlOutput.writeNClob(x);
    }
  }

  @Override
  public void writeRowId(RowId x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLOUTPUT_WRITEROWID)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.sqlOutput,
          JdbcMethod.SQLOUTPUT_WRITEROWID,
          () -> this.sqlOutput.writeRowId(x),
          x);
    } else {
      this.sqlOutput.writeRowId(x);
    }
  }

  @Override
  public void writeSQLXML(SQLXML x) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLOUTPUT_WRITESQLXML)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.sqlOutput,
          JdbcMethod.SQLOUTPUT_WRITESQLXML,
          () -> this.sqlOutput.writeSQLXML(x),
          x);
    } else {
      this.sqlOutput.writeSQLXML(x);
    }
  }

  @Override
  public String toString() {
    return super.toString() + " - " + this.sqlOutput;
  }
}
