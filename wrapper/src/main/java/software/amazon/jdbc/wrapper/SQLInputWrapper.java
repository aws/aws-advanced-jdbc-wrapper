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
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.ConnectionPluginManager;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.util.WrapperUtils;

public class SQLInputWrapper implements SQLInput {

  protected final SQLInput sqlInput;
  protected final ConnectionWrapper connectionWrapper;
  protected final ConnectionPluginManager pluginManager;

  public SQLInputWrapper(
      @NonNull SQLInput sqlInput,
      @NonNull ConnectionWrapper connectionWrapper,
      @NonNull ConnectionPluginManager pluginManager) {
    this.sqlInput = sqlInput;
    this.connectionWrapper = connectionWrapper;
    this.pluginManager = pluginManager;
  }

  @Override
  public String readString() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLINPUT_READSTRING)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.sqlInput,
          JdbcMethod.SQLINPUT_READSTRING,
          this.sqlInput::readString);
    } else {
      return this.sqlInput.readString();
    }
  }

  @Override
  public boolean readBoolean() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLINPUT_READBOOLEAN)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.sqlInput,
          JdbcMethod.SQLINPUT_READBOOLEAN,
          this.sqlInput::readBoolean);
    } else {
      return this.sqlInput.readBoolean();
    }
  }

  @Override
  public byte readByte() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLINPUT_READBYTE)) {
      return WrapperUtils.executeWithPlugins(
          byte.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.sqlInput,
          JdbcMethod.SQLINPUT_READBYTE,
          this.sqlInput::readByte);
    } else {
      return this.sqlInput.readByte();
    }
  }

  @Override
  public short readShort() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLINPUT_READSHORT)) {
      return WrapperUtils.executeWithPlugins(
          short.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.sqlInput,
          JdbcMethod.SQLINPUT_READSHORT,
          this.sqlInput::readShort);
    } else {
      return this.sqlInput.readShort();
    }
  }

  @Override
  public int readInt() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLINPUT_READINT)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.sqlInput,
          JdbcMethod.SQLINPUT_READINT,
          this.sqlInput::readInt);
    } else {
      return this.sqlInput.readInt();
    }
  }

  @Override
  public long readLong() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLINPUT_READLONG)) {
      return WrapperUtils.executeWithPlugins(
          long.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.sqlInput,
          JdbcMethod.SQLINPUT_READLONG,
          this.sqlInput::readLong);
    } else {
      return this.sqlInput.readLong();
    }
  }

  @Override
  public float readFloat() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLINPUT_READFLOAT)) {
      return WrapperUtils.executeWithPlugins(
          float.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.sqlInput,
          JdbcMethod.SQLINPUT_READFLOAT,
          this.sqlInput::readFloat);
    } else {
      return this.sqlInput.readFloat();
    }
  }

  @Override
  public double readDouble() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLINPUT_READDOUBLE)) {
      return WrapperUtils.executeWithPlugins(
          double.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.sqlInput,
          JdbcMethod.SQLINPUT_READDOUBLE,
          this.sqlInput::readDouble);
    } else {
      return this.sqlInput.readDouble();
    }
  }

  @Override
  public BigDecimal readBigDecimal() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLINPUT_READBIGDECIMAL)) {
      return WrapperUtils.executeWithPlugins(
          BigDecimal.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.sqlInput,
          JdbcMethod.SQLINPUT_READBIGDECIMAL,
          this.sqlInput::readBigDecimal);
    } else {
      return this.sqlInput.readBigDecimal();
    }
  }

  @Override
  public byte[] readBytes() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLINPUT_READBYTES)) {
      return WrapperUtils.executeWithPlugins(
          byte[].class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.sqlInput,
          JdbcMethod.SQLINPUT_READBYTES,
          this.sqlInput::readBytes);
    } else {
      return this.sqlInput.readBytes();
    }
  }

  @Override
  public Date readDate() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLINPUT_READDATE)) {
      return WrapperUtils.executeWithPlugins(
          Date.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.sqlInput,
          JdbcMethod.SQLINPUT_READDATE,
          this.sqlInput::readDate);
    } else {
      return this.sqlInput.readDate();
    }
  }

  @Override
  public Time readTime() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLINPUT_READTIME)) {
      return WrapperUtils.executeWithPlugins(
          Time.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.sqlInput,
          JdbcMethod.SQLINPUT_READTIME,
          this.sqlInput::readTime);
    } else {
      return this.sqlInput.readTime();
    }
  }

  @Override
  public Timestamp readTimestamp() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLINPUT_READTIMESTAMP)) {
      return WrapperUtils.executeWithPlugins(
          Timestamp.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.sqlInput,
          JdbcMethod.SQLINPUT_READTIMESTAMP,
          this.sqlInput::readTimestamp);
    } else {
      return this.sqlInput.readTimestamp();
    }
  }

  @Override
  public Reader readCharacterStream() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLINPUT_READCHARACTERSTREAM)) {
      return WrapperUtils.executeWithPlugins(
          Reader.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.sqlInput,
          JdbcMethod.SQLINPUT_READCHARACTERSTREAM,
          this.sqlInput::readCharacterStream);
    } else {
      return this.sqlInput.readCharacterStream();
    }
  }

  @Override
  public InputStream readAsciiStream() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLINPUT_READASCIISTREAM)) {
      return WrapperUtils.executeWithPlugins(
          InputStream.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.sqlInput,
          JdbcMethod.SQLINPUT_READASCIISTREAM,
          this.sqlInput::readAsciiStream);
    } else {
      return this.sqlInput.readAsciiStream();
    }
  }

  @Override
  public InputStream readBinaryStream() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLINPUT_READBINARYSTREAM)) {
      return WrapperUtils.executeWithPlugins(
          InputStream.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.sqlInput,
          JdbcMethod.SQLINPUT_READBINARYSTREAM,
          this.sqlInput::readBinaryStream);
    } else {
      return this.sqlInput.readBinaryStream();
    }
  }

  @Override
  public Object readObject() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLINPUT_READOBJECT)) {
      return WrapperUtils.executeWithPlugins(
          Object.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.sqlInput,
          JdbcMethod.SQLINPUT_READOBJECT,
          this.sqlInput::readObject);
    } else {
      return this.sqlInput.readObject();
    }
  }

  @Override
  public <T> T readObject(Class<T> type) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLINPUT_READOBJECT)) {
      return WrapperUtils.executeWithPlugins(
          type,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.sqlInput,
          JdbcMethod.SQLINPUT_READOBJECT,
          () -> this.sqlInput.readObject(type),
          type);
    } else {
      return this.sqlInput.readObject(type);
    }
  }

  @Override
  public Ref readRef() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Ref.class,
        SQLException.class,
        this.connectionWrapper,
        this.pluginManager,
        this.sqlInput,
        JdbcMethod.SQLINPUT_READREF,
        this.sqlInput::readRef);
  }

  @Override
  public Blob readBlob() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Blob.class,
        SQLException.class,
        this.connectionWrapper,
        this.pluginManager,
        this.sqlInput,
        JdbcMethod.SQLINPUT_READBLOB,
        this.sqlInput::readBlob);
  }

  @Override
  public Clob readClob() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Clob.class,
        SQLException.class,
        this.connectionWrapper,
        this.pluginManager,
        this.sqlInput,
        JdbcMethod.SQLINPUT_READCLOB,
        this.sqlInput::readClob);
  }

  @Override
  public Array readArray() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Array.class,
        SQLException.class,
        this.connectionWrapper,
        this.pluginManager,
        this.sqlInput,
        JdbcMethod.SQLINPUT_READARRAY,
        this.sqlInput::readArray);
  }

  @Override
  public boolean wasNull() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLINPUT_WASNULL)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.sqlInput,
          JdbcMethod.SQLINPUT_WASNULL,
          this.sqlInput::wasNull);
    } else {
      return this.sqlInput.wasNull();
    }
  }

  @Override
  public URL readURL() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLINPUT_READURL)) {
      return WrapperUtils.executeWithPlugins(
          URL.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.sqlInput,
          JdbcMethod.SQLINPUT_READURL,
          this.sqlInput::readURL);
    } else {
      return this.sqlInput.readURL();
    }
  }

  @Override
  public NClob readNClob() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        NClob.class,
        SQLException.class,
        this.connectionWrapper,
        this.pluginManager,
        this.sqlInput,
        JdbcMethod.SQLINPUT_READNCLOB,
        this.sqlInput::readNClob);
  }

  @Override
  public String readNString() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLINPUT_READNSTRING)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.sqlInput,
          JdbcMethod.SQLINPUT_READNSTRING,
          this.sqlInput::readNString);
    } else {
      return this.sqlInput.readNString();
    }
  }

  @Override
  public SQLXML readSQLXML() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLINPUT_READSQLXML)) {
      return WrapperUtils.executeWithPlugins(
          SQLXML.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.sqlInput,
          JdbcMethod.SQLINPUT_READSQLXML,
          this.sqlInput::readSQLXML);
    } else {
      return this.sqlInput.readSQLXML();
    }
  }

  @Override
  public RowId readRowId() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLINPUT_READROWID)) {
      return WrapperUtils.executeWithPlugins(
          RowId.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.sqlInput,
          JdbcMethod.SQLINPUT_READROWID,
          this.sqlInput::readRowId);
    } else {
      return this.sqlInput.readRowId();
    }
  }

  @Override
  public String toString() {
    return super.toString() + " - " + this.sqlInput;
  }
}
