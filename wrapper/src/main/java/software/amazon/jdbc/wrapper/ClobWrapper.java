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
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.Clob;
import java.sql.SQLException;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.ConnectionPluginManager;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.util.WrapperUtils;

public class ClobWrapper implements Clob {

  protected Clob clob;
  protected ConnectionPluginManager pluginManager;

  public ClobWrapper(@NonNull Clob clob, @NonNull ConnectionPluginManager pluginManager) {
    this.clob = clob;
    this.pluginManager = pluginManager;
  }

  @Override
  public long length() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CLOB_LENGTH)) {
      return WrapperUtils.executeWithPlugins(
          long.class,
          SQLException.class,
          this.pluginManager,
          this.clob,
          JdbcMethod.CLOB_LENGTH,
          () -> this.clob.length());
    } else {
      return this.clob.length();
    }
  }

  @Override
  public String getSubString(long pos, int length) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CLOB_GETSUBSTRING)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.pluginManager,
          this.clob,
          JdbcMethod.CLOB_GETSUBSTRING,
          () -> this.clob.getSubString(pos, length),
          pos,
          length);
    } else {
      return this.clob.getSubString(pos, length);
    }
  }

  @Override
  public Reader getCharacterStream() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CLOB_GETCHARACTERSTREAM)) {
      return WrapperUtils.executeWithPlugins(
          Reader.class,
          SQLException.class,
          this.pluginManager,
          this.clob,
          JdbcMethod.CLOB_GETCHARACTERSTREAM,
          () -> this.clob.getCharacterStream());
    } else {
      return this.clob.getCharacterStream();
    }
  }

  @Override
  public Reader getCharacterStream(long pos, long length) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CLOB_GETCHARACTERSTREAM)) {
      return WrapperUtils.executeWithPlugins(
          Reader.class,
          SQLException.class,
          this.pluginManager,
          this.clob,
          JdbcMethod.CLOB_GETCHARACTERSTREAM,
          () -> this.clob.getCharacterStream(pos, length),
          pos,
          length);
    } else {
      return this.clob.getCharacterStream(pos, length);
    }
  }

  @Override
  public InputStream getAsciiStream() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CLOB_GETASCIISTREAM)) {
      return WrapperUtils.executeWithPlugins(
          InputStream.class,
          SQLException.class,
          this.pluginManager,
          this.clob,
          JdbcMethod.CLOB_GETASCIISTREAM,
          () -> this.clob.getAsciiStream());
    } else {
      return this.clob.getAsciiStream();
    }
  }

  @Override
  public long position(String searchStr, long start) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CLOB_POSITION)) {
      return WrapperUtils.executeWithPlugins(
          long.class,
          SQLException.class,
          this.pluginManager,
          this.clob,
          JdbcMethod.CLOB_POSITION,
          () -> this.clob.position(searchStr, start),
          searchStr,
          start);
    } else {
      return this.clob.position(searchStr, start);
    }
  }

  @Override
  public long position(Clob searchStr, long start) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CLOB_POSITION)) {
      return WrapperUtils.executeWithPlugins(
          long.class,
          SQLException.class,
          this.pluginManager,
          this.clob,
          JdbcMethod.CLOB_POSITION,
          () -> this.clob.position(searchStr, start),
          searchStr,
          start);
    } else {
      return this.clob.position(searchStr, start);
    }
  }

  @Override
  public int setString(long pos, String str) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CLOB_SETSTRING)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.clob,
          JdbcMethod.CLOB_SETSTRING,
          () -> this.clob.setString(pos, str),
          pos,
          str);
    } else {
      return this.clob.setString(pos, str);
    }
  }

  @Override
  public int setString(long pos, String str, int offset, int len) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CLOB_SETSTRING)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.clob,
          JdbcMethod.CLOB_SETSTRING,
          () -> this.clob.setString(pos, str, offset, len),
          pos,
          str,
          offset,
          len);
    } else {
      return this.clob.setString(pos, str, offset, len);
    }
  }

  @Override
  public OutputStream setAsciiStream(long pos) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CLOB_SETASCIISTREAM)) {
      return WrapperUtils.executeWithPlugins(
          OutputStream.class,
          SQLException.class,
          this.pluginManager,
          this.clob,
          JdbcMethod.CLOB_SETASCIISTREAM,
          () -> this.clob.setAsciiStream(pos),
          pos);
    } else {
      return this.clob.setAsciiStream(pos);
    }
  }

  @Override
  public Writer setCharacterStream(long pos) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CLOB_SETCHARACTERSTREAM)) {
      return WrapperUtils.executeWithPlugins(
          Writer.class,
          SQLException.class,
          this.pluginManager,
          this.clob,
          JdbcMethod.CLOB_SETCHARACTERSTREAM,
          () -> this.clob.setCharacterStream(pos),
          pos);
    } else {
      return this.clob.setCharacterStream(pos);
    }
  }

  @Override
  public void truncate(long len) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CLOB_TRUNCATE)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.clob,
          JdbcMethod.CLOB_TRUNCATE,
          () -> this.clob.truncate(len),
          len);
    } else {
      this.clob.truncate(len);
    }
  }

  @Override
  public void free() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.CLOB_FREE)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.clob,
          JdbcMethod.CLOB_FREE,
          () -> this.clob.free());
    } else {
      this.clob.free();
    }
  }

  @Override
  public String toString() {
    return super.toString() + " - " + this.clob;
  }
}
