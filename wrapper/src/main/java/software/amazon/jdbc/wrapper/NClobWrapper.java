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
import java.sql.NClob;
import java.sql.SQLException;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.ConnectionPluginManager;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.util.WrapperUtils;

public class NClobWrapper implements NClob {

  protected NClob nclob;
  protected ConnectionPluginManager pluginManager;

  public NClobWrapper(@NonNull NClob nclob, @NonNull ConnectionPluginManager pluginManager) {
    this.nclob = nclob;
    this.pluginManager = pluginManager;
  }

  @Override
  public long length() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.NCLOB_LENGTH)) {
      return WrapperUtils.executeWithPlugins(
          long.class,
          SQLException.class,
          this.pluginManager,
          this.nclob,
          JdbcMethod.NCLOB_LENGTH,
          () -> this.nclob.length());
    } else {
      return this.nclob.length();
    }
  }

  @Override
  public String getSubString(long pos, int length) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.NCLOB_GETSUBSTRING)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.pluginManager,
          this.nclob,
          JdbcMethod.NCLOB_GETSUBSTRING,
          () -> this.nclob.getSubString(pos, length),
          pos,
          length);
    } else {
      return this.nclob.getSubString(pos, length);
    }
  }

  @Override
  public Reader getCharacterStream() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.NCLOB_GETASCIISTREAM)) {
      return WrapperUtils.executeWithPlugins(
          Reader.class,
          SQLException.class,
          this.pluginManager,
          this.nclob,
          JdbcMethod.NCLOB_GETASCIISTREAM,
          () -> this.nclob.getCharacterStream());
    } else {
      return this.nclob.getCharacterStream();
    }
  }

  @Override
  public Reader getCharacterStream(long pos, long length) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.NCLOB_GETCHARACTERSTREAM)) {
      return WrapperUtils.executeWithPlugins(
          Reader.class,
          SQLException.class,
          this.pluginManager,
          this.nclob,
          JdbcMethod.NCLOB_GETCHARACTERSTREAM,
          () -> this.nclob.getCharacterStream(pos, length),
          pos,
          length);
    } else {
      return this.nclob.getCharacterStream(pos, length);
    }
  }

  @Override
  public InputStream getAsciiStream() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.NCLOB_GETASCIISTREAM)) {
      return WrapperUtils.executeWithPlugins(
          InputStream.class,
          SQLException.class,
          this.pluginManager,
          this.nclob,
          JdbcMethod.NCLOB_GETASCIISTREAM,
          () -> this.nclob.getAsciiStream());
    } else {
      return this.nclob.getAsciiStream();
    }
  }

  @Override
  public long position(String searchStr, long start) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.NCLOB_POSITION)) {
      return WrapperUtils.executeWithPlugins(
          long.class,
          SQLException.class,
          this.pluginManager,
          this.nclob,
          JdbcMethod.NCLOB_POSITION,
          () -> this.nclob.position(searchStr, start),
          searchStr,
          start);
    } else {
      return this.nclob.position(searchStr, start);
    }
  }

  @Override
  public long position(Clob searchStr, long start) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.NCLOB_POSITION)) {
      return WrapperUtils.executeWithPlugins(
          long.class,
          SQLException.class,
          this.pluginManager,
          this.nclob,
          JdbcMethod.NCLOB_POSITION,
          () -> this.nclob.position(searchStr, start),
          searchStr,
          start);
    } else {
      return this.nclob.position(searchStr, start);
    }
  }

  @Override
  public int setString(long pos, String str) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.NCLOB_SETSTRING)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.nclob,
          JdbcMethod.NCLOB_SETSTRING,
          () -> this.nclob.setString(pos, str),
          pos,
          str);
    } else {
      return this.nclob.setString(pos, str);
    }
  }

  @Override
  public int setString(long pos, String str, int offset, int len) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.NCLOB_SETSTRING)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.nclob,
          JdbcMethod.NCLOB_SETSTRING,
          () -> this.nclob.setString(pos, str, offset, len),
          pos,
          str,
          offset,
          len);
    } else {
      return this.nclob.setString(pos, str, offset, len);
    }
  }

  @Override
  public OutputStream setAsciiStream(long pos) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.NCLOB_SETASCIISTREAM)) {
      return WrapperUtils.executeWithPlugins(
          OutputStream.class,
          SQLException.class,
          this.pluginManager,
          this.nclob,
          JdbcMethod.NCLOB_SETASCIISTREAM,
          () -> this.nclob.setAsciiStream(pos),
          pos);
    } else {
      return this.nclob.setAsciiStream(pos);
    }
  }

  @Override
  public Writer setCharacterStream(long pos) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.NCLOB_SETCHARACTERSTREAM)) {
      return WrapperUtils.executeWithPlugins(
          Writer.class,
          SQLException.class,
          this.pluginManager,
          this.nclob,
          JdbcMethod.NCLOB_SETCHARACTERSTREAM,
          () -> this.nclob.setCharacterStream(pos),
          pos);
    } else {
      return this.nclob.setCharacterStream(pos);
    }
  }

  @Override
  public void truncate(long len) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.NCLOB_TRUNCATE)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.nclob,
          JdbcMethod.NCLOB_TRUNCATE,
          () -> this.nclob.truncate(len),
          len);
    } else {
      this.nclob.truncate(len);
    }
  }

  @Override
  public void free() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.NCLOB_FREE)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.nclob,
          JdbcMethod.NCLOB_FREE,
          () -> this.nclob.free());
    } else {
      this.nclob.free();
    }
  }

  @Override
  public String toString() {
    return super.toString() + " - " + this.nclob;
  }
}
