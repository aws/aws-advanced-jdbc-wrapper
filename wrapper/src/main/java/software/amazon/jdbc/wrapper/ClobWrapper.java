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
    return WrapperUtils.executeWithPlugins(
        long.class,
        SQLException.class,
        this.pluginManager,
        this.clob,
        "Clob.length",
        () -> this.clob.length());
  }

  @Override
  public String getSubString(long pos, int length) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        String.class,
        SQLException.class,
        this.pluginManager,
        this.clob,
        "Clob.getSubString",
        () -> this.clob.getSubString(pos, length),
        pos,
        length);
  }

  @Override
  public Reader getCharacterStream() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Reader.class,
        SQLException.class,
        this.pluginManager,
        this.clob,
        "Clob.getCharacterStream",
        () -> this.clob.getCharacterStream());
  }

  @Override
  public Reader getCharacterStream(long pos, long length) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Reader.class,
        SQLException.class,
        this.pluginManager,
        this.clob,
        "Clob.getCharacterStream",
        () -> this.clob.getCharacterStream(pos, length),
        pos,
        length);
  }

  @Override
  public InputStream getAsciiStream() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        InputStream.class,
        SQLException.class,
        this.pluginManager,
        this.clob,
        "Clob.getAsciiStream",
        () -> this.clob.getAsciiStream());
  }

  @Override
  public long position(String searchStr, long start) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        long.class,
        SQLException.class,
        this.pluginManager,
        this.clob,
        "Clob.position",
        () -> this.clob.position(searchStr, start),
        searchStr,
        start);
  }

  @Override
  public long position(Clob searchStr, long start) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        long.class,
        SQLException.class,
        this.pluginManager,
        this.clob,
        "Clob.position",
        () -> this.clob.position(searchStr, start),
        searchStr,
        start);
  }

  @Override
  public int setString(long pos, String str) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.clob,
        "Clob.setString",
        () -> this.clob.setString(pos, str),
        pos,
        str);
  }

  @Override
  public int setString(long pos, String str, int offset, int len) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.clob,
        "Clob.setString",
        () -> this.clob.setString(pos, str, offset, len),
        pos,
        str,
        offset,
        len);
  }

  @Override
  public OutputStream setAsciiStream(long pos) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        OutputStream.class,
        SQLException.class,
        this.pluginManager,
        this.clob,
        "Clob.setAsciiStream",
        () -> this.clob.setAsciiStream(pos),
        pos);
  }

  @Override
  public Writer setCharacterStream(long pos) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Writer.class,
        SQLException.class,
        this.pluginManager,
        this.clob,
        "Clob.setCharacterStream",
        () -> this.clob.setCharacterStream(pos),
        pos);
  }

  @Override
  public void truncate(long len) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.clob,
        "Clob.truncate",
        () -> this.clob.truncate(len),
        len);
  }

  @Override
  public void free() throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class, this.pluginManager, this.clob, "Clob.free", () -> this.clob.free());
  }
}
