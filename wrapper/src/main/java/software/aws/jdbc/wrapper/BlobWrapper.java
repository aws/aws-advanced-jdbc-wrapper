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

import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.aws.jdbc.ConnectionPluginManager;
import software.aws.jdbc.util.WrapperUtils;

public class BlobWrapper implements Blob {

  protected Blob blob;
  protected ConnectionPluginManager pluginManager;

  public BlobWrapper(@NonNull Blob blob, @NonNull ConnectionPluginManager pluginManager) {
    this.blob = blob;
    this.pluginManager = pluginManager;
  }

  @Override
  public long length() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        long.class,
        SQLException.class,
        this.pluginManager,
        this.blob,
        "Blob.length",
        () -> this.blob.length());
  }

  @Override
  public byte[] getBytes(long pos, int length) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        byte[].class,
        SQLException.class,
        this.pluginManager,
        this.blob,
        "Blob.getBytes",
        () -> this.blob.getBytes(pos, length),
        pos,
        length);
  }

  @Override
  public InputStream getBinaryStream() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        InputStream.class,
        SQLException.class,
        this.pluginManager,
        this.blob,
        "Blob.getBinaryStream",
        () -> this.blob.getBinaryStream());
  }

  @Override
  public InputStream getBinaryStream(long pos, long length) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        InputStream.class,
        SQLException.class,
        this.pluginManager,
        this.blob,
        "Blob.getBinaryStream",
        () -> this.blob.getBinaryStream(pos, length),
        pos,
        length);
  }

  @Override
  public long position(byte[] pattern, long start) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        long.class,
        SQLException.class,
        this.pluginManager,
        this.blob,
        "Blob.position",
        () -> this.blob.position(pattern, start),
        pattern,
        start);
  }

  @Override
  public long position(Blob pattern, long start) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        long.class,
        SQLException.class,
        this.pluginManager,
        this.blob,
        "Blob.position",
        () -> this.blob.position(pattern, start),
        pattern,
        start);
  }

  @Override
  public int setBytes(long pos, byte[] bytes) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.blob,
        "Blob.setBytes",
        () -> this.blob.setBytes(pos, bytes),
        pos,
        bytes);
  }

  @Override
  public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.blob,
        "Blob.setBytes",
        () -> this.blob.setBytes(pos, bytes, offset, len),
        pos,
        bytes,
        offset,
        len);
  }

  @Override
  public OutputStream setBinaryStream(long pos) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        OutputStream.class,
        SQLException.class,
        this.pluginManager,
        this.blob,
        "Blob.setBinaryStream",
        () -> this.blob.setBinaryStream(pos),
        pos);
  }

  @Override
  public void truncate(long len) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.blob,
        "Blob.truncate",
        () -> this.blob.truncate(len),
        len);
  }

  @Override
  public void free() throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class, this.pluginManager, this.blob, "Blob.free", () -> this.blob.free());
  }
}
