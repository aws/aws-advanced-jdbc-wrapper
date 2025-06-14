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
import java.sql.Blob;
import java.sql.SQLException;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.ConnectionPluginManager;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.util.WrapperUtils;

public class BlobWrapper implements Blob {

  protected Blob blob;
  protected ConnectionPluginManager pluginManager;

  public BlobWrapper(@NonNull Blob blob, @NonNull ConnectionPluginManager pluginManager) {
    this.blob = blob;
    this.pluginManager = pluginManager;
  }

  @Override
  public long length() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.BLOB_LENGTH)) {
      return WrapperUtils.executeWithPlugins(
          long.class,
          SQLException.class,
          this.pluginManager,
          this.blob,
          JdbcMethod.BLOB_LENGTH,
          () -> this.blob.length());
    } else {
      return this.blob.length();
    }
  }

  @Override
  public byte[] getBytes(long pos, int length) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.BLOB_GETBYTES)) {
      return WrapperUtils.executeWithPlugins(
          byte[].class,
          SQLException.class,
          this.pluginManager,
          this.blob,
          JdbcMethod.BLOB_GETBYTES,
          () -> this.blob.getBytes(pos, length),
          pos,
          length);
    } else {
      return this.blob.getBytes(pos, length);
    }
  }

  @Override
  public InputStream getBinaryStream() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.BLOB_GETBINARYSTREAM)) {
      return WrapperUtils.executeWithPlugins(
          InputStream.class,
          SQLException.class,
          this.pluginManager,
          this.blob,
          JdbcMethod.BLOB_GETBINARYSTREAM,
          () -> this.blob.getBinaryStream());
    } else {
      return this.blob.getBinaryStream();
    }
  }

  @Override
  public InputStream getBinaryStream(long pos, long length) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.BLOB_GETBINARYSTREAM)) {
      return WrapperUtils.executeWithPlugins(
          InputStream.class,
          SQLException.class,
          this.pluginManager,
          this.blob,
          JdbcMethod.BLOB_GETBINARYSTREAM,
          () -> this.blob.getBinaryStream(pos, length),
          pos,
          length);
    } else {
      return this.blob.getBinaryStream(pos, length);
    }
  }

  @Override
  public long position(byte[] pattern, long start) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.BLOB_POSITION)) {
      return WrapperUtils.executeWithPlugins(
          long.class,
          SQLException.class,
          this.pluginManager,
          this.blob,
          JdbcMethod.BLOB_POSITION,
          () -> this.blob.position(pattern, start),
          pattern,
          start);
    } else {
      return this.blob.position(pattern, start);
    }
  }

  @Override
  public long position(Blob pattern, long start) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.BLOB_POSITION)) {
      return WrapperUtils.executeWithPlugins(
          long.class,
          SQLException.class,
          this.pluginManager,
          this.blob,
          JdbcMethod.BLOB_POSITION,
          () -> this.blob.position(pattern, start),
          pattern,
          start);
    } else {
      return this.blob.position(pattern, start);
    }
  }

  @Override
  public int setBytes(long pos, byte[] bytes) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.BLOB_SETBYTES)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.blob,
          JdbcMethod.BLOB_SETBYTES,
          () -> this.blob.setBytes(pos, bytes),
          pos,
          bytes);
    } else {
      return this.blob.setBytes(pos, bytes);
    }
  }

  @Override
  public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.BLOB_SETBYTES)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.pluginManager,
          this.blob,
          JdbcMethod.BLOB_SETBYTES,
          () -> this.blob.setBytes(pos, bytes, offset, len),
          pos,
          bytes,
          offset,
          len);
    } else {
      return this.blob.setBytes(pos, bytes, offset, len);
    }
  }

  @Override
  public OutputStream setBinaryStream(long pos) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.BLOB_SETBINARYSTREAM)) {
      return WrapperUtils.executeWithPlugins(
          OutputStream.class,
          SQLException.class,
          this.pluginManager,
          this.blob,
          JdbcMethod.BLOB_SETBINARYSTREAM,
          () -> this.blob.setBinaryStream(pos),
          pos);
    } else {
      return this.blob.setBinaryStream(pos);
    }
  }

  @Override
  public void truncate(long len) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.BLOB_TRUNCATE)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.blob,
          JdbcMethod.BLOB_TRUNCATE,
          () -> this.blob.truncate(len),
          len);
    } else {
      this.blob.truncate(len);
    }
  }

  @Override
  public void free() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.BLOB_FREE)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.blob,
          JdbcMethod.BLOB_FREE,
          () -> this.blob.free());
    } else {
      this.blob.free();
    }
  }

  @Override
  public String toString() {
    return super.toString() + " - " + this.blob;
  }
}
