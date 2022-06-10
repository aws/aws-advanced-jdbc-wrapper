/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.wrapper;

import org.checkerframework.checker.nullness.qual.NonNull;
import software.aws.rds.jdbc.proxydriver.ConnectionPluginManager;
import software.aws.rds.jdbc.proxydriver.util.WrapperUtils;

import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;

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
                pos, length);
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
    public long position(byte[] pattern, long start) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                long.class,
                SQLException.class,
                this.pluginManager,
                this.blob,
                "Blob.position",
                () -> this.blob.position(pattern, start),
                pattern, start);
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
                pattern, start);
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
                pos, bytes);
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
                pos, bytes, offset, len);
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
                SQLException.class,
                this.pluginManager,
                this.blob,
                "Blob.free",
                () -> this.blob.free());
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
                pos, length);
    }
}
