/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.wrapper;

import software.aws.rds.jdbc.proxydriver.ConnectionPluginManager;
import software.aws.rds.jdbc.proxydriver.util.WrapperUtils;

import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;

public class BlobWrapper implements Blob {

    protected Blob blob;
    protected Class<?> blobClass;
    protected ConnectionPluginManager pluginManager;

    public BlobWrapper(Blob blob, ConnectionPluginManager pluginManager) {
        if (blob == null) {
            throw new IllegalArgumentException("blob");
        }
        if (pluginManager == null) {
            throw new IllegalArgumentException("pluginManager");
        }

        this.blob = blob;
        this.blobClass = this.blob.getClass();
        this.pluginManager = pluginManager;
    }

    @Override
    public long length() throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.blobClass,
                "Blob.length",
                () -> this.blob.length());
    }

    @Override
    public byte[] getBytes(long pos, int length) throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.blobClass,
                "Blob.getBytes",
                () -> this.blob.getBytes(pos, length),
                pos, length);
    }

    @Override
    public InputStream getBinaryStream() throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.blobClass,
                "Blob.getBinaryStream",
                () -> this.blob.getBinaryStream());
    }

    @Override
    public long position(byte[] pattern, long start) throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.blobClass,
                "Blob.position",
                () -> this.blob.position(pattern, start),
                pattern, start);
    }

    @Override
    public long position(Blob pattern, long start) throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.blobClass,
                "Blob.position",
                () -> this.blob.position(pattern, start),
                pattern, start);
    }

    @Override
    public int setBytes(long pos, byte[] bytes) throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.blobClass,
                "Blob.setBytes",
                () -> this.blob.setBytes(pos, bytes),
                pos, bytes);
    }

    @Override
    public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.blobClass,
                "Blob.setBytes",
                () -> this.blob.setBytes(pos, bytes, offset, len),
                pos, bytes, offset, len);
    }

    @Override
    public OutputStream setBinaryStream(long pos) throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.blobClass,
                "Blob.setBinaryStream",
                () -> this.blob.setBinaryStream(pos),
                pos);
    }

    @Override
    public void truncate(long len) throws SQLException {
        WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.blobClass,
                "Blob.truncate",
                () -> {
                    this.blob.truncate(len);
                    return null;
                },
                len);
    }

    @Override
    public void free() throws SQLException {
        WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.blobClass,
                "Blob.free",
                () -> {
                    this.blob.free();
                    return null;
                });
    }

    @Override
    public InputStream getBinaryStream(long pos, long length) throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.blobClass,
                "Blob.getBinaryStream",
                () -> this.blob.getBinaryStream(pos, length),
                pos, length);
    }
}
