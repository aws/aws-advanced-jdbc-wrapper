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
import java.io.Reader;
import java.io.Writer;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLException;

public class NClobWrapper implements NClob {

    protected NClob nclob;
    protected Class<?> nclobClass;
    protected ConnectionPluginManager pluginManager;

    public NClobWrapper(NClob nclob, ConnectionPluginManager pluginManager) {
        if (nclob == null) {
            throw new IllegalArgumentException("nclob");
        }
        if (pluginManager == null) {
            throw new IllegalArgumentException("pluginManager");
        }

        this.nclob = nclob;
        this.nclobClass = this.nclob.getClass();
        this.pluginManager = pluginManager;
    }

    @Override
    public long length() throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.nclobClass,
                "NClob.length",
                () -> this.nclob.length());
    }

    @Override
    public String getSubString(long pos, int length) throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.nclobClass,
                "NClob.getSubString",
                () -> this.nclob.getSubString(pos, length),
                pos, length);
    }

    @Override
    public Reader getCharacterStream() throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.nclobClass,
                "NClob.getCharacterStream",
                () -> this.nclob.getCharacterStream());
    }

    @Override
    public InputStream getAsciiStream() throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.nclobClass,
                "NClob.getAsciiStream",
                () -> this.nclob.getAsciiStream());
    }

    @Override
    public long position(String searchStr, long start) throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.nclobClass,
                "NClob.position",
                () -> this.nclob.position(searchStr, start),
                searchStr, start);
    }

    @Override
    public long position(Clob searchStr, long start) throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.nclobClass,
                "NClob.position",
                () -> this.nclob.position(searchStr, start),
                searchStr, start);
    }

    @Override
    public int setString(long pos, String str) throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.nclobClass,
                "NClob.setString",
                () -> this.nclob.setString(pos, str),
                pos, str);
    }

    @Override
    public int setString(long pos, String str, int offset, int len) throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.nclobClass,
                "NClob.setString",
                () -> this.nclob.setString(pos, str, offset, len),
                pos, str, offset, len);
    }

    @Override
    public OutputStream setAsciiStream(long pos) throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.nclobClass,
                "NClob.setAsciiStream",
                () -> this.nclob.setAsciiStream(pos),
                pos);
    }

    @Override
    public Writer setCharacterStream(long pos) throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.nclobClass,
                "NClob.setCharacterStream",
                () -> this.nclob.setCharacterStream(pos),
                pos);
    }

    @Override
    public void truncate(long len) throws SQLException {
        WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.nclobClass,
                "NClob.truncate",
                () -> {
                    this.nclob.truncate(len);
                    return null;
                },
                len);
    }

    @Override
    public void free() throws SQLException {
        WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.nclobClass,
                "NClob.free",
                () -> {
                    this.nclob.free();
                    return null;
                });
    }

    @Override
    public Reader getCharacterStream(long pos, long length) throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.nclobClass,
                "NClob.getCharacterStream",
                () -> this.nclob.getCharacterStream(pos, length),
                pos, length);
    }
}
