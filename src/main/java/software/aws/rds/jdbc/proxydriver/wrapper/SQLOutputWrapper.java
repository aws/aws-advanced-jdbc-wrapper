/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.wrapper;

import software.aws.rds.jdbc.proxydriver.ConnectionPluginManager;
import software.aws.rds.jdbc.proxydriver.util.WrapperUtils;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;

public class SQLOutputWrapper implements SQLOutput {

    protected SQLOutput sqlOutput;
    protected Class<?> sqlOutputClass;
    protected ConnectionPluginManager pluginManager;

    public SQLOutputWrapper(SQLOutput sqlOutput, ConnectionPluginManager pluginManager) {
        if (sqlOutput == null) {
            throw new IllegalArgumentException("sqlOutput");
        }
        if (pluginManager == null) {
            throw new IllegalArgumentException("pluginManager");
        }

        this.sqlOutput = sqlOutput;
        this.sqlOutputClass = this.sqlOutput.getClass();
        this.pluginManager = pluginManager;
    }

    @Override
    public void writeString(String x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.sqlOutputClass,
                "SQLOutput.writeString",
                () -> {
                    this.sqlOutput.writeString(x);
                    return null;
                },
                x);
    }

    @Override
    public void writeBoolean(boolean x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.sqlOutputClass,
                "SQLOutput.writeBoolean",
                () -> {
                    this.sqlOutput.writeBoolean(x);
                    return null;
                },
                x);
    }

    @Override
    public void writeByte(byte x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.sqlOutputClass,
                "SQLOutput.writeByte",
                () -> {
                    this.sqlOutput.writeByte(x);
                    return null;
                },
                x);
    }

    @Override
    public void writeShort(short x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.sqlOutputClass,
                "SQLOutput.writeShort",
                () -> {
                    this.sqlOutput.writeShort(x);
                    return null;
                },
                x);
    }

    @Override
    public void writeInt(int x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.sqlOutputClass,
                "SQLOutput.writeInt",
                () -> {
                    this.sqlOutput.writeInt(x);
                    return null;
                },
                x);
    }

    @Override
    public void writeLong(long x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.sqlOutputClass,
                "SQLOutput.writeLong",
                () -> {
                    this.sqlOutput.writeLong(x);
                    return null;
                },
                x);
    }

    @Override
    public void writeFloat(float x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.sqlOutputClass,
                "SQLOutput.writeFloat",
                () -> {
                    this.sqlOutput.writeFloat(x);
                    return null;
                },
                x);
    }

    @Override
    public void writeDouble(double x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.sqlOutputClass,
                "SQLOutput.writeDouble",
                () -> {
                    this.sqlOutput.writeDouble(x);
                    return null;
                },
                x);
    }

    @Override
    public void writeBigDecimal(BigDecimal x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.sqlOutputClass,
                "SQLOutput.writeBigDecimal",
                () -> {
                    this.sqlOutput.writeBigDecimal(x);
                    return null;
                },
                x);
    }

    @Override
    public void writeBytes(byte[] x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.sqlOutputClass,
                "SQLOutput.writeBytes",
                () -> {
                    this.sqlOutput.writeBytes(x);
                    return null;
                },
                x);
    }

    @Override
    public void writeDate(Date x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.sqlOutputClass,
                "SQLOutput.writeDate",
                () -> {
                    this.sqlOutput.writeDate(x);
                    return null;
                },
                x);
    }

    @Override
    public void writeTime(Time x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.sqlOutputClass,
                "SQLOutput.writeTime",
                () -> {
                    this.sqlOutput.writeTime(x);
                    return null;
                },
                x);
    }

    @Override
    public void writeTimestamp(Timestamp x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.sqlOutputClass,
                "SQLOutput.writeTimestamp",
                () -> {
                    this.sqlOutput.writeTimestamp(x);
                    return null;
                },
                x);
    }

    @Override
    public void writeCharacterStream(Reader x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.sqlOutputClass,
                "SQLOutput.writeCharacterStream",
                () -> {
                    this.sqlOutput.writeCharacterStream(x);
                    return null;
                },
                x);
    }

    @Override
    public void writeAsciiStream(InputStream x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.sqlOutputClass,
                "SQLOutput.writeAsciiStream",
                () -> {
                    this.sqlOutput.writeAsciiStream(x);
                    return null;
                },
                x);
    }

    @Override
    public void writeBinaryStream(InputStream x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.sqlOutputClass,
                "SQLOutput.writeBinaryStream",
                () -> {
                    this.sqlOutput.writeBinaryStream(x);
                    return null;
                },
                x);
    }

    @Override
    public void writeObject(SQLData x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.sqlOutputClass,
                "SQLOutput.writeObject",
                () -> {
                    this.sqlOutput.writeObject(x);
                    return null;
                },
                x);
    }

    @Override
    public void writeRef(Ref x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.sqlOutputClass,
                "SQLOutput.writeRef",
                () -> {
                    this.sqlOutput.writeRef(x);
                    return null;
                },
                x);
    }

    @Override
    public void writeBlob(Blob x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.sqlOutputClass,
                "SQLOutput.writeBlob",
                () -> {
                    this.sqlOutput.writeBlob(x);
                    return null;
                },
                x);
    }

    @Override
    public void writeClob(Clob x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.sqlOutputClass,
                "SQLOutput.writeClob",
                () -> {
                    this.sqlOutput.writeClob(x);
                    return null;
                },
                x);
    }

    @Override
    public void writeStruct(Struct x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.sqlOutputClass,
                "SQLOutput.writeStruct",
                () -> {
                    this.sqlOutput.writeStruct(x);
                    return null;
                },
                x);
    }

    @Override
    public void writeArray(Array x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.sqlOutputClass,
                "SQLOutput.writeArray",
                () -> {
                    this.sqlOutput.writeArray(x);
                    return null;
                },
                x);
    }

    @Override
    public void writeURL(URL x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.sqlOutputClass,
                "SQLOutput.writeURL",
                () -> {
                    this.sqlOutput.writeURL(x);
                    return null;
                },
                x);
    }

    @Override
    public void writeNString(String x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.sqlOutputClass,
                "SQLOutput.writeNString",
                () -> {
                    this.sqlOutput.writeNString(x);
                    return null;
                },
                x);
    }

    @Override
    public void writeNClob(NClob x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.sqlOutputClass,
                "SQLOutput.writeNClob",
                () -> {
                    this.sqlOutput.writeNClob(x);
                    return null;
                },
                x);
    }

    @Override
    public void writeRowId(RowId x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.sqlOutputClass,
                "SQLOutput.writeRowId",
                () -> {
                    this.sqlOutput.writeRowId(x);
                    return null;
                },
                x);
    }

    @Override
    public void writeSQLXML(SQLXML x) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.sqlOutputClass,
                "SQLOutput.writeSQLXML",
                () -> {
                    this.sqlOutput.writeSQLXML(x);
                    return null;
                },
                x);
    }

    @Override
    public void writeObject(Object x, SQLType targetSqlType) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.sqlOutputClass,
                "SQLOutput.writeObject",
                () -> {
                    this.sqlOutput.writeObject(x, targetSqlType);
                    return null;
                },
                x, targetSqlType);
    }
}
