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

public class SQLInputWrapper implements SQLInput {

    protected SQLInput sqlInput;
    protected Class<?> sqlInputClass;
    protected ConnectionPluginManager pluginManager;

    public SQLInputWrapper(SQLInput sqlInput, ConnectionPluginManager pluginManager) {
        if (sqlInput == null) {
            throw new IllegalArgumentException("sqlInput");
        }
        if (pluginManager == null) {
            throw new IllegalArgumentException("pluginManager");
        }

        this.sqlInput = sqlInput;
        this.sqlInputClass = this.sqlInput.getClass();
        this.pluginManager = pluginManager;
    }

    @Override
    public String readString() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                String.class,
                SQLException.class,
                this.pluginManager,
                this.sqlInputClass,
                "SQLInput.readString",
                () -> this.sqlInput.readString());
    }

    @Override
    public boolean readBoolean() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.sqlInputClass,
                "SQLInput.readBoolean",
                () -> this.sqlInput.readBoolean());
    }

    @Override
    public byte readByte() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                byte.class,
                SQLException.class,
                this.pluginManager,
                this.sqlInputClass,
                "SQLInput.readByte",
                () -> this.sqlInput.readByte());
    }

    @Override
    public short readShort() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                short.class,
                SQLException.class,
                this.pluginManager,
                this.sqlInputClass,
                "SQLInput.readShort",
                () -> this.sqlInput.readShort());
    }

    @Override
    public int readInt() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.sqlInputClass,
                "SQLInput.readInt",
                () -> this.sqlInput.readInt());
    }

    @Override
    public long readLong() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                long.class,
                SQLException.class,
                this.pluginManager,
                this.sqlInputClass,
                "SQLInput.readLong",
                () -> this.sqlInput.readLong());
    }

    @Override
    public float readFloat() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                float.class,
                SQLException.class,
                this.pluginManager,
                this.sqlInputClass,
                "SQLInput.readFloat",
                () -> this.sqlInput.readFloat());
    }

    @Override
    public double readDouble() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                double.class,
                SQLException.class,
                this.pluginManager,
                this.sqlInputClass,
                "SQLInput.readDouble",
                () -> this.sqlInput.readDouble());
    }

    @Override
    public BigDecimal readBigDecimal() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                BigDecimal.class,
                SQLException.class,
                this.pluginManager,
                this.sqlInputClass,
                "SQLInput.readBigDecimal",
                () -> this.sqlInput.readBigDecimal());
    }

    @Override
    public byte[] readBytes() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                byte[].class,
                SQLException.class,
                this.pluginManager,
                this.sqlInputClass,
                "SQLInput.readBytes",
                () -> this.sqlInput.readBytes());
    }

    @Override
    public Date readDate() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                Date.class,
                SQLException.class,
                this.pluginManager,
                this.sqlInputClass,
                "SQLInput.readDate",
                () -> this.sqlInput.readDate());
    }

    @Override
    public Time readTime() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                Time.class,
                SQLException.class,
                this.pluginManager,
                this.sqlInputClass,
                "SQLInput.readTime",
                () -> this.sqlInput.readTime());
    }

    @Override
    public Timestamp readTimestamp() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                Timestamp.class,
                SQLException.class,
                this.pluginManager,
                this.sqlInputClass,
                "SQLInput.readTimestamp",
                () -> this.sqlInput.readTimestamp());
    }

    @Override
    public Reader readCharacterStream() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                Reader.class,
                SQLException.class,
                this.pluginManager,
                this.sqlInputClass,
                "SQLInput.readCharacterStream",
                () -> this.sqlInput.readCharacterStream());
    }

    @Override
    public InputStream readAsciiStream() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                InputStream.class,
                SQLException.class,
                this.pluginManager,
                this.sqlInputClass,
                "SQLInput.readAsciiStream",
                () -> this.sqlInput.readAsciiStream());
    }

    @Override
    public InputStream readBinaryStream() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                InputStream.class,
                SQLException.class,
                this.pluginManager,
                this.sqlInputClass,
                "SQLInput.readBinaryStream",
                () -> this.sqlInput.readBinaryStream());
    }

    @Override
    public Object readObject() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                Object.class,
                SQLException.class,
                this.pluginManager,
                this.sqlInputClass,
                "SQLInput.readObject",
                () -> this.sqlInput.readObject());
    }

    @Override
    public Ref readRef() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                Ref.class,
                SQLException.class,
                this.pluginManager,
                this.sqlInputClass,
                "SQLInput.readRef",
                () -> this.sqlInput.readRef());
    }

    @Override
    public Blob readBlob() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                Blob.class,
                SQLException.class,
                this.pluginManager,
                this.sqlInputClass,
                "SQLInput.readBlob",
                () -> this.sqlInput.readBlob());
    }

    @Override
    public Clob readClob() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                Clob.class,
                SQLException.class,
                this.pluginManager,
                this.sqlInputClass,
                "SQLInput.readClob",
                () -> this.sqlInput.readClob());
    }

    @Override
    public Array readArray() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                Array.class,
                SQLException.class,
                this.pluginManager,
                this.sqlInputClass,
                "SQLInput.readArray",
                () -> this.sqlInput.readArray());
    }

    @Override
    public boolean wasNull() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.sqlInputClass,
                "SQLInput.wasNull",
                () -> this.sqlInput.wasNull());
    }

    @Override
    public URL readURL() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                URL.class,
                SQLException.class,
                this.pluginManager,
                this.sqlInputClass,
                "SQLInput.readURL",
                () -> this.sqlInput.readURL());
    }

    @Override
    public NClob readNClob() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                NClob.class,
                SQLException.class,
                this.pluginManager,
                this.sqlInputClass,
                "SQLInput.readNClob",
                () -> this.sqlInput.readNClob());
    }

    @Override
    public String readNString() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                String.class,
                SQLException.class,
                this.pluginManager,
                this.sqlInputClass,
                "SQLInput.readNString",
                () -> this.sqlInput.readNString());
    }

    @Override
    public SQLXML readSQLXML() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                SQLXML.class,
                SQLException.class,
                this.pluginManager,
                this.sqlInputClass,
                "SQLInput.readSQLXML",
                () -> this.sqlInput.readSQLXML());
    }

    @Override
    public RowId readRowId() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                RowId.class,
                SQLException.class,
                this.pluginManager,
                this.sqlInputClass,
                "SQLInput.readRowId",
                () -> this.sqlInput.readRowId());
    }

    @Override
    public <T> T readObject(Class<T> type) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                type,
                SQLException.class,
                this.pluginManager,
                this.sqlInputClass,
                "SQLInput.readString",
                () -> this.sqlInput.readObject(type),
                type);
    }
}
