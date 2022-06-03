/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.wrapper;

import software.aws.rds.jdbc.proxydriver.ConnectionPluginManager;
import software.aws.rds.jdbc.proxydriver.util.WrapperUtils;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class ResultSetMetaDataWrapper implements ResultSetMetaData {

    protected ResultSetMetaData resultSetMetaData;
    protected Class<?> resultSetMetaDataClass;
    protected ConnectionPluginManager pluginManager;

    public ResultSetMetaDataWrapper(ResultSetMetaData resultSetMetaData, ConnectionPluginManager pluginManager) {
        if (resultSetMetaData == null) {
            throw new IllegalArgumentException("resultSetMetaData");
        }
        if (pluginManager == null) {
            throw new IllegalArgumentException("pluginManager");
        }

        this.resultSetMetaData = resultSetMetaData;
        this.resultSetMetaDataClass = this.resultSetMetaData.getClass();
        this.pluginManager = pluginManager;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.resultSetMetaDataClass,
                "ResultSetMetaData.getColumnCount",
                () -> this.resultSetMetaData.getColumnCount());
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.resultSetMetaDataClass,
                "ResultSetMetaData.isAutoIncrement",
                () -> this.resultSetMetaData.isAutoIncrement(column),
                column);
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.resultSetMetaDataClass,
                "ResultSetMetaData.isCaseSensitive",
                () -> this.resultSetMetaData.isCaseSensitive(column),
                column);
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.resultSetMetaDataClass,
                "ResultSetMetaData.isSearchable",
                () -> this.resultSetMetaData.isSearchable(column),
                column);
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.resultSetMetaDataClass,
                "ResultSetMetaData.isCurrency",
                () -> this.resultSetMetaData.isCurrency(column),
                column);
    }

    @Override
    public int isNullable(int column) throws SQLException {
        //noinspection MagicConstant
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.resultSetMetaDataClass,
                "ResultSetMetaData.isNullable",
                () -> this.resultSetMetaData.isNullable(column),
                column);
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.resultSetMetaDataClass,
                "ResultSetMetaData.isSigned",
                () -> this.resultSetMetaData.isSigned(column),
                column);
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.resultSetMetaDataClass,
                "ResultSetMetaData.getColumnDisplaySize",
                () -> this.resultSetMetaData.getColumnDisplaySize(column),
                column);
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.resultSetMetaDataClass,
                "ResultSetMetaData.getColumnLabel",
                () -> this.resultSetMetaData.getColumnLabel(column),
                column);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.resultSetMetaDataClass,
                "ResultSetMetaData.getColumnName",
                () -> this.resultSetMetaData.getColumnName(column),
                column);
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.resultSetMetaDataClass,
                "ResultSetMetaData.getSchemaName",
                () -> this.resultSetMetaData.getSchemaName(column),
                column);
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.resultSetMetaDataClass,
                "ResultSetMetaData.getPrecision",
                () -> this.resultSetMetaData.getPrecision(column),
                column);
    }

    @Override
    public int getScale(int column) throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.resultSetMetaDataClass,
                "ResultSetMetaData.getScale",
                () -> this.resultSetMetaData.getScale(column),
                column);
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.resultSetMetaDataClass,
                "ResultSetMetaData.getTableName",
                () -> this.resultSetMetaData.getTableName(column),
                column);
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.resultSetMetaDataClass,
                "ResultSetMetaData.getCatalogName",
                () -> this.resultSetMetaData.getCatalogName(column),
                column);
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.resultSetMetaDataClass,
                "ResultSetMetaData.getColumnType",
                () -> this.resultSetMetaData.getColumnType(column),
                column);
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.resultSetMetaDataClass,
                "ResultSetMetaData.getColumnTypeName",
                () -> this.resultSetMetaData.getColumnTypeName(column),
                column);
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.resultSetMetaDataClass,
                "ResultSetMetaData.isReadOnly",
                () -> this.resultSetMetaData.isReadOnly(column),
                column);
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.resultSetMetaDataClass,
                "ResultSetMetaData.isWritable",
                () -> this.resultSetMetaData.isWritable(column),
                column);
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.resultSetMetaDataClass,
                "ResultSetMetaData.isDefinitelyWritable",
                () -> this.resultSetMetaData.isDefinitelyWritable(column),
                column);
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.resultSetMetaDataClass,
                "ResultSetMetaData.getColumnClassName",
                () -> this.resultSetMetaData.getColumnClassName(column),
                column);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return this.resultSetMetaData.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return this.resultSetMetaData.isWrapperFor(iface);
    }
}
