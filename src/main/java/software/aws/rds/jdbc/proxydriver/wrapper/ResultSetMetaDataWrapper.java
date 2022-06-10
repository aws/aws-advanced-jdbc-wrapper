/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.wrapper;

import org.checkerframework.checker.nullness.qual.NonNull;
import software.aws.rds.jdbc.proxydriver.ConnectionPluginManager;
import software.aws.rds.jdbc.proxydriver.util.WrapperUtils;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class ResultSetMetaDataWrapper implements ResultSetMetaData {

    protected ResultSetMetaData resultSetMetaData;
    protected ConnectionPluginManager pluginManager;

    public ResultSetMetaDataWrapper(@NonNull ResultSetMetaData resultSetMetaData,
                                    @NonNull ConnectionPluginManager pluginManager) {
        this.resultSetMetaData = resultSetMetaData;
        this.pluginManager = pluginManager;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.resultSetMetaData,
                "ResultSetMetaData.getColumnCount",
                () -> this.resultSetMetaData.getColumnCount());
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.resultSetMetaData,
                "ResultSetMetaData.isAutoIncrement",
                () -> this.resultSetMetaData.isAutoIncrement(column),
                column);
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.resultSetMetaData,
                "ResultSetMetaData.isCaseSensitive",
                () -> this.resultSetMetaData.isCaseSensitive(column),
                column);
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.resultSetMetaData,
                "ResultSetMetaData.isSearchable",
                () -> this.resultSetMetaData.isSearchable(column),
                column);
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.resultSetMetaData,
                "ResultSetMetaData.isCurrency",
                () -> this.resultSetMetaData.isCurrency(column),
                column);
    }

    @Override
    public int isNullable(int column) throws SQLException {
        //noinspection MagicConstant
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.resultSetMetaData,
                "ResultSetMetaData.isNullable",
                () -> this.resultSetMetaData.isNullable(column),
                column);
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.resultSetMetaData,
                "ResultSetMetaData.isSigned",
                () -> this.resultSetMetaData.isSigned(column),
                column);
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.resultSetMetaData,
                "ResultSetMetaData.getColumnDisplaySize",
                () -> this.resultSetMetaData.getColumnDisplaySize(column),
                column);
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                String.class,
                SQLException.class,
                this.pluginManager,
                this.resultSetMetaData,
                "ResultSetMetaData.getColumnLabel",
                () -> this.resultSetMetaData.getColumnLabel(column),
                column);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                String.class,
                SQLException.class,
                this.pluginManager,
                this.resultSetMetaData,
                "ResultSetMetaData.getColumnName",
                () -> this.resultSetMetaData.getColumnName(column),
                column);
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                String.class,
                SQLException.class,
                this.pluginManager,
                this.resultSetMetaData,
                "ResultSetMetaData.getSchemaName",
                () -> this.resultSetMetaData.getSchemaName(column),
                column);
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.resultSetMetaData,
                "ResultSetMetaData.getPrecision",
                () -> this.resultSetMetaData.getPrecision(column),
                column);
    }

    @Override
    public int getScale(int column) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.resultSetMetaData,
                "ResultSetMetaData.getScale",
                () -> this.resultSetMetaData.getScale(column),
                column);
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                String.class,
                SQLException.class,
                this.pluginManager,
                this.resultSetMetaData,
                "ResultSetMetaData.getTableName",
                () -> this.resultSetMetaData.getTableName(column),
                column);
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                String.class,
                SQLException.class,
                this.pluginManager,
                this.resultSetMetaData,
                "ResultSetMetaData.getCatalogName",
                () -> this.resultSetMetaData.getCatalogName(column),
                column);
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.resultSetMetaData,
                "ResultSetMetaData.getColumnType",
                () -> this.resultSetMetaData.getColumnType(column),
                column);
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                String.class,
                SQLException.class,
                this.pluginManager,
                this.resultSetMetaData,
                "ResultSetMetaData.getColumnTypeName",
                () -> this.resultSetMetaData.getColumnTypeName(column),
                column);
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.resultSetMetaData,
                "ResultSetMetaData.isReadOnly",
                () -> this.resultSetMetaData.isReadOnly(column),
                column);
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.resultSetMetaData,
                "ResultSetMetaData.isWritable",
                () -> this.resultSetMetaData.isWritable(column),
                column);
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.resultSetMetaData,
                "ResultSetMetaData.isDefinitelyWritable",
                () -> this.resultSetMetaData.isDefinitelyWritable(column),
                column);
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                String.class,
                SQLException.class,
                this.pluginManager,
                this.resultSetMetaData,
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
