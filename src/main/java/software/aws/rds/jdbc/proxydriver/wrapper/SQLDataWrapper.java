/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.wrapper;

import org.checkerframework.checker.nullness.qual.NonNull;
import software.aws.rds.jdbc.proxydriver.ConnectionPluginManager;
import software.aws.rds.jdbc.proxydriver.util.WrapperUtils;

import java.sql.*;

public class SQLDataWrapper implements SQLData {

    protected SQLData sqlData;
    protected ConnectionPluginManager pluginManager;

    public SQLDataWrapper(@NonNull SQLData sqlData, @NonNull ConnectionPluginManager pluginManager) {
        this.sqlData = sqlData;
        this.pluginManager = pluginManager;
    }

    @Override
    public String getSQLTypeName() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                String.class,
                SQLException.class,
                this.pluginManager,
                this.sqlData,
                "SQLData.getSQLTypeName",
                () -> this.sqlData.getSQLTypeName());
    }

    @Override
    public void readSQL(SQLInput stream, String typeName) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.sqlData,
                "SQLData.readSQL",
                () -> this.sqlData.readSQL(stream, typeName),
                stream, typeName);
    }

    @Override
    public void writeSQL(SQLOutput stream) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.sqlData,
                "SQLData.writeSQL",
                () -> this.sqlData.writeSQL(stream),
                stream);
    }
}
