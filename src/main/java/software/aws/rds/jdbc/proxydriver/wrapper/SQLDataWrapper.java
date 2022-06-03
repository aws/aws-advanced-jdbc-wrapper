/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.wrapper;

import software.aws.rds.jdbc.proxydriver.ConnectionPluginManager;
import software.aws.rds.jdbc.proxydriver.util.WrapperUtils;

import java.sql.*;

public class SQLDataWrapper implements SQLData {

    protected SQLData sqlData;
    protected Class<?> sqlDataClass;
    protected ConnectionPluginManager pluginManager;

    public SQLDataWrapper(SQLData sqlData, ConnectionPluginManager pluginManager) {
        if (sqlData == null) {
            throw new IllegalArgumentException("sqlData");
        }
        if (pluginManager == null) {
            throw new IllegalArgumentException("pluginManager");
        }

        this.sqlData = sqlData;
        this.sqlDataClass = this.sqlData.getClass();
        this.pluginManager = pluginManager;
    }

    @Override
    public String getSQLTypeName() throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.sqlDataClass,
                "SQLData.getSQLTypeName",
                () -> this.sqlData.getSQLTypeName());
    }

    @Override
    public void readSQL(SQLInput stream, String typeName) throws SQLException {
        WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.sqlDataClass,
                "SQLData.readSQL",
                () -> {
                    this.sqlData.readSQL(stream, typeName);
                    return null;
                },
                stream, typeName);
    }

    @Override
    public void writeSQL(SQLOutput stream) throws SQLException {
        WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.sqlDataClass,
                "SQLData.writeSQL",
                () -> {
                    this.sqlData.writeSQL(stream);
                    return null;
                },
                stream);
    }
}
