/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.wrapper;

import software.aws.rds.jdbc.proxydriver.ConnectionPluginManager;
import software.aws.rds.jdbc.proxydriver.util.WrapperUtils;

import java.sql.SQLType;

public class SQLTypeWrapper implements SQLType {

    protected SQLType sqlType;
    protected Class<?> sqlTypeClass;
    protected ConnectionPluginManager pluginManager;

    public SQLTypeWrapper(SQLType sqlType, ConnectionPluginManager pluginManager) {
        if (sqlType == null) {
            throw new IllegalArgumentException("sqlType");
        }
        if (pluginManager == null) {
            throw new IllegalArgumentException("pluginManager");
        }

        this.sqlType = sqlType;
        this.sqlTypeClass = this.sqlType.getClass();
        this.pluginManager = pluginManager;
    }

    @Override
    public String getName() {
        return WrapperUtils.executeWithPlugins(
                String.class,
                this.pluginManager,
                this.sqlTypeClass,
                "SQLType.getName",
                () -> this.sqlType.getName());
    }

    @Override
    public String getVendor() {
        return WrapperUtils.executeWithPlugins(
                String.class,
                this.pluginManager,
                this.sqlTypeClass,
                "SQLType.getVendor",
                () -> this.sqlType.getVendor());
    }

    @Override
    public Integer getVendorTypeNumber() {
        return WrapperUtils.executeWithPlugins(
                Integer.class,
                this.pluginManager,
                this.sqlTypeClass,
                "SQLType.getVendorTypeNumber",
                () -> this.sqlType.getVendorTypeNumber());
    }
}
