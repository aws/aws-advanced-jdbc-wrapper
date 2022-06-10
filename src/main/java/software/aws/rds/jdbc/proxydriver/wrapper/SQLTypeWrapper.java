/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.wrapper;

import org.checkerframework.checker.nullness.qual.NonNull;
import software.aws.rds.jdbc.proxydriver.ConnectionPluginManager;
import software.aws.rds.jdbc.proxydriver.util.WrapperUtils;

import java.sql.SQLType;

public class SQLTypeWrapper implements SQLType {

    protected SQLType sqlType;
    protected ConnectionPluginManager pluginManager;

    public SQLTypeWrapper(@NonNull SQLType sqlType, @NonNull ConnectionPluginManager pluginManager) {
        this.sqlType = sqlType;
        this.pluginManager = pluginManager;
    }

    @Override
    public String getName() {
        return WrapperUtils.executeWithPlugins(
                String.class,
                this.pluginManager,
                this.sqlType,
                "SQLType.getName",
                () -> this.sqlType.getName());
    }

    @Override
    public String getVendor() {
        return WrapperUtils.executeWithPlugins(
                String.class,
                this.pluginManager,
                this.sqlType,
                "SQLType.getVendor",
                () -> this.sqlType.getVendor());
    }

    @Override
    public Integer getVendorTypeNumber() {
        return WrapperUtils.executeWithPlugins(
                Integer.class,
                this.pluginManager,
                this.sqlType,
                "SQLType.getVendorTypeNumber",
                () -> this.sqlType.getVendorTypeNumber());
    }
}
