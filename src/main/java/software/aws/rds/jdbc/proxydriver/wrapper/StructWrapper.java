/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.wrapper;

import software.aws.rds.jdbc.proxydriver.ConnectionPluginManager;
import software.aws.rds.jdbc.proxydriver.util.WrapperUtils;

import java.sql.SQLException;
import java.sql.Struct;
import java.util.Map;

public class StructWrapper implements Struct {

    protected Struct struct;
    protected Class<?> structClass;
    protected ConnectionPluginManager pluginManager;

    public StructWrapper(Struct struct, ConnectionPluginManager pluginManager) {
        if (struct == null) {
            throw new IllegalArgumentException("struct");
        }
        if (pluginManager == null) {
            throw new IllegalArgumentException("pluginManager");
        }

        this.struct = struct;
        this.structClass = this.struct.getClass();
        this.pluginManager = pluginManager;
    }

    @Override
    public String getSQLTypeName() throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.structClass,
                "Struct.getSQLTypeName",
                () -> this.struct.getSQLTypeName());
    }

    @Override
    public Object[] getAttributes() throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.structClass,
                "Struct.getAttributes",
                () -> this.struct.getAttributes());
    }

    @Override
    public Object[] getAttributes(Map<String, Class<?>> map) throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.structClass,
                "Struct.getAttributes",
                () -> this.struct.getAttributes(map),
                map);
    }
}
