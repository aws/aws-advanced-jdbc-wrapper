/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.wrapper;

import software.aws.rds.jdbc.proxydriver.ConnectionPluginManager;
import software.aws.rds.jdbc.proxydriver.util.WrapperUtils;

import java.sql.Ref;
import java.sql.SQLException;
import java.util.Map;

public class RefWrapper implements Ref {

    protected Ref ref;
    protected Class<?> refClass;
    protected ConnectionPluginManager pluginManager;

    public RefWrapper(Ref ref, ConnectionPluginManager pluginManager) {
        if (ref == null) {
            throw new IllegalArgumentException("ref");
        }
        if (pluginManager == null) {
            throw new IllegalArgumentException("pluginManager");
        }

        this.ref = ref;
        this.refClass = this.ref.getClass();
        this.pluginManager = pluginManager;
    }

    @Override
    public String getBaseTypeName() throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.refClass,
                "Ref.getBaseTypeName",
                () -> this.ref.getBaseTypeName());
    }

    @Override
    public Object getObject(Map<String, Class<?>> map) throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.refClass,
                "Ref.getObject",
                () -> this.ref.getObject(map),
                map);
    }

    @Override
    public Object getObject() throws SQLException {
        return WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.refClass,
                "Ref.getObject",
                () -> this.ref.getObject());
    }

    @Override
    public void setObject(Object value) throws SQLException {
        WrapperUtils.executeWithPlugins_SQLException(this.pluginManager,
                this.refClass,
                "Ref.setObject",
                () -> {
                    this.ref.setObject(value);
                    return null;
                },
                value);
    }
}
