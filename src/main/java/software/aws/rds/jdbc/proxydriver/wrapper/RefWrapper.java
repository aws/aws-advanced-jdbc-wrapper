/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.wrapper;

import org.checkerframework.checker.nullness.qual.NonNull;
import software.aws.rds.jdbc.proxydriver.ConnectionPluginManager;
import software.aws.rds.jdbc.proxydriver.util.WrapperUtils;

import java.sql.Ref;
import java.sql.SQLException;
import java.util.Map;

public class RefWrapper implements Ref {

    protected Ref ref;
    protected ConnectionPluginManager pluginManager;

    public RefWrapper(@NonNull Ref ref, @NonNull ConnectionPluginManager pluginManager) {
        this.ref = ref;
        this.pluginManager = pluginManager;
    }

    @Override
    public String getBaseTypeName() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                String.class,
                SQLException.class,
                this.pluginManager,
                this.ref,
                "Ref.getBaseTypeName",
                () -> this.ref.getBaseTypeName());
    }

    @Override
    public Object getObject(Map<String, Class<?>> map) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                Object.class,
                SQLException.class,
                this.pluginManager,
                this.ref,
                "Ref.getObject",
                () -> this.ref.getObject(map),
                map);
    }

    @Override
    public Object getObject() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                Object.class,
                SQLException.class,
                this.pluginManager,
                this.ref,
                "Ref.getObject",
                () -> this.ref.getObject());
    }

    @Override
    public void setObject(Object value) throws SQLException {
        WrapperUtils.runWithPlugins(
                SQLException.class,
                this.pluginManager,
                this.ref,
                "Ref.setObject",
                () -> this.ref.setObject(value),
                value);
    }
}
