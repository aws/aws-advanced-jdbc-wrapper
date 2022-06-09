/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.wrapper;

import software.aws.rds.jdbc.proxydriver.ConnectionPluginManager;
import software.aws.rds.jdbc.proxydriver.util.WrapperUtils;

import java.sql.SQLException;
import java.sql.Savepoint;

public class SavepointWrapper implements Savepoint {

    protected Savepoint savepoint;
    protected Class<?> savepointClass;
    protected ConnectionPluginManager pluginManager;

    public SavepointWrapper(Savepoint savepoint, ConnectionPluginManager pluginManager) {
        if (savepoint == null) {
            throw new IllegalArgumentException("savepoint");
        }
        if (pluginManager == null) {
            throw new IllegalArgumentException("pluginManager");
        }

        this.savepoint = savepoint;
        this.savepointClass = this.savepoint.getClass();
        this.pluginManager = pluginManager;
    }

    @Override
    public int getSavepointId() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.savepointClass,
                "Savepoint.getSavepointId",
                () -> this.savepoint.getSavepointId());
    }

    @Override
    public String getSavepointName() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                String.class,
                SQLException.class,
                this.pluginManager,
                this.savepointClass,
                "Savepoint.getSavepointName",
                () -> this.savepoint.getSavepointName());
    }
}
