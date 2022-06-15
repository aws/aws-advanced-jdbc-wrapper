/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.wrapper;

import java.sql.SQLException;
import java.sql.Savepoint;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.aws.rds.jdbc.proxydriver.ConnectionPluginManager;
import software.aws.rds.jdbc.proxydriver.util.WrapperUtils;

public class SavepointWrapper implements Savepoint {

  protected Savepoint savepoint;
  protected ConnectionPluginManager pluginManager;

  public SavepointWrapper(
      @NonNull Savepoint savepoint, @NonNull ConnectionPluginManager pluginManager) {
    this.savepoint = savepoint;
    this.pluginManager = pluginManager;
  }

  @Override
  public int getSavepointId() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.savepoint,
        "Savepoint.getSavepointId",
        () -> this.savepoint.getSavepointId());
  }

  @Override
  public String getSavepointName() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        String.class,
        SQLException.class,
        this.pluginManager,
        this.savepoint,
        "Savepoint.getSavepointName",
        () -> this.savepoint.getSavepointName());
  }
}
