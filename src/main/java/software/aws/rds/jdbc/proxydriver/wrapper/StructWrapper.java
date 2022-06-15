/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.wrapper;

import java.sql.SQLException;
import java.sql.Struct;
import java.util.Map;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.aws.rds.jdbc.proxydriver.ConnectionPluginManager;
import software.aws.rds.jdbc.proxydriver.util.WrapperUtils;

public class StructWrapper implements Struct {

  protected Struct struct;
  protected ConnectionPluginManager pluginManager;

  public StructWrapper(@NonNull Struct struct, @NonNull ConnectionPluginManager pluginManager) {
    this.struct = struct;
    this.pluginManager = pluginManager;
  }

  @Override
  public String getSQLTypeName() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        String.class,
        SQLException.class,
        this.pluginManager,
        this.struct,
        "Struct.getSQLTypeName",
        () -> this.struct.getSQLTypeName());
  }

  @Override
  public Object[] getAttributes() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Object[].class,
        SQLException.class,
        this.pluginManager,
        this.struct,
        "Struct.getAttributes",
        () -> this.struct.getAttributes());
  }

  @Override
  public Object[] getAttributes(Map<String, Class<?>> map) throws SQLException {
    return WrapperUtils.executeWithPlugins(
        Object[].class,
        SQLException.class,
        this.pluginManager,
        this.struct,
        "Struct.getAttributes",
        () -> this.struct.getAttributes(map),
        map);
  }
}
