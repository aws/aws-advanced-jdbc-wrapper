/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.wrapper;

import org.checkerframework.checker.nullness.qual.NonNull;
import software.aws.rds.jdbc.proxydriver.ConnectionPluginManager;
import software.aws.rds.jdbc.proxydriver.util.WrapperUtils;

import java.sql.ParameterMetaData;
import java.sql.SQLException;

public class ParameterMetaDataWrapper implements ParameterMetaData {

    protected ParameterMetaData parameterMetaData;
    protected ConnectionPluginManager pluginManager;

    public ParameterMetaDataWrapper(@NonNull ParameterMetaData parameterMetaData,
                                    @NonNull ConnectionPluginManager pluginManager) {
        this.parameterMetaData = parameterMetaData;
        this.pluginManager = pluginManager;
    }

    @Override
    public int getParameterCount() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.parameterMetaData,
                "ParameterMetaData.getParameterCount",
                () -> this.parameterMetaData.getParameterCount());
    }

    @SuppressWarnings("MagicConstant")
    @Override
    public int isNullable(int param) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.parameterMetaData,
                "ParameterMetaData.isNullable",
                () -> this.parameterMetaData.isNullable(param),
                param);
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                boolean.class,
                SQLException.class,
                this.pluginManager,
                this.parameterMetaData,
                "ParameterMetaData.isSigned",
                () -> this.parameterMetaData.isSigned(param),
                param);
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.parameterMetaData,
                "ParameterMetaData.getPrecision",
                () -> this.parameterMetaData.getPrecision(param),
                param);
    }

    @Override
    public int getScale(int param) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.parameterMetaData,
                "ParameterMetaData.getScale",
                () -> this.parameterMetaData.getScale(param),
                param);
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.parameterMetaData,
                "ParameterMetaData.getParameterType",
                () -> this.parameterMetaData.getParameterType(param),
                param);
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                String.class,
                SQLException.class,
                this.pluginManager,
                this.parameterMetaData,
                "ParameterMetaData.getParameterTypeName",
                () -> this.parameterMetaData.getParameterTypeName(param),
                param);
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                String.class,
                SQLException.class,
                this.pluginManager,
                this.parameterMetaData,
                "ParameterMetaData.getParameterClassName",
                () -> this.parameterMetaData.getParameterClassName(param),
                param);
    }

    @SuppressWarnings("MagicConstant")
    @Override
    public int getParameterMode(int param) throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.parameterMetaData,
                "ParameterMetaData.getParameterMode",
                () -> this.parameterMetaData.getParameterMode(param),
                param);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return this.parameterMetaData.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return this.parameterMetaData.isWrapperFor(iface);
    }
}
