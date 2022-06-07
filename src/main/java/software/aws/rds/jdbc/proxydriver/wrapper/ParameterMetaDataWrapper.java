/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.wrapper;

import software.aws.rds.jdbc.proxydriver.ConnectionPluginManager;
import software.aws.rds.jdbc.proxydriver.util.WrapperUtils;

import java.sql.ParameterMetaData;
import java.sql.SQLException;

public class ParameterMetaDataWrapper implements ParameterMetaData {

    protected ParameterMetaData parameterMetaData;
    protected Class<?> parameterMetaDataClass;
    protected ConnectionPluginManager pluginManager;

    public ParameterMetaDataWrapper(ParameterMetaData parameterMetaData, ConnectionPluginManager pluginManager) {
        if (parameterMetaData == null) {
            throw new IllegalArgumentException("parameterMetaData");
        }
        if (pluginManager == null) {
            throw new IllegalArgumentException("pluginManager");
        }

        this.parameterMetaData = parameterMetaData;
        this.parameterMetaDataClass = this.parameterMetaData.getClass();
        this.pluginManager = pluginManager;
    }

    @Override
    public int getParameterCount() throws SQLException {
        return WrapperUtils.executeWithPlugins(
                int.class,
                SQLException.class,
                this.pluginManager,
                this.parameterMetaDataClass,
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
                this.parameterMetaDataClass,
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
                this.parameterMetaDataClass,
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
                this.parameterMetaDataClass,
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
                this.parameterMetaDataClass,
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
                this.parameterMetaDataClass,
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
                this.parameterMetaDataClass,
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
                this.parameterMetaDataClass,
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
                this.parameterMetaDataClass,
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
