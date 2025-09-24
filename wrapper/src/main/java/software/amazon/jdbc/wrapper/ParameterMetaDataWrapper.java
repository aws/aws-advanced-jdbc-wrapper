/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package software.amazon.jdbc.wrapper;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.ConnectionPluginManager;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.util.WrapperUtils;

public class ParameterMetaDataWrapper implements ParameterMetaData {

  protected final ParameterMetaData parameterMetaData;
  protected final ConnectionWrapper connectionWrapper;
  protected final ConnectionPluginManager pluginManager;

  public ParameterMetaDataWrapper(
      @NonNull ParameterMetaData parameterMetaData,
      @NonNull ConnectionWrapper connectionWrapper,
      @NonNull ConnectionPluginManager pluginManager) {
    this.parameterMetaData = parameterMetaData;
    this.connectionWrapper = connectionWrapper;
    this.pluginManager = pluginManager;
  }

  @Override
  public int getParameterCount() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PARAMETERMETADATA_GETPARAMETERCOUNT)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.parameterMetaData,
          JdbcMethod.PARAMETERMETADATA_GETPARAMETERCOUNT,
          this.parameterMetaData::getParameterCount);
    } else {
      return this.parameterMetaData.getParameterCount();
    }
  }

  @SuppressWarnings("MagicConstant")
  @Override
  public int isNullable(int param) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PARAMETERMETADATA_ISNULLABLE)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.parameterMetaData,
          JdbcMethod.PARAMETERMETADATA_ISNULLABLE,
          () -> this.parameterMetaData.isNullable(param),
          param);
    } else {
      return this.parameterMetaData.isNullable(param);
    }
  }

  @Override
  public boolean isSigned(int param) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PARAMETERMETADATA_ISSIGNED)) {
      return WrapperUtils.executeWithPlugins(
          boolean.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.parameterMetaData,
          JdbcMethod.PARAMETERMETADATA_ISSIGNED,
          () -> this.parameterMetaData.isSigned(param),
          param);
    } else {
      return this.parameterMetaData.isSigned(param);
    }
  }

  @Override
  public int getPrecision(int param) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PARAMETERMETADATA_GETPRECISION)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.parameterMetaData,
          JdbcMethod.PARAMETERMETADATA_GETPRECISION,
          () -> this.parameterMetaData.getPrecision(param),
          param);
    } else {
      return this.parameterMetaData.getPrecision(param);
    }
  }

  @Override
  public int getScale(int param) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PARAMETERMETADATA_GETSCALE)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.parameterMetaData,
          JdbcMethod.PARAMETERMETADATA_GETSCALE,
          () -> this.parameterMetaData.getScale(param),
          param);
    } else {
      return this.parameterMetaData.getScale(param);
    }
  }

  @Override
  public int getParameterType(int param) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PARAMETERMETADATA_GETPARAMETERTYPE)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.parameterMetaData,
          JdbcMethod.PARAMETERMETADATA_GETPARAMETERTYPE,
          () -> this.parameterMetaData.getParameterType(param),
          param);
    } else {
      return this.parameterMetaData.getParameterType(param);
    }
  }

  @Override
  public String getParameterTypeName(int param) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PARAMETERMETADATA_GETPARAMETERTYPENAME)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.parameterMetaData,
          JdbcMethod.PARAMETERMETADATA_GETPARAMETERTYPENAME,
          () -> this.parameterMetaData.getParameterTypeName(param),
          param);
    } else {
      return this.parameterMetaData.getParameterTypeName(param);
    }
  }

  @Override
  public String getParameterClassName(int param) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PARAMETERMETADATA_GETPARAMETERCLASSNAME)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.parameterMetaData,
          JdbcMethod.PARAMETERMETADATA_GETPARAMETERCLASSNAME,
          () -> this.parameterMetaData.getParameterClassName(param),
          param);
    } else {
      return this.parameterMetaData.getParameterClassName(param);
    }
  }

  @SuppressWarnings("MagicConstant")
  @Override
  public int getParameterMode(int param) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.PARAMETERMETADATA_GETPARAMETERMODE)) {
      return WrapperUtils.executeWithPlugins(
          int.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.parameterMetaData,
          JdbcMethod.PARAMETERMETADATA_GETPARAMETERMODE,
          () -> this.parameterMetaData.getParameterMode(param),
          param);
    } else {
      return this.parameterMetaData.getParameterMode(param);
    }
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return this.parameterMetaData.unwrap(iface);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return this.parameterMetaData.isWrapperFor(iface);
  }

  @Override
  public String toString() {
    return super.toString() + " - " + this.parameterMetaData;
  }
}
