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
import software.amazon.jdbc.util.WrapperUtils;

public class ParameterMetaDataWrapper implements ParameterMetaData {

  protected ParameterMetaData parameterMetaData;
  protected ConnectionPluginManager pluginManager;

  public ParameterMetaDataWrapper(
      @NonNull ParameterMetaData parameterMetaData,
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

  @Override
  public String toString() {
    return super.toString() + " - " + this.parameterMetaData;
  }
}
