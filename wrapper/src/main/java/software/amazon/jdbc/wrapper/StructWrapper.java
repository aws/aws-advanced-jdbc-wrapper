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

import java.sql.SQLException;
import java.sql.Struct;
import java.util.Map;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.ConnectionPluginManager;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.util.WrapperUtils;

public class StructWrapper implements Struct {

  protected final Struct struct;
  protected final ConnectionWrapper connectionWrapper;
  protected final ConnectionPluginManager pluginManager;

  public StructWrapper(
      @NonNull Struct struct,
      @NonNull ConnectionWrapper connectionWrapper,
      @NonNull ConnectionPluginManager pluginManager) {
    this.struct = struct;
    this.connectionWrapper = connectionWrapper;
    this.pluginManager = pluginManager;
  }

  @Override
  public String getSQLTypeName() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.STRUCT_GETSQLTYPENAME)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.struct,
          JdbcMethod.STRUCT_GETSQLTYPENAME,
          this.struct::getSQLTypeName);
    } else {
      return this.struct.getSQLTypeName();
    }
  }

  @Override
  public Object[] getAttributes() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.STRUCT_GETATTRIBUTES)) {
      return WrapperUtils.executeWithPlugins(
          Object[].class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.struct,
          JdbcMethod.STRUCT_GETATTRIBUTES,
          this.struct::getAttributes);
    } else {
      return this.struct.getAttributes();
    }
  }

  @Override
  public Object[] getAttributes(Map<String, Class<?>> map) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.STRUCT_GETATTRIBUTES)) {
      return WrapperUtils.executeWithPlugins(
          Object[].class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.struct,
          JdbcMethod.STRUCT_GETATTRIBUTES,
          () -> this.struct.getAttributes(map),
          map);
    } else {
      return this.struct.getAttributes(map);
    }
  }

  @Override
  public String toString() {
    return super.toString() + " - " + this.struct;
  }
}
