/*
 *
 *     Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License").
 *     You may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.amazon.awslabs.jdbc.wrapper;

import com.amazon.awslabs.jdbc.ConnectionPluginManager;
import com.amazon.awslabs.jdbc.util.WrapperUtils;
import java.sql.SQLException;
import java.sql.Struct;
import java.util.Map;
import org.checkerframework.checker.nullness.qual.NonNull;

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
