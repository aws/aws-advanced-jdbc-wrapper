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

package software.aws.jdbc.wrapper;

import java.sql.Ref;
import java.sql.SQLException;
import java.util.Map;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.aws.jdbc.ConnectionPluginManager;
import software.aws.jdbc.util.WrapperUtils;

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
