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

import java.sql.Ref;
import java.sql.SQLException;
import java.util.Map;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.ConnectionPluginManager;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.util.WrapperUtils;

public class RefWrapper implements Ref {

  protected final Ref ref;
  protected final ConnectionWrapper connectionWrapper;
  protected final ConnectionPluginManager pluginManager;

  public RefWrapper(
      @NonNull Ref ref,
      @NonNull ConnectionWrapper connectionWrapper,
      @NonNull ConnectionPluginManager pluginManager) {
    this.ref = ref;
    this.connectionWrapper = connectionWrapper;
    this.pluginManager = pluginManager;
  }

  @Override
  public String getBaseTypeName() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.REF_GETBASETYPENAME)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.ref,
          JdbcMethod.REF_GETBASETYPENAME,
          this.ref::getBaseTypeName);
    } else {
      return this.ref.getBaseTypeName();
    }
  }

  @Override
  public Object getObject(Map<String, Class<?>> map) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.REF_GETOBJECT)) {
      return WrapperUtils.executeWithPlugins(
          Object.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.ref,
          JdbcMethod.REF_GETOBJECT,
          () -> this.ref.getObject(map),
          map);
    } else {
      return this.ref.getObject(map);
    }
  }

  @Override
  public Object getObject() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.REF_GETOBJECT)) {
      return WrapperUtils.executeWithPlugins(
          Object.class,
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.ref,
          JdbcMethod.REF_GETOBJECT,
          this.ref::getObject);
    } else {
      return this.ref.getObject();
    }
  }

  @Override
  public void setObject(Object value) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.REF_SETOBJECT)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.connectionWrapper,
          this.pluginManager,
          this.ref,
          JdbcMethod.REF_SETOBJECT,
          () -> this.ref.setObject(value),
          value);
    } else {
      this.ref.setObject(value);
    }
  }

  @Override
  public String toString() {
    return super.toString() + " - " + this.ref;
  }
}
