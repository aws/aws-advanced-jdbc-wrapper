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

import java.sql.SQLType;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.ConnectionPluginManager;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.util.WrapperUtils;

public class SQLTypeWrapper implements SQLType {

  protected final SQLType sqlType;
  protected final ConnectionWrapper connectionWrapper;
  protected final ConnectionPluginManager pluginManager;

  public SQLTypeWrapper(
      @NonNull SQLType sqlType,
      @NonNull ConnectionWrapper connectionWrapper,
      @NonNull ConnectionPluginManager pluginManager) {
    this.sqlType = sqlType;
    this.connectionWrapper = connectionWrapper;
    this.pluginManager = pluginManager;
  }

  @Override
  public String getName() {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLTYPE_GETNAME)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          this.connectionWrapper,
          this.pluginManager,
          this.sqlType,
          JdbcMethod.SQLTYPE_GETNAME,
          this.sqlType::getName);
    } else {
      return this.sqlType.getName();
    }
  }

  @Override
  public String getVendor() {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLTYPE_GETVENDOR)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          this.connectionWrapper,
          this.pluginManager,
          this.sqlType,
          JdbcMethod.SQLTYPE_GETVENDOR,
          this.sqlType::getVendor);
    } else {
      return this.sqlType.getVendor();
    }
  }

  @Override
  public Integer getVendorTypeNumber() {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLTYPE_GETVENDORTYPENUMBER)) {
      return WrapperUtils.executeWithPlugins(
          Integer.class,
          this.connectionWrapper,
          this.pluginManager,
          this.sqlType,
          JdbcMethod.SQLTYPE_GETVENDORTYPENUMBER,
          this.sqlType::getVendorTypeNumber);
    } else {
      return this.sqlType.getVendorTypeNumber();
    }
  }

  @Override
  public String toString() {
    return super.toString() + " - " + this.sqlType;
  }
}
