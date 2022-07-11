/*
*    Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
* 
*    Licensed under the Apache License, Version 2.0 (the "License").
*    You may not use this file except in compliance with the License.
*    You may obtain a copy of the License at
* 
*    http://www.apache.org/licenses/LICENSE-2.0
* 
*    Unless required by applicable law or agreed to in writing, software
*    distributed under the License is distributed on an "AS IS" BASIS,
*    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*    See the License for the specific language governing permissions and
*    limitations under the License.
*/

package software.aws.rds.jdbc.proxydriver.wrapper;

import java.sql.SQLType;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.aws.rds.jdbc.proxydriver.ConnectionPluginManager;
import software.aws.rds.jdbc.proxydriver.util.WrapperUtils;

public class SQLTypeWrapper implements SQLType {

  protected SQLType sqlType;
  protected ConnectionPluginManager pluginManager;

  public SQLTypeWrapper(@NonNull SQLType sqlType, @NonNull ConnectionPluginManager pluginManager) {
    this.sqlType = sqlType;
    this.pluginManager = pluginManager;
  }

  @Override
  public String getName() {
    return WrapperUtils.executeWithPlugins(
        String.class,
        this.pluginManager,
        this.sqlType,
        "SQLType.getName",
        () -> this.sqlType.getName());
  }

  @Override
  public String getVendor() {
    return WrapperUtils.executeWithPlugins(
        String.class,
        this.pluginManager,
        this.sqlType,
        "SQLType.getVendor",
        () -> this.sqlType.getVendor());
  }

  @Override
  public Integer getVendorTypeNumber() {
    return WrapperUtils.executeWithPlugins(
        Integer.class,
        this.pluginManager,
        this.sqlType,
        "SQLType.getVendorTypeNumber",
        () -> this.sqlType.getVendorTypeNumber());
  }
}
