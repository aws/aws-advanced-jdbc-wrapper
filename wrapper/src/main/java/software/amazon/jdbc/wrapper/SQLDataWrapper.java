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

import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLOutput;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.ConnectionPluginManager;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.util.WrapperUtils;

public class SQLDataWrapper implements SQLData {

  protected SQLData sqlData;
  protected ConnectionPluginManager pluginManager;

  public SQLDataWrapper(@NonNull SQLData sqlData, @NonNull ConnectionPluginManager pluginManager) {
    this.sqlData = sqlData;
    this.pluginManager = pluginManager;
  }

  @Override
  public String getSQLTypeName() throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLDATA_GETSQLTYPENAME)) {
      return WrapperUtils.executeWithPlugins(
          String.class,
          SQLException.class,
          this.pluginManager,
          this.sqlData,
          JdbcMethod.SQLDATA_GETSQLTYPENAME,
          () -> this.sqlData.getSQLTypeName());
    } else {
      return this.sqlData.getSQLTypeName();
    }
  }

  @Override
  public void readSQL(SQLInput stream, String typeName) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLDATA_READSQL)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.sqlData,
          JdbcMethod.SQLDATA_READSQL,
          () -> this.sqlData.readSQL(stream, typeName),
          stream,
          typeName);
    } else {
      this.sqlData.readSQL(stream, typeName);
    }
  }

  @Override
  public void writeSQL(SQLOutput stream) throws SQLException {
    if (this.pluginManager.mustUsePipeline(JdbcMethod.SQLDATA_WRITESQL)) {
      WrapperUtils.runWithPlugins(
          SQLException.class,
          this.pluginManager,
          this.sqlData,
          JdbcMethod.SQLDATA_WRITESQL,
          () -> this.sqlData.writeSQL(stream),
          stream);
    } else {
      this.sqlData.writeSQL(stream);
    }
  }

  @Override
  public String toString() {
    return super.toString() + " - " + this.sqlData;
  }
}
