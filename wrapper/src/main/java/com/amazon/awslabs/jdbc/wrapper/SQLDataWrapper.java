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

package com.amazon.awslabs.jdbc.wrapper;

import com.amazon.awslabs.jdbc.ConnectionPluginManager;
import com.amazon.awslabs.jdbc.util.WrapperUtils;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLOutput;
import org.checkerframework.checker.nullness.qual.NonNull;

public class SQLDataWrapper implements SQLData {

  protected SQLData sqlData;
  protected ConnectionPluginManager pluginManager;

  public SQLDataWrapper(@NonNull SQLData sqlData, @NonNull ConnectionPluginManager pluginManager) {
    this.sqlData = sqlData;
    this.pluginManager = pluginManager;
  }

  @Override
  public String getSQLTypeName() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        String.class,
        SQLException.class,
        this.pluginManager,
        this.sqlData,
        "SQLData.getSQLTypeName",
        () -> this.sqlData.getSQLTypeName());
  }

  @Override
  public void readSQL(SQLInput stream, String typeName) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.sqlData,
        "SQLData.readSQL",
        () -> this.sqlData.readSQL(stream, typeName),
        stream,
        typeName);
  }

  @Override
  public void writeSQL(SQLOutput stream) throws SQLException {
    WrapperUtils.runWithPlugins(
        SQLException.class,
        this.pluginManager,
        this.sqlData,
        "SQLData.writeSQL",
        () -> this.sqlData.writeSQL(stream),
        stream);
  }
}
