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

package software.amazon.jdbc.plugin.encryption.parser.ast;

/** Column definition in CREATE TABLE. */
public class ColumnDefinition extends AstNode {
  private final Identifier columnName;
  private final String dataType;
  private final boolean notNull;
  private final boolean primaryKey;

  public ColumnDefinition(
      Identifier columnName, String dataType, boolean notNull, boolean primaryKey) {
    this.columnName = columnName;
    this.dataType = dataType;
    this.notNull = notNull;
    this.primaryKey = primaryKey;
  }

  public Identifier getColumnName() {
    return columnName;
  }

  public String getDataType() {
    return dataType;
  }

  public boolean isNotNull() {
    return notNull;
  }

  public boolean isPrimaryKey() {
    return primaryKey;
  }

  @Override
  public <T> T accept(AstVisitor<T> visitor) {
    // ColumnDefinition doesn't have a visitor method, so we delegate to the column name
    return columnName.accept(visitor);
  }
}
