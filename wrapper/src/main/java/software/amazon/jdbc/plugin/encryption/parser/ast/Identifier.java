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

/** Identifier (table name, column name, etc.) */
public class Identifier extends Expression {
  private final String name;
  private final String schema;

  public Identifier(String name) {
    this(null, name);
  }

  public Identifier(String schema, String name) {
    this.schema = schema;
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public String getSchema() {
    return schema;
  }

  public String getFullName() {
    return schema != null ? schema + "." + name : name;
  }

  @Override
  public <T> T accept(AstVisitor<T> visitor) {
    return visitor.visit(this);
  }
}
