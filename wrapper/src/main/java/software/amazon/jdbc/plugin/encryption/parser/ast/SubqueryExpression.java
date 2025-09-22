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

/** Represents a subquery expression in SQL. */
public class SubqueryExpression extends Expression {
  private final SelectStatement selectStatement;

  public SubqueryExpression(SelectStatement selectStatement) {
    this.selectStatement = selectStatement;
  }

  public SelectStatement getSelectStatement() {
    return selectStatement;
  }

  @Override
  public <T> T accept(AstVisitor<T> visitor) {
    return visitor.visit(this);
  }

  @Override
  public String toString() {
    return "(" + selectStatement.toString() + ")";
  }
}
