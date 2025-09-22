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

import java.util.List;

/** UPDATE statement. */
public class UpdateStatement extends Statement {
  private final TableReference table;
  private final List<Assignment> assignments;
  private final Expression whereClause;

  public UpdateStatement(
      TableReference table, List<Assignment> assignments, Expression whereClause) {
    this.table = table;
    this.assignments = assignments;
    this.whereClause = whereClause;
  }

  public TableReference getTable() {
    return table;
  }

  public List<Assignment> getAssignments() {
    return assignments;
  }

  public Expression getWhereClause() {
    return whereClause;
  }

  @Override
  public <T> T accept(AstVisitor<T> visitor) {
    return visitor.visit(this);
  }
}
