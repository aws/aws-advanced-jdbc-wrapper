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

/** SELECT statement. */
public class SelectStatement extends Statement {
  private final List<SelectItem> selectList;
  private final List<TableReference> fromList;
  private final Expression whereClause;
  private final List<Expression> groupByList;
  private final Expression havingClause;
  private final List<OrderByItem> orderByList;
  private final Integer limit;

  public SelectStatement(
      List<SelectItem> selectList,
      List<TableReference> fromList,
      Expression whereClause,
      List<Expression> groupByList,
      Expression havingClause,
      List<OrderByItem> orderByList,
      Integer limit) {
    this.selectList = selectList;
    this.fromList = fromList;
    this.whereClause = whereClause;
    this.groupByList = groupByList;
    this.havingClause = havingClause;
    this.orderByList = orderByList;
    this.limit = limit;
  }

  public List<SelectItem> getSelectList() {
    return selectList;
  }

  public List<TableReference> getFromList() {
    return fromList;
  }

  public List<TableReference> getFromClause() {
    return fromList;
  } // convenience method

  public Expression getWhereClause() {
    return whereClause;
  }

  public List<Expression> getGroupByList() {
    return groupByList;
  }

  public Expression getHavingClause() {
    return havingClause;
  }

  public List<OrderByItem> getOrderByList() {
    return orderByList;
  }

  public List<OrderByItem> getOrderBy() {
    return orderByList;
  } // convenience method

  public Integer getLimit() {
    return limit;
  }

  @Override
  public <T> T accept(AstVisitor<T> visitor) {
    return visitor.visit(this);
  }
}
