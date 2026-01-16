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

/** Assignment in UPDATE statement. */
public class Assignment extends AstNode {
  private final Identifier column;
  private final Expression value;

  public Assignment(Identifier column, Expression value) {
    this.column = column;
    this.value = value;
  }

  public Identifier getColumn() {
    return column;
  }

  public Expression getValue() {
    return value;
  }

  @Override
  public <T> T accept(AstVisitor<T> visitor) {
    // Assignment doesn't have a visitor method, so we delegate to the value
    return value.accept(visitor);
  }
}
