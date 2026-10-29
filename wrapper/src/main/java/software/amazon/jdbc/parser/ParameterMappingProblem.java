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

package software.amazon.jdbc.parser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A reason why some parameters of a statement could not be reliably mapped to target columns.
 *
 * <p>Instances are immutable and never contain SQL text or parameter values. Consumers decide
 * whether a problem is relevant, for example only when the table has encrypted columns.
 */
public final class ParameterMappingProblem {

  /** Kind of mapping problem. */
  public enum Type {
    /** Parameters are used inside an expression (e.g. {@code lower(?)}) assigned to a column. */
    PARAMS_IN_EXPRESSION,
    /** Parameters whose target column cannot be determined (e.g. sub-select, arity mismatch). */
    UNKNOWN_TARGET_PARAMS,
    /** INSERT without a column list. */
    NO_COLUMN_LIST,
    /** Numbered parameter placeholders ({@code ?1}, {@code ?2}, ...). */
    FIXED_INDEX_PARAMETER,
    /** The driver's placeholder count differs from the parameters found by the SQL parser. */
    PLACEHOLDER_COUNT_MISMATCH,
    /** A write statement with parameters that the SQL parser could not read. */
    UNPARSED_STATEMENT,
    /** A write statement kind the mapper does not support (MERGE, REPLACE). */
    UNSUPPORTED_STATEMENT
  }

  private final Type type;
  private final @Nullable String table;
  private final List<String> columns;
  private final List<Integer> parameterIndexes;
  private final List<String> candidateTables;
  private final @Nullable String statementKind;
  private final int expectedCount;
  private final int actualCount;

  private ParameterMappingProblem(
      final Type type,
      final @Nullable String table,
      final List<String> columns,
      final List<Integer> parameterIndexes,
      final List<String> candidateTables,
      final @Nullable String statementKind,
      final int expectedCount,
      final int actualCount) {
    this.type = type;
    this.table = table;
    this.columns = Collections.unmodifiableList(new ArrayList<>(columns));
    this.parameterIndexes = Collections.unmodifiableList(new ArrayList<>(parameterIndexes));
    this.candidateTables = Collections.unmodifiableList(new ArrayList<>(candidateTables));
    this.statementKind = statementKind;
    this.expectedCount = expectedCount;
    this.actualCount = actualCount;
  }

  /** Parameters inside an expression assigned to {@code column}. */
  public static ParameterMappingProblem paramsInExpression(
      final @Nullable String table, final String column, final List<Integer> parameterIndexes) {
    return new ParameterMappingProblem(Type.PARAMS_IN_EXPRESSION, table,
        Collections.singletonList(column), parameterIndexes, Collections.emptyList(), null, 0, 0);
  }

  /** Parameters without a resolvable target column; {@code columns} are the possible targets. */
  public static ParameterMappingProblem unknownTargetParams(
      final @Nullable String table, final List<String> columns, final List<Integer> parameterIndexes) {
    return new ParameterMappingProblem(Type.UNKNOWN_TARGET_PARAMS, table,
        columns, parameterIndexes, Collections.emptyList(), null, 0, 0);
  }

  /** INSERT without a column list. */
  public static ParameterMappingProblem noColumnList(
      final @Nullable String table, final List<Integer> parameterIndexes) {
    return new ParameterMappingProblem(Type.NO_COLUMN_LIST, table,
        Collections.emptyList(), parameterIndexes, Collections.emptyList(), null, 0, 0);
  }

  /** Numbered parameter placeholders. */
  public static ParameterMappingProblem fixedIndexParameter(
      final @Nullable String table, final List<Integer> parameterIndexes) {
    return new ParameterMappingProblem(Type.FIXED_INDEX_PARAMETER, table,
        Collections.emptyList(), parameterIndexes, Collections.emptyList(), null, 0, 0);
  }

  /** Placeholder count from the lexical scan ({@code expected}) vs parsed parameters ({@code actual}). */
  public static ParameterMappingProblem placeholderCountMismatch(
      final @Nullable String table,
      final int expected,
      final int actual,
      final List<Integer> parameterIndexes) {
    return new ParameterMappingProblem(Type.PLACEHOLDER_COUNT_MISMATCH, table,
        Collections.emptyList(), parameterIndexes, Collections.emptyList(), null, expected, actual);
  }

  /** Write statement the parser could not read; {@code candidateTables} are its identifiers. */
  public static ParameterMappingProblem unparsedStatement(
      final List<String> candidateTables, final List<Integer> parameterIndexes) {
    return new ParameterMappingProblem(Type.UNPARSED_STATEMENT, null,
        Collections.emptyList(), parameterIndexes, candidateTables, null, 0, 0);
  }

  /** Unsupported write statement kind, e.g. {@code MERGE} or {@code REPLACE}. */
  public static ParameterMappingProblem unsupportedStatement(
      final @Nullable String table, final String statementKind, final List<Integer> parameterIndexes) {
    return new ParameterMappingProblem(Type.UNSUPPORTED_STATEMENT, table,
        Collections.emptyList(), parameterIndexes, Collections.emptyList(), statementKind, 0, 0);
  }

  public Type getType() {
    return type;
  }

  /** Bare target table name, or null when unknown. */
  public @Nullable String getTable() {
    return table;
  }

  /** Bare column names concerned by the problem; may be empty. */
  public List<String> getColumns() {
    return columns;
  }

  /** 1-based parameter indexes concerned by the problem. */
  public List<Integer> getParameterIndexes() {
    return parameterIndexes;
  }

  /** Identifiers of an unparsed statement that may name tables ({@link Type#UNPARSED_STATEMENT}). */
  public List<String> getCandidateTables() {
    return candidateTables;
  }

  /** Statement kind for {@link Type#UNSUPPORTED_STATEMENT}, e.g. {@code MERGE}. */
  public @Nullable String getStatementKind() {
    return statementKind;
  }

  /** Placeholder count of the lexical scan ({@link Type#PLACEHOLDER_COUNT_MISMATCH}). */
  public int getExpectedCount() {
    return expectedCount;
  }

  /** Parameter count found by the SQL parser ({@link Type#PLACEHOLDER_COUNT_MISMATCH}). */
  public int getActualCount() {
    return actualCount;
  }

  @Override
  public String toString() {
    return "ParameterMappingProblem{type=" + type
        + ", table=" + table
        + ", columns=" + columns
        + ", parameterIndexes=" + parameterIndexes
        + ", candidateTables=" + candidateTables.size()
        + ", statementKind=" + statementKind
        + ", expectedCount=" + expectedCount
        + ", actualCount=" + actualCount
        + "}";
  }
}
