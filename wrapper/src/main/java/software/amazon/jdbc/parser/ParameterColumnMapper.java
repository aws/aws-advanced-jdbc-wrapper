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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.logging.Logger;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.DateTimeLiteralExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.HexValue;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.insert.InsertConflictAction;
import net.sf.jsqlparser.statement.merge.Merge;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.Values;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.update.UpdateSet;
import net.sf.jsqlparser.statement.upsert.Upsert;
import net.sf.jsqlparser.statement.upsert.UpsertType;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;
import net.sf.jsqlparser.util.deparser.StatementDeParser;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.util.Messages;

/**
 * Maps JDBC parameter indexes of a parsed statement to the columns they are assigned to.
 *
 * <p>The mapping uses the parameter positions from the SQL syntax tree ({@link JdbcParameter#getIndex()}),
 * so literals, expressions and comments do not shift it. A parameter is mapped only when it is the
 * whole value of a column (parentheses and casts allowed). Everything the mapper cannot map reliably
 * is reported as a {@link ParameterMappingProblem}; consumers decide whether a problem is relevant.
 */
public final class ParameterColumnMapper {

  private static final Logger LOGGER = Logger.getLogger(ParameterColumnMapper.class.getName());

  private static final Set<String> UNPARSED_WRITE_FIRST_WORDS =
      Collections.unmodifiableSet(new HashSet<>(
          Arrays.asList("INSERT", "UPDATE", "MERGE", "REPLACE", "UPSERT")));

  private static final Set<String> UNPARSED_WITH_WRITE_WORDS =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList("INSERT", "UPDATE", "MERGE")));

  private ParameterColumnMapper() {
    // Utility class
  }

  /**
   * Maps the parameters of a statement to columns.
   *
   * @param statement the parsed statement, or null when the SQL could not be parsed
   * @param scan the lexical scan of the same SQL (placeholder count and words)
   * @return the mapping and the problems found; never throws on unexpected statement shapes
   */
  public static ParameterMapping map(
      final @Nullable Statement statement, final PlaceholderScanner.Result scan) {
    // A string holding several statements is read word by word: the parser either rejects it or
    // reads only the first statement, and a parameter of a write in any of them has to be seen.
    if (statement == null || scan.getStatements().size() > 1) {
      return mapUnparsed(scan);
    }

    final boolean write = isWriteStatement(statement);
    @Nullable String table = null;
    try {
      table = targetTable(statement);
      return new MappingBuilder(statement, table, write, scan).build();
    } catch (RuntimeException e) {
      LOGGER.fine(() -> Messages.get(
          "ParameterColumnMapper.mappingFailed", new Object[] {e.getClass().getName()}));
      if (!write) {
        return new ParameterMapping(
            Collections.emptyMap(), 0, Collections.emptyList(), Collections.emptyList(), false, table);
      }
      // Fail closed: every parameter of a write statement is reported as unmapped.
      return new ParameterMapping(
          Collections.emptyMap(),
          0,
          Collections.singletonList(ParameterMappingProblem.unknownTargetParams(
              table, Collections.emptyList(), range(scan.getPlaceholderCount()))),
          Collections.emptyList(),
          true,
          table);
    }
  }

  /**
   * A statement the parser could not read is judged by its words. Only a write with placeholders is
   * reported, since none of its parameters can be mapped. Without placeholders there is nothing to
   * encrypt, and whatever it writes is left to the database's own check.
   */
  private static ParameterMapping mapUnparsed(final PlaceholderScanner.Result scan) {
    boolean write = false;
    for (List<String> statementWords : scan.getStatements()) {
      if (isUnparsedWrite(statementWords)) {
        write = true;
        break;
      }
    }
    if (write && scan.getPlaceholderCount() > 0) {
      return new ParameterMapping(
          Collections.emptyMap(),
          0,
          Collections.singletonList(ParameterMappingProblem.unparsedStatement(
              new ArrayList<>(new LinkedHashSet<>(scan.getWords())), range(scan.getPlaceholderCount()))),
          Collections.emptyList(),
          true,
          null);
    }
    return new ParameterMapping(
        Collections.emptyMap(), 0, Collections.emptyList(), Collections.emptyList(), write, null);
  }

  private static boolean isUnparsedWrite(final List<String> words) {
    if (words.isEmpty()) {
      return false;
    }
    final String first = words.get(0).toUpperCase(Locale.ROOT);
    if (UNPARSED_WRITE_FIRST_WORDS.contains(first)) {
      return true;
    }
    if ("WITH".equals(first)) {
      for (int i = 1; i < words.size(); i++) {
        if (UNPARSED_WITH_WRITE_WORDS.contains(words.get(i).toUpperCase(Locale.ROOT))) {
          return true;
        }
      }
    }
    return false;
  }

  private static boolean isWriteStatement(final Statement statement) {
    return statement instanceof Insert
        || statement instanceof Update
        || statement instanceof Upsert
        || statement instanceof Merge;
  }

  private static @Nullable String targetTable(final Statement statement) {
    final Table table;
    if (statement instanceof Insert) {
      table = ((Insert) statement).getTable();
    } else if (statement instanceof Update) {
      table = ((Update) statement).getTable();
    } else if (statement instanceof Upsert) {
      table = ((Upsert) statement).getTable();
    } else if (statement instanceof Merge) {
      table = ((Merge) statement).getTable();
    } else {
      return null;
    }
    if (table == null || table.getName() == null) {
      return null;
    }
    return stripQuotes(table.getName());
  }

  /** Removes MySQL backticks and ANSI double quotes from an identifier. */
  static String stripQuotes(final String identifier) {
    return identifier.replace("`", "").replace("\"", "");
  }

  private static List<Integer> range(final int count) {
    final List<Integer> result = new ArrayList<>(count);
    for (int i = 1; i <= count; i++) {
      result.add(i);
    }
    return result;
  }

  /** Collects the parameters inside an expression (including sub-selects). */
  private static List<Integer> parameterIndexes(final Expression expression) {
    final ParameterCollector collector = new ParameterCollector();
    collector.collect(expression);
    return new ArrayList<>(collector.indexes());
  }

  /** Removes parentheses and casts around a value. */
  private static Expression unwrapValue(final Expression value) {
    Expression current = value;
    while (true) {
      final @Nullable Expression inner;
      if (current instanceof Parenthesis) {
        inner = ((Parenthesis) current).getExpression();
      } else if (current instanceof CastExpression) {
        inner = ((CastExpression) current).getLeftExpression();
      } else if (current instanceof ParenthesedExpressionList
          && ((ParenthesedExpressionList<?>) current).size() == 1) {
        inner = ((ParenthesedExpressionList<?>) current).get(0);
      } else {
        return current;
      }
      if (inner == null) {
        return current;
      }
      current = inner;
    }
  }

  /**
   * Whether a value (without parameters) is computed by the client's SQL alone: it references no
   * column and holds no subquery. Such a value is certain to reach the column as plaintext. A value
   * that reads a column or a subquery, and DEFAULT, may be ciphertext the database already holds,
   * and is left to the database's own check.
   */
  static boolean isClientConstant(final Expression value) {
    if (value instanceof NullValue || isBareKeyword(value, "DEFAULT")) {
      return false;
    }
    final ReferenceDetector detector = new ReferenceDetector();
    value.accept(detector.expressionDeParser);
    return !detector.found;
  }

  /** How a client constant is described in messages. */
  static ValueKind kindOf(final Expression value) {
    if (value instanceof SignedExpression) {
      return kindOf(unwrapValue(((SignedExpression) value).getExpression()));
    }
    if (value instanceof StringValue
        || value instanceof LongValue
        || value instanceof DoubleValue
        || value instanceof HexValue
        || value instanceof DateValue
        || value instanceof TimeValue
        || value instanceof TimestampValue
        || value instanceof DateTimeLiteralExpression
        || isBareKeyword(value, "TRUE")
        || isBareKeyword(value, "FALSE")) {
      return ValueKind.LITERAL;
    }
    return ValueKind.EXPRESSION;
  }

  /** JSQLParser reads DEFAULT, TRUE and FALSE as unqualified columns of that name. */
  private static boolean isBareKeyword(final Expression value, final String keyword) {
    if (!(value instanceof Column)) {
      return false;
    }
    final Column column = (Column) value;
    return column.getTable() == null && keyword.equalsIgnoreCase(column.getColumnName());
  }

  private static boolean isComparison(final Expression expression) {
    return expression instanceof EqualsTo
        || expression instanceof NotEqualsTo
        || expression instanceof GreaterThan
        || expression instanceof GreaterThanEquals
        || expression instanceof MinorThan
        || expression instanceof MinorThanEquals;
  }

  /** Walks a statement and builds its mapping. */
  private static final class MappingBuilder {
    private final Statement statement;
    private final @Nullable String table;
    private final boolean write;
    private final PlaceholderScanner.Result scan;
    private final Map<Integer, String> columns = new TreeMap<>();
    private final List<ParameterMappingProblem> problems = new ArrayList<>();
    private final List<UnboundColumn> unboundColumns = new ArrayList<>();

    MappingBuilder(
        final Statement statement,
        final @Nullable String table,
        final boolean write,
        final PlaceholderScanner.Result scan) {
      this.statement = statement;
      this.table = table;
      this.write = write;
      this.scan = scan;
    }

    ParameterMapping build() {
      final ParameterCollector collector = new ParameterCollector();
      collector.collect(statement);
      final TreeSet<Integer> astIndexes = collector.indexes();

      if (statement instanceof Insert) {
        mapInsert((Insert) statement, astIndexes);
      } else if (statement instanceof Update) {
        mapUpdate((Update) statement);
      } else if (statement instanceof Upsert) {
        reportUnsupported(upsertKind((Upsert) statement), astIndexes);
      } else if (statement instanceof Merge) {
        reportUnsupported("MERGE", astIndexes);
      } else if (statement instanceof PlainSelect) {
        mapWhere(((PlainSelect) statement).getWhere());
      }

      if (write) {
        final List<Integer> fixedIndexes = new ArrayList<>(collector.fixedIndexes());
        if (!fixedIndexes.isEmpty()) {
          problems.add(ParameterMappingProblem.fixedIndexParameter(table, fixedIndexes));
        }
        final int expected = scan.getPlaceholderCount();
        if (expected != astIndexes.size() || !isOneToN(astIndexes)) {
          problems.add(ParameterMappingProblem.placeholderCountMismatch(
              table, expected, astIndexes.size(), range(expected)));
        }
      }

      return new ParameterMapping(columns, astIndexes.size(), problems, unboundColumns, write, table);
    }

    private boolean isOneToN(final TreeSet<Integer> indexes) {
      return indexes.isEmpty() || (indexes.first() == 1 && indexes.last() == indexes.size());
    }

    private void reportUnsupported(final String kind, final Set<Integer> astIndexes) {
      if (!astIndexes.isEmpty()) {
        problems.add(ParameterMappingProblem.unsupportedStatement(
            table, kind, new ArrayList<>(astIndexes)));
      } else if (scan.getPlaceholderCount() > 0) {
        problems.add(ParameterMappingProblem.unsupportedStatement(
            table, kind, range(scan.getPlaceholderCount())));
      }
    }

    private String upsertKind(final Upsert upsert) {
      final UpsertType type = upsert.getUpsertType();
      if (type == null || type == UpsertType.REPLACE || type == UpsertType.REPLACE_SET) {
        return "REPLACE";
      }
      return type.name().replace('_', ' ');
    }

    private void mapInsert(final Insert insert, final Set<Integer> astIndexes) {
      final List<String> insertColumns = columnNames(insert.getColumns());
      final List<UpdateSet> setUpdateSets = insert.getSetUpdateSets();
      if (!insertColumns.isEmpty()) {
        final Select source = insert.getSelect();
        if (source != null) {
          mapInsertSource(source, insertColumns);
        }
      } else if (setUpdateSets == null || setUpdateSets.isEmpty()) {
        if (!astIndexes.isEmpty()) {
          problems.add(ParameterMappingProblem.noColumnList(table, new ArrayList<>(astIndexes)));
        }
      }
      mapUpdateSets(setUpdateSets);
      mapUpdateSets(insert.getDuplicateUpdateSets());
      final InsertConflictAction conflictAction = insert.getConflictAction();
      if (conflictAction != null) {
        mapUpdateSets(conflictAction.getUpdateSets());
      }
    }

    private void mapInsertSource(final Select source, final List<String> insertColumns) {
      final Select select = unwrapSelect(source);
      if (select instanceof Values) {
        final ExpressionList<?> expressions = ((Values) select).getExpressions();
        if (expressions != null) {
          for (List<Expression> row : rows(expressions)) {
            pairRow(insertColumns, row);
          }
        }
      } else if (select instanceof PlainSelect) {
        final List<Expression> items = new ArrayList<>();
        for (SelectItem<?> item : ((PlainSelect) select).getSelectItems()) {
          items.add(item.getExpression());
        }
        pairRow(insertColumns, items);
      } else if (select instanceof SetOperationList) {
        for (Select branch : ((SetOperationList) select).getSelects()) {
          mapInsertSource(branch, insertColumns);
        }
      } else {
        final List<Integer> indexes = parameterIndexes(select);
        if (!indexes.isEmpty()) {
          problems.add(ParameterMappingProblem.unknownTargetParams(table, insertColumns, indexes));
        }
      }
    }

    private Select unwrapSelect(final Select source) {
      Select current = source;
      while (current instanceof ParenthesedSelect) {
        final Select inner = ((ParenthesedSelect) current).getSelect();
        if (inner == null) {
          break;
        }
        current = inner;
      }
      return current;
    }

    /**
     * Splits VALUES into rows. A parenthesized list is one row; a plain list holds one row per
     * element, where a non-list element is a single-value row (single-column VALUES).
     */
    private List<List<Expression>> rows(final ExpressionList<?> expressions) {
      final List<List<Expression>> rows = new ArrayList<>();
      if (expressions instanceof ParenthesedExpressionList) {
        rows.add(new ArrayList<Expression>(expressions));
        return rows;
      }
      for (Expression element : expressions) {
        if (element instanceof ExpressionList) {
          rows.add(new ArrayList<Expression>((ExpressionList<?>) element));
        } else if (isRowConstructor(element)) {
          // MySQL's VALUES ROW(a, b), ROW(c, d) is read as a function named ROW.
          rows.add(new ArrayList<Expression>(((Function) element).getParameters()));
        } else {
          rows.add(Collections.singletonList(element));
        }
      }
      return rows;
    }

    private boolean isRowConstructor(final Expression element) {
      if (!(element instanceof Function)) {
        return false;
      }
      final Function function = (Function) element;
      return "ROW".equalsIgnoreCase(function.getName()) && function.getParameters() != null;
    }

    private void pairRow(final List<String> targetColumns, final List<Expression> values) {
      if (values.size() != targetColumns.size()) {
        final List<Integer> indexes = new ArrayList<>();
        for (Expression value : values) {
          indexes.addAll(parameterIndexes(value));
        }
        if (!indexes.isEmpty()) {
          problems.add(ParameterMappingProblem.unknownTargetParams(table, targetColumns, indexes));
        }
        return;
      }
      for (int k = 0; k < values.size(); k++) {
        pair(targetColumns.get(k), values.get(k));
      }
    }

    private void mapUpdateSets(final @Nullable List<UpdateSet> updateSets) {
      if (updateSets == null) {
        return;
      }
      for (UpdateSet updateSet : updateSets) {
        final List<String> setColumns = columnNames(updateSet.getColumns());
        final ExpressionList<?> values = updateSet.getValues();
        if (values != null && values.size() == setColumns.size()) {
          for (int k = 0; k < values.size(); k++) {
            pair(setColumns.get(k), values.get(k));
          }
        } else if (values != null) {
          reportUnpairedSet(table, setColumns, values);
        }
      }
    }

    /**
     * An UPDATE. MySQL can update several tables at once ({@code UPDATE a JOIN b ... SET b.c = ?});
     * each assigned column is then attributed to the table its qualifier names. A column whose
     * table cannot be told is checked against every updated table, and a parameter assigned to it
     * is reported rather than mapped.
     */
    private void mapUpdate(final Update update) {
      final Map<String, String> targets = updateTargets(update);
      if (targets.isEmpty() || update.getUpdateSets() == null) {
        mapUpdateSets(update.getUpdateSets());
        return;
      }
      final Set<String> tables = new LinkedHashSet<>(targets.values());
      for (UpdateSet updateSet : update.getUpdateSets()) {
        final @Nullable ExpressionList<Column> setColumns = updateSet.getColumns();
        final ExpressionList<?> values = updateSet.getValues();
        if (setColumns == null || values == null) {
          continue;
        }
        if (values.size() != setColumns.size()) {
          for (String target : tables) {
            reportUnpairedSet(target, columnNames(setColumns), values);
          }
          continue;
        }
        for (int k = 0; k < values.size(); k++) {
          final Column column = setColumns.get(k);
          final String name = stripQuotes(column.getColumnName());
          final @Nullable String resolved = resolveQualifier(column, targets);
          if (resolved != null) {
            pairTo(resolved, name, values.get(k), true);
          } else {
            for (String target : tables) {
              pairTo(target, name, values.get(k), false);
            }
          }
        }
      }
    }

    /**
     * The tables a multi-table UPDATE writes, keyed by lower-case alias and name. Empty for an
     * UPDATE of a single table (a PostgreSQL {@code FROM} list only supplies values).
     */
    private Map<String, String> updateTargets(final Update update) {
      final Map<String, String> targets = new HashMap<>();
      final List<Join> startJoins = update.getStartJoins();
      if (startJoins == null || startJoins.isEmpty()) {
        return targets;
      }
      addTarget(targets, update.getTable());
      for (Join join : startJoins) {
        if (join.getRightItem() instanceof Table) {
          addTarget(targets, (Table) join.getRightItem());
        }
      }
      return targets;
    }

    private void addTarget(final Map<String, String> targets, final @Nullable Table target) {
      if (target == null || target.getName() == null) {
        return;
      }
      final String name = stripQuotes(target.getName());
      targets.put(name.toLowerCase(Locale.ROOT), name);
      if (target.getAlias() != null && target.getAlias().getName() != null) {
        targets.put(stripQuotes(target.getAlias().getName()).toLowerCase(Locale.ROOT), name);
      }
    }

    private @Nullable String resolveQualifier(final Column column, final Map<String, String> targets) {
      final @Nullable Table qualifier = column.getTable();
      if (qualifier == null || qualifier.getName() == null) {
        return null;
      }
      return targets.get(stripQuotes(qualifier.getName()).toLowerCase(Locale.ROOT));
    }

    /**
     * SET columns whose values could not be paired with them one to one, such as
     * {@code SET (a, b) = (SELECT ...)}. Parameters among those values cannot be mapped.
     */
    private void reportUnpairedSet(
        final @Nullable String target, final List<String> setColumns, final ExpressionList<?> values) {
      final List<Integer> indexes = parameterIndexes(values);
      if (!indexes.isEmpty()) {
        problems.add(ParameterMappingProblem.unknownTargetParams(target, setColumns, indexes));
      }
    }

    /**
     * Pairs a column with the value written to it. A value without parameters is recorded as an
     * unbound column when it is a client constant (see {@link #isClientConstant}).
     */
    private void pair(final String column, final Expression value) {
      pairTo(table, column, value, true);
    }

    /**
     * Pairs a column of {@code target} with its value. When {@code mapParameter} is false the
     * column's table is uncertain, so a parameter assigned to it is reported instead of mapped.
     */
    private void pairTo(
        final @Nullable String target,
        final String column,
        final Expression value,
        final boolean mapParameter) {
      final Expression unwrapped = unwrapValue(value);
      if (unwrapped instanceof JdbcParameter) {
        final Integer index = ((JdbcParameter) unwrapped).getIndex();
        if (index != null && mapParameter) {
          columns.put(index, target == null ? column : target + "." + column);
        } else if (index != null) {
          problems.add(ParameterMappingProblem.unknownTargetParams(
              target, Collections.singletonList(column), Collections.singletonList(index)));
        }
        return;
      }
      final List<Integer> indexes = parameterIndexes(unwrapped);
      if (!indexes.isEmpty()) {
        problems.add(ParameterMappingProblem.paramsInExpression(target, column, indexes));
      } else if (isClientConstant(unwrapped)) {
        unboundColumns.add(new UnboundColumn(target, column, kindOf(unwrapped)));
      }
    }

    /** SELECT: maps only parameters compared directly with a column. */
    private void mapWhere(final @Nullable Expression where) {
      if (where == null) {
        return;
      }
      Expression expression = where;
      while (expression instanceof Parenthesis && ((Parenthesis) expression).getExpression() != null) {
        expression = ((Parenthesis) expression).getExpression();
      }
      if (expression instanceof AndExpression || expression instanceof OrExpression) {
        final BinaryExpression binary = (BinaryExpression) expression;
        mapWhere(binary.getLeftExpression());
        mapWhere(binary.getRightExpression());
      } else if (isComparison(expression)) {
        final BinaryExpression comparison = (BinaryExpression) expression;
        final Expression left = unwrapValue(comparison.getLeftExpression());
        final Expression right = unwrapValue(comparison.getRightExpression());
        if (left instanceof JdbcParameter && right instanceof Column) {
          putBare((JdbcParameter) left, (Column) right);
        } else if (right instanceof JdbcParameter && left instanceof Column) {
          putBare((JdbcParameter) right, (Column) left);
        }
      }
    }

    private void putBare(final JdbcParameter parameter, final Column column) {
      final Integer index = parameter.getIndex();
      if (index != null && column.getColumnName() != null) {
        columns.put(index, stripQuotes(column.getColumnName()));
      }
    }

    private List<String> columnNames(final @Nullable ExpressionList<Column> columnList) {
      final List<String> names = new ArrayList<>();
      if (columnList == null) {
        return names;
      }
      for (Column column : columnList) {
        names.add(stripQuotes(column.getColumnName()));
      }
      return names;
    }
  }

  /**
   * Collects {@link JdbcParameter}s with the JSQLParser de-parsers, which visit every part of a
   * statement. {@code ON CONFLICT ... DO UPDATE} is not visited by the de-parser and is added
   * explicitly.
   */
  private static final class ParameterCollector {
    private final List<JdbcParameter> parameters = new ArrayList<>();
    private final StringBuilder buffer = new StringBuilder();
    private final ExpressionDeParser expressionDeParser;
    private final StatementDeParser statementDeParser;

    ParameterCollector() {
      this.expressionDeParser = new ExpressionDeParser() {
        @Override
        public void visit(final JdbcParameter jdbcParameter) {
          parameters.add(jdbcParameter);
          super.visit(jdbcParameter);
        }
      };
      final SelectDeParser selectDeParser = new SelectDeParser(this.expressionDeParser, this.buffer);
      this.expressionDeParser.setSelectVisitor(selectDeParser);
      this.expressionDeParser.setBuffer(this.buffer);
      this.statementDeParser =
          new StatementDeParser(this.expressionDeParser, selectDeParser, this.buffer);
    }

    void collect(final Statement statement) {
      statement.accept(statementDeParser);
      if (statement instanceof Insert) {
        final InsertConflictAction conflictAction = ((Insert) statement).getConflictAction();
        if (conflictAction != null) {
          if (conflictAction.getUpdateSets() != null) {
            for (UpdateSet updateSet : conflictAction.getUpdateSets()) {
              if (updateSet.getValues() != null) {
                collect(updateSet.getValues());
              }
            }
          }
          if (conflictAction.getWhereExpression() != null) {
            collect(conflictAction.getWhereExpression());
          }
        }
      }
    }

    void collect(final Expression expression) {
      expression.accept(expressionDeParser);
    }

    TreeSet<Integer> indexes() {
      final TreeSet<Integer> indexes = new TreeSet<>();
      for (JdbcParameter parameter : parameters) {
        if (parameter.getIndex() != null) {
          indexes.add(parameter.getIndex());
        }
      }
      return indexes;
    }

    TreeSet<Integer> fixedIndexes() {
      final TreeSet<Integer> indexes = new TreeSet<>();
      for (JdbcParameter parameter : parameters) {
        if (parameter.isUseFixedIndex() && parameter.getIndex() != null) {
          indexes.add(parameter.getIndex());
        }
      }
      return indexes;
    }
  }

  /** Result of {@link #map}. Immutable. */
  public static final class ParameterMapping {
    private final Map<Integer, String> columns;
    private final int astParameterCount;
    private final List<ParameterMappingProblem> problems;
    private final List<UnboundColumn> unboundColumns;
    private final boolean writeStatement;
    private final @Nullable String targetTable;

    ParameterMapping(
        final Map<Integer, String> columns,
        final int astParameterCount,
        final List<ParameterMappingProblem> problems,
        final List<UnboundColumn> unboundColumns,
        final boolean writeStatement,
        final @Nullable String targetTable) {
      this.columns = Collections.unmodifiableMap(new TreeMap<>(columns));
      this.astParameterCount = astParameterCount;
      this.problems = Collections.unmodifiableList(new ArrayList<>(problems));
      this.unboundColumns = Collections.unmodifiableList(new ArrayList<>(unboundColumns));
      this.writeStatement = writeStatement;
      this.targetTable = targetTable;
    }

    /**
     * Parameter index (1-based) to column: {@code table.column} for INSERT/UPDATE (bare table name),
     * bare column name for SELECT.
     *
     * @return an immutable map of parameter indexes to columns, in ascending index order
     */
    public Map<Integer, String> getColumns() {
      return columns;
    }

    /**
     * Parameter index (1-based) to bare column name.
     *
     * @return a map of parameter indexes to bare column names
     */
    public Map<Integer, String> getBareColumns() {
      final Map<Integer, String> bare = new HashMap<>();
      for (Map.Entry<Integer, String> entry : columns.entrySet()) {
        final String value = entry.getValue();
        bare.put(entry.getKey(), value.substring(value.lastIndexOf('.') + 1));
      }
      return bare;
    }

    /**
     * Number of distinct parameters found in the syntax tree.
     *
     * @return the number of distinct parameters
     */
    public int getAstParameterCount() {
      return astParameterCount;
    }

    /**
     * Problems found while mapping; empty when every relevant parameter was mapped.
     *
     * @return an immutable list of mapping problems
     */
    public List<ParameterMappingProblem> getProblems() {
      return problems;
    }

    /**
     * Columns the statement writes with a client constant: a value with no bind parameter, column
     * reference or subquery in it, such as a literal or {@code concat('a', 'b')}. Such a value is
     * certain to be stored as plaintext. NULL and DEFAULT are not listed, and neither are values
     * read from a column or a subquery, which may be ciphertext the database already holds.
     *
     * @return an immutable list of unbound columns, in statement order
     */
    public List<UnboundColumn> getUnboundColumns() {
      return unboundColumns;
    }

    /**
     * True for INSERT, UPDATE, MERGE, REPLACE / UPSERT (also when detected without parsing).
     *
     * @return {@code true} if the SQL is a write statement; {@code false} otherwise
     */
    public boolean isWriteStatement() {
      return writeStatement;
    }

    /**
     * Bare target table of a write statement, or null.
     *
     * @return the bare target table, or {@code null} if it is unknown or not applicable
     */
    public @Nullable String getTargetTable() {
      return targetTable;
    }
  }

  /** The kind of value an {@link UnboundColumn} is written with. */
  public enum ValueKind {
    /** A literal, such as {@code '123'}, {@code 42} or {@code TRUE}. */
    LITERAL,
    /** An expression computed without any column or subquery, such as {@code concat('a', 'b')}. */
    EXPRESSION
  }

  /**
   * Finds a column reference or a subquery anywhere in an expression, using the JSQLParser
   * de-parsers, which visit every part of it.
   */
  private static final class ReferenceDetector {
    private boolean found;
    private final StringBuilder buffer = new StringBuilder();
    private final ExpressionDeParser expressionDeParser;

    ReferenceDetector() {
      this.expressionDeParser = new ExpressionDeParser() {
        @Override
        public void visit(final Column column) {
          if (!isBareKeyword(column, "TRUE") && !isBareKeyword(column, "FALSE")) {
            found = true;
          }
          super.visit(column);
        }
      };
      final SelectDeParser selectDeParser = new SelectDeParser(this.expressionDeParser, this.buffer) {
        @Override
        public void visit(final PlainSelect plainSelect) {
          found = true;
          super.visit(plainSelect);
        }
      };
      this.expressionDeParser.setSelectVisitor(selectDeParser);
      this.expressionDeParser.setBuffer(this.buffer);
    }
  }

  /**
   * A column a statement writes with a value that is not a bind parameter. Carries no SQL text,
   * only the table, the column and the kind of value. Immutable.
   */
  public static final class UnboundColumn {
    private final @Nullable String table;
    private final String column;
    private final ValueKind kind;

    UnboundColumn(final @Nullable String table, final String column, final ValueKind kind) {
      this.table = table;
      this.column = column;
      this.kind = kind;
    }

    /**
     * Bare target table of the statement.
     *
     * @return the table, or {@code null} when it is unknown
     */
    public @Nullable String getTable() {
      return table;
    }

    /**
     * Bare column name.
     *
     * @return the column name
     */
    public String getColumn() {
      return column;
    }

    /**
     * The kind of value written to the column.
     *
     * @return the value kind
     */
    public ValueKind getKind() {
      return kind;
    }

    @Override
    public String toString() {
      return "UnboundColumn{table=" + table + ", column=" + column + ", kind=" + kind + '}';
    }
  }
}
