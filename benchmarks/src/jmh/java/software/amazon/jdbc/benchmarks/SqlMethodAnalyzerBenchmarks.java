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

package software.amazon.jdbc.benchmarks;

import java.sql.Connection;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.benchmarks.support.FakeJdbc;
import software.amazon.jdbc.util.SqlMethodAnalyzer;

/**
 * Micro-benchmarks for {@link SqlMethodAnalyzer}.
 *
 * <p>This class sits on the hottest path in the driver: {@code DefaultConnectionPlugin.execute}
 * consults it two to four times for every single JDBC call that carries SQL, and the SQL-inspecting
 * paths are not cheap - each one strips comments character by character, collapses whitespace with a
 * regex, splits on {@code ;}, then upper-cases the result. Nothing here was measured before.
 *
 * <p>The benchmarks are split by the shape of the input rather than by method, because the cost is
 * driven almost entirely by whether the analyzer takes an early return or falls through to parsing:
 *
 * <ul>
 *   <li>{@code *NonSqlMethod} - the common case for non-SQL JDBC calls, expected to short-circuit.
 *   <li>{@code *SimpleSelect} - a typical single statement, the full parse path.
 *   <li>{@code *WithComments} - the same statement carrying line and block comments, which exercises
 *       the character-by-character comment stripper.
 *   <li>{@code *MultiStatement} - a batch, which additionally exercises the split.
 * </ul>
 *
 * <p>Results are directly comparable across those four inputs, which is what makes them useful:
 * a large gap between {@code SimpleSelect} and {@code NonSqlMethod} is the per-call tax paid by
 * every statement execution.
 */
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class SqlMethodAnalyzerBenchmarks {

  private static final String SIMPLE_SELECT = "SELECT id, name FROM users WHERE id = 42";
  private static final String COMMENTED_SELECT =
      "-- pick a user\n/* by primary key */ SELECT id, name FROM users WHERE id = 42 -- trailing";
  private static final String MULTI_STATEMENT =
      "BEGIN; UPDATE users SET name = 'a' WHERE id = 1; UPDATE users SET name = 'b' WHERE id = 2; COMMIT";
  private static final String SET_AUTOCOMMIT = "SET AUTOCOMMIT = 1";

  private static final String EXECUTE_METHOD = JdbcMethod.STATEMENT_EXECUTE.methodName;
  private static final String NON_SQL_METHOD = JdbcMethod.CONNECTION_GETMETADATA.methodName;
  private static final String SET_AUTOCOMMIT_METHOD = JdbcMethod.CONNECTION_SETAUTOCOMMIT.methodName;

  private final SqlMethodAnalyzer analyzer = new SqlMethodAnalyzer();

  private Connection autoCommitOn;
  private Connection autoCommitOff;

  private Object[] simpleSelectArgs;
  private Object[] commentedSelectArgs;
  private Object[] multiStatementArgs;
  private Object[] setAutoCommitArgs;
  private Object[] setAutoCommitTrueArgs;

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(SqlMethodAnalyzerBenchmarks.class.getSimpleName())
        .addProfiler(GCProfiler.class)
        .detectJvmArgs()
        .build();

    new Runner(opt).run();
  }

  @Setup(Level.Trial)
  public void setUp() {
    this.autoCommitOn = FakeJdbc.connection(true, 0);
    this.autoCommitOff = FakeJdbc.connection(false, 0);
    this.simpleSelectArgs = new Object[] {SIMPLE_SELECT};
    this.commentedSelectArgs = new Object[] {COMMENTED_SELECT};
    this.multiStatementArgs = new Object[] {MULTI_STATEMENT};
    this.setAutoCommitArgs = new Object[] {SET_AUTOCOMMIT};
    this.setAutoCommitTrueArgs = new Object[] {Boolean.TRUE};
  }

  @Benchmark
  public boolean doesOpenTransactionNonSqlMethod() {
    return analyzer.doesOpenTransaction(autoCommitOn, NON_SQL_METHOD, null);
  }

  @Benchmark
  public boolean doesOpenTransactionSimpleSelect() {
    return analyzer.doesOpenTransaction(autoCommitOff, EXECUTE_METHOD, simpleSelectArgs);
  }

  @Benchmark
  public boolean doesOpenTransactionWithComments() {
    return analyzer.doesOpenTransaction(autoCommitOff, EXECUTE_METHOD, commentedSelectArgs);
  }

  @Benchmark
  public boolean doesOpenTransactionMultiStatement() {
    return analyzer.doesOpenTransaction(autoCommitOff, EXECUTE_METHOD, multiStatementArgs);
  }

  @Benchmark
  public boolean doesCloseTransactionNonSqlMethod() {
    return analyzer.doesCloseTransaction(autoCommitOn, NON_SQL_METHOD, null);
  }

  @Benchmark
  public boolean doesCloseTransactionSimpleSelect() {
    return analyzer.doesCloseTransaction(autoCommitOff, EXECUTE_METHOD, simpleSelectArgs);
  }

  @Benchmark
  public boolean doesCloseTransactionMultiStatement() {
    return analyzer.doesCloseTransaction(autoCommitOff, EXECUTE_METHOD, multiStatementArgs);
  }

  /**
   * The path taken by every SQL execution regardless of content: an early {@code false} when the
   * statement is not an autocommit change.
   */
  @Benchmark
  public boolean doesSwitchAutoCommitFalseTrueSimpleSelect() {
    return analyzer.doesSwitchAutoCommitFalseTrue(autoCommitOff, EXECUTE_METHOD, simpleSelectArgs);
  }

  @Benchmark
  public boolean doesSwitchAutoCommitFalseTrueViaSql() {
    return analyzer.doesSwitchAutoCommitFalseTrue(autoCommitOff, EXECUTE_METHOD, setAutoCommitArgs);
  }

  @Benchmark
  public boolean doesSwitchAutoCommitFalseTrueViaSetter() {
    return analyzer.doesSwitchAutoCommitFalseTrue(
        autoCommitOff, SET_AUTOCOMMIT_METHOD, setAutoCommitTrueArgs);
  }

  @Benchmark
  public boolean isStatementSettingAutoCommit() {
    return analyzer.isStatementSettingAutoCommit(EXECUTE_METHOD, setAutoCommitArgs);
  }

  @Benchmark
  public Boolean getAutoCommitValueFromSqlStatement() {
    return analyzer.getAutoCommitValueFromSqlStatement(setAutoCommitArgs);
  }

  /** Pure set lookup, included as the floor for the other measurements. */
  @Benchmark
  public boolean isMethodClosingSqlObject() {
    return analyzer.isMethodClosingSqlObject(EXECUTE_METHOD);
  }
}
