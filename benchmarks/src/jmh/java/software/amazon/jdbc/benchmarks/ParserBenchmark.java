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

import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import software.amazon.jdbc.plugin.encryption.parser.JSQLParserAnalyzer;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
public class ParserBenchmark {

    @Benchmark
    public void parseSimpleSelect() {
        JSQLParserAnalyzer.analyze("SELECT * FROM users");
    }

    @Benchmark
    public void parseSelectWithWhere() {
        JSQLParserAnalyzer.analyze("SELECT id, name FROM users WHERE age > 25");
    }

    @Benchmark
    public void parseSelectWithOrderBy() {
        JSQLParserAnalyzer.analyze("SELECT * FROM products ORDER BY price DESC");
    }

    @Benchmark
    public void parseComplexSelect() {
        JSQLParserAnalyzer.analyze("SELECT u.name, o.total FROM users u, orders o WHERE u.id = o.user_id AND o.total > 100");
    }

    @Benchmark
    public void parseInsert() {
        JSQLParserAnalyzer.analyze("INSERT INTO users (name, age, email) VALUES ('John', 30, 'john@example.com')");
    }

    @Benchmark
    public void parseInsertWithPlaceholders() {
        JSQLParserAnalyzer.analyze("INSERT INTO users (name, age, email) VALUES (?, ?, ?)");
    }

    @Benchmark
    public void parseUpdate() {
        JSQLParserAnalyzer.analyze("UPDATE users SET name = 'Jane', age = 25 WHERE id = 1");
    }

    @Benchmark
    public void parseUpdateWithPlaceholders() {
        JSQLParserAnalyzer.analyze("UPDATE users SET name = ?, age = ? WHERE id = ?");
    }

    @Benchmark
    public void parseDelete() {
        JSQLParserAnalyzer.analyze("DELETE FROM users WHERE age < 18");
    }

    @Benchmark
    public void parseCreateTable() {
        JSQLParserAnalyzer.analyze("CREATE TABLE products (id INTEGER PRIMARY KEY, name VARCHAR NOT NULL, price DECIMAL)");
    }

    @Benchmark
    public void parseComplexExpression() {
        JSQLParserAnalyzer.analyze("SELECT * FROM orders WHERE (total > 100 AND status = 'pending') OR (total > 500 AND status = 'shipped')");
    }

    @Benchmark
    public void parseScientificNotation() {
        JSQLParserAnalyzer.analyze("INSERT INTO measurements VALUES (42, 3.14159, 2.5e10)");
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ParserBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }
}
