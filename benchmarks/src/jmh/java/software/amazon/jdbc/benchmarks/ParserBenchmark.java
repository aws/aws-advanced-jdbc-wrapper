package software.amazon.jdbc.benchmarks;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import software.amazon.jdbc.plugin.encryption.parser.PostgreSqlParser;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
public class ParserBenchmark {

    private PostgreSqlParser parser;

    @Setup
    public void setup() {
        parser = new PostgreSqlParser();
    }

    @Benchmark
    public void parseSimpleSelect() {
        parser.parse("SELECT * FROM users");
    }

    @Benchmark
    public void parseSelectWithWhere() {
        parser.parse("SELECT id, name FROM users WHERE age > 25");
    }

    @Benchmark
    public void parseSelectWithOrderBy() {
        parser.parse("SELECT * FROM products ORDER BY price DESC");
    }

    @Benchmark
    public void parseComplexSelect() {
        parser.parse("SELECT u.name, o.total FROM users u, orders o WHERE u.id = o.user_id AND o.total > 100");
    }

    @Benchmark
    public void parseInsert() {
        parser.parse("INSERT INTO users (name, age, email) VALUES ('John', 30, 'john@example.com')");
    }

    @Benchmark
    public void parseInsertWithPlaceholders() {
        parser.parse("INSERT INTO users (name, age, email) VALUES (?, ?, ?)");
    }

    @Benchmark
    public void parseUpdate() {
        parser.parse("UPDATE users SET name = 'Jane', age = 25 WHERE id = 1");
    }

    @Benchmark
    public void parseUpdateWithPlaceholders() {
        parser.parse("UPDATE users SET name = ?, age = ? WHERE id = ?");
    }

    @Benchmark
    public void parseDelete() {
        parser.parse("DELETE FROM users WHERE age < 18");
    }

    @Benchmark
    public void parseCreateTable() {
        parser.parse("CREATE TABLE products (id INTEGER PRIMARY KEY, name VARCHAR NOT NULL, price DECIMAL)");
    }

    @Benchmark
    public void parseComplexExpression() {
        parser.parse("SELECT * FROM orders WHERE (total > 100 AND status = 'pending') OR (total > 500 AND status = 'shipped')");
    }

    @Benchmark
    public void parseScientificNotation() {
        parser.parse("INSERT INTO measurements VALUES (42, 3.14159, 2.5e10)");
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ParserBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }
}
