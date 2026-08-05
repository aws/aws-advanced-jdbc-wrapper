# Benchmarks for AWS Advanced JDBC Wrapper

This directory contains a set of [JMH](https://github.com/openjdk/jmh) benchmarks for the AWS Advanced JDBC Wrapper.
These benchmarks measure the overhead the wrapper adds to JDBC method calls with multiple connection plugins enabled.
They do not measure the performance of target JDBC drivers nor the performance of the failover process.

## Benchmark classes

| Class                               | What it measures                                                                              | Needs a database? |
|-------------------------------------|-----------------------------------------------------------------------------------------------|-------------------|
| `ConnectionPluginManagerBenchmarks` | Plugin-chain construction and the `connect` / `execute` / `notifyConnectionChanged` pipelines | No (mocked)       |
| `PluginBenchmarks`                  | `ConnectionWrapper` lifecycle and statement execution through the wrapper                     | No (mocked)       |
| `PgCacheBenchmarks`                 | The remote query cache plugin against a real PostgreSQL instance and cache server             | Yes               |

## Usage

1. Build the benchmark JAR:

   ```shell
   ../gradlew :benchmarks:jmhJar
   ```

   The shaded JAR is written to `build/libs/benchmarks-<version>-jmh.jar`.

2. Run the benchmarks that do not require infrastructure:

   ```shell
   java -jar build/libs/benchmarks-<version>-jmh.jar -e PgCacheBenchmarks
   ```

   `-e PgCacheBenchmarks` excludes the benchmark that needs a live database and cache server. Without
   it, that class fails during setup and its results are reported as `<failure>`.

Useful JMH flags while iterating: `-f 1 -wi 1 -i 1` for a fast smoke run, `-rf json -rff result.json`
to capture machine-readable results, and `-prof gc` to add allocation numbers.

### Running `PgCacheBenchmarks`

This class needs a reachable PostgreSQL instance holding the pre-populated `test` table (see the
class javadoc for the schema) plus a reachable cache server. Endpoints are supplied as system
properties, so nothing environment-specific is hardcoded:

```shell
java -Dbenchmarks.pg.url=jdbc:aws-wrapper:postgresql://my-db:5432/postgres \
     -Dbenchmarks.cache.rw=my-cache:6379 \
     -Dbenchmarks.cache.ro=my-cache:6380 \
     -jar build/libs/benchmarks-<version>-jmh.jar PgCacheBenchmarks
```

If a property is missing, setup fails immediately with a message naming it rather than timing out
against a placeholder host name.

## Notes

- The mocked benchmarks measure the wrapper's own overhead. `PluginBenchmarks` drives a
  `ConnectionWrapper` backed by a mocked `ConnectionPluginManager`, so the `wrapperPlugins` values in
  its property helpers select *which* JDBC pipeline shape is exercised, not a real plugin chain.
  `ConnectionPluginManagerBenchmarks` is the class that builds real plugin chains (10 instances of a
  no-op `BenchmarkPlugin`).
- `../gradlew test` does not compile this module: the benchmarks live in the `jmh` source set, so
  `:benchmarks:jmhJar` (or `:benchmarks:compileJmhJava`) is what catches drift against the wrapper's
  internal APIs.
