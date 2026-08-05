# Benchmarks for AWS Advanced JDBC Wrapper

This directory contains a set of [JMH](https://github.com/openjdk/jmh) benchmarks for the AWS Advanced JDBC Wrapper.
They measure the overhead the wrapper adds on top of the target JDBC driver, and the per-call cost of
the driver's own components. They do not measure the performance of target JDBC drivers themselves,
nor the failover process.

## Benchmark classes

| Class | What it measures | Needs a database? |
|---|---|---|
| `WrapperOverheadBenchmarks` | **Wrapper cost versus the target driver.** Paired `raw*`/`wrapped*` benchmarks per statement, per parameter and per row | No |
| `RealPluginChainBenchmarks` | Per-call cost of real plugins through `ConnectionPluginManager.execute` | No |
| `PluginServiceBenchmarks` | The real `PluginServiceImpl` and `SessionStateServiceImpl` | No |
| `SqlMethodAnalyzerBenchmarks` | SQL inspection run on every statement execution | No |
| `RdsUtilsBenchmarks` | RDS endpoint classification, cached and uncached | No |
| `ConnectionUrlParserBenchmarks` | Per-connection URL and property parsing | No |
| `StorageBenchmarks` | `CacheMap`, `ExpirationCache`, `SlidingExpirationCache`, `StorageServiceImpl`, incl. contended | No |
| `HostSelectorBenchmarks` | All seven `HostSelector` implementations, incl. contended | No |
| `ConnectionPluginManagerBenchmarks` | Plugin-chain construction and the connect/execute pipelines with 10 no-op plugins | No |
| `PluginBenchmarks` | `ConnectionWrapper` lifecycle through a mocked plugin manager | No |
| `PgCacheBenchmarks` | The remote query cache plugin against real infrastructure | **Yes** |

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

Useful flags while iterating: `-f 1 -wi 1 -i 1` for a fast smoke run, `-rf json -rff result.json` to
capture machine-readable results, `-prof gc` to add allocation numbers, and a class-name regex to
select a subset, for example:

```shell
java -jar build/libs/benchmarks-<version>-jmh.jar WrapperOverheadBenchmarks
```

A smoke run is enough to confirm every benchmark still executes, but **not** enough to draw
conclusions from. Use the default iteration counts before quoting a number.

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

## Interpreting the results

**Read pairs and differences, not absolute values.** The benchmarks run without a database, so the
target driver is stood in for by `support/FakeJdbc` - `java.lang.reflect.Proxy` instances that serve
constant values. That is deliberate: the alternative is Mockito, whose dispatch and invocation
recording cost more than the wrapper code under test. The stand-in cost sits underneath both sides of
every `raw*`/`wrapped*` pair and underneath the baseline of every plugin benchmark, so it cancels out
of the difference. It does not cancel out of an absolute number, and none of these numbers include a
network round trip.

**Plugin cost is a cliff, not a gradient.** `ConnectionPluginManager` computes whether a JDBC method
has subscribers while explicitly excluding the terminal `DefaultConnectionPlugin`. When nothing
subscribes, the whole pipeline is bypassed. So the interesting quantity in
`RealPluginChainBenchmarks` is the step from `noPlugins` to the first *subscribing* plugin, most of
which is the default plugin's SQL analysis rather than the plugin itself. Adding further plugins on
top costs comparatively little.

**Contended benchmarks are separate on purpose.** Several driver caches take a write path on read,
and the round-robin host selector holds a lock across its whole selection. Those costs are invisible
single-threaded, so the affected classes carry explicit `contended*` variants annotated with
`@Threads`.

## What is not covered

- Failover, monitor probes, topology refreshes and credential fetches. These are driven by network
  events and background threads rather than by a JDBC call, and cannot be triggered from a benchmark
  without real infrastructure.
- Connect-only plugins (`iam`, `awsSecretsManager`, `federatedAuth`, `okta`). Their real cost is a
  network call; on the execute path they would score flat.
- Dialect detection, which requires a live database. `PluginServiceBenchmarks` supplies a fixed
  dialect so the rest of `PluginServiceImpl` can be measured.

## Notes for maintainers

- `../gradlew test` does not compile this module: the benchmarks live in the `jmh` source set. CI runs
  `:benchmarks:jmhJar` in the `lint` job for exactly that reason, so drift against the wrapper's
  internal APIs fails the build rather than being discovered later.
- The `support` package holds the shared stand-ins. Where the driver has a real fallback
  implementation, the real one is used instead of a stand-in - `UnknownDialect`,
  `GenericTargetDriverDialect`, `MonitorService`, `SimpleConnectionContextServiceImpl` and
  `ImportantEventService`. Real plugins read these during construction and on the hot path, so
  standing them in would both crash and misrepresent cost.
- `PluginBenchmarks` still drives a mocked `ConnectionPluginManager`, so its `wrapperPlugins` values
  select which JDBC pipeline shape is exercised rather than a real plugin chain. Real chains live in
  `RealPluginChainBenchmarks`.
