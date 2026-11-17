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

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import software.amazon.jdbc.benchmarks.support.RealPluginChain;

/**
 * Per-call cost of the host monitoring plugins when many connections to the same host execute concurrently.
 *
 * <p>{@link RealPluginChainBenchmarks} measures one connection on one thread. That misses what distinguishes the
 * monitoring plugins from each other: {@code efm} and {@code efm2} register a context with a JVM-wide pool (one
 * {@code ReentrantLock}) and look up the host monitor through a {@code ConcurrentHashMap.compute} on the host key
 * on every call, so their cost grows with the number of threads hitting one host. {@code efm3} writes only to a
 * per-connection slot. This benchmark gives every thread its own real plugin chain, i.e. its own connection, all to
 * the same host, and reports the average time one call takes as the thread count grows.
 *
 * <p>Run it at several thread counts, e.g. {@code -t 1}, {@code -t 8}, {@code -t 32}, {@code -t 128}, and compare
 * each plugin against {@code executionTime}, a trivial subscribing plugin that prices the pipeline itself. A plugin
 * whose score stays flat as threads grow has no contended state on the call path.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Threads(16)
public class EfmContentionBenchmarks {

  /** One wrapper connection per benchmark thread. */
  @State(Scope.Thread)
  public static class PerThreadConnection {

    @Param({"executionTime", "efm2", "efm3"})
    public String plugin;

    private RealPluginChain chain;

    @Setup
    public void setUp() throws SQLException {
      this.chain = RealPluginChain.build(this.plugin);
    }

    @TearDown
    public void tearDown() {
      this.chain.close();
    }
  }

  @Benchmark
  public Integer executeQuery(final PerThreadConnection connection) {
    return connection.chain.executeQuery();
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(EfmContentionBenchmarks.class.getSimpleName())
        .addProfiler(GCProfiler.class)
        .detectJvmArgs()
        .build();
    new Runner(opt).run();
  }
}
