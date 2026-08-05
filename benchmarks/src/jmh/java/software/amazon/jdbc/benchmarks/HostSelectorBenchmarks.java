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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import software.amazon.jdbc.HighestLoadHostSelector;
import software.amazon.jdbc.HighestWeightHostSelector;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.LeastConnectionsHostSelector;
import software.amazon.jdbc.LowestLoadHostSelector;
import software.amazon.jdbc.RandomHostSelector;
import software.amazon.jdbc.RoundRobinHostSelector;
import software.amazon.jdbc.WeightedRandomHostSelector;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.storage.SlidingExpirationCache;

/**
 * Benchmarks for every {@link software.amazon.jdbc.HostSelector} implementation.
 *
 * <p>A selector runs once per reader acquisition, which under read/write splitting with query-level
 * load balancing means once per read query rather than once per connection. None of them had
 * coverage, and their implementations differ enough that the cost is not obviously uniform: some
 * stream and collect, one sorts, one takes a lock, and one copies a map per candidate host.
 *
 * <p>All selectors are measured against the same five-reader topology so the numbers are directly
 * comparable. Where a selector has a second cost driver it gets extra benchmarks:
 *
 * <ul>
 *   <li>{@code roundRobin*} holds a {@link software.amazon.jdbc.util.ResourceLock} for the whole
 *       selection and sorts the eligible list on every call, so it is also measured under contention.
 *   <li>{@code leastConnections*} calls {@code SlidingExpirationCache.getEntries()} once per
 *       candidate host, and that method copies the whole pool map. It is measured with an empty pool
 *       cache and with 20 pools to show whether the cost scales with pool count.
 *   <li>{@code weightedRandom*} is measured using {@code HostSpec} weights and using the
 *       {@code weightedRandomHostWeightPairs} property, which takes a different, parsing path.
 * </ul>
 */
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class HostSelectorBenchmarks {

  private static final int READER_COUNT = 5;
  private static final int POOL_COUNT = 20;
  private static final String DOMAIN = ".XYZ.us-east-2.rds.amazonaws.com";

  private List<HostSpec> hosts;
  private Properties emptyProps;
  private Properties weightedRandomProps;
  private Properties roundRobinWeightedProps;

  private RandomHostSelector randomSelector;
  private RoundRobinHostSelector roundRobinSelector;
  private WeightedRandomHostSelector weightedRandomSelector;
  private HighestWeightHostSelector highestWeightSelector;
  private LowestLoadHostSelector lowestLoadSelector;
  private HighestLoadHostSelector highestLoadSelector;
  private LeastConnectionsHostSelector leastConnectionsEmptyPools;
  private LeastConnectionsHostSelector leastConnectionsManyPools;

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(HostSelectorBenchmarks.class.getSimpleName())
        .addProfiler(GCProfiler.class)
        .detectJvmArgs()
        .build();

    new Runner(opt).run();
  }

  @Setup(Level.Trial)
  public void setUp() {
    // One writer plus five readers, each carrying distinct weight/cpu/lag so the load- and
    // weight-based selectors have something to discriminate on rather than hitting a tie.
    this.hosts = new ArrayList<>(READER_COUNT + 1);
    this.hosts.add(host("instance-writer", HostRole.WRITER, 1, 10f, 0f));
    for (int i = 0; i < READER_COUNT; i++) {
      this.hosts.add(host("instance-" + i, HostRole.READER, i + 1L, 20f + i * 10, 5f + i * 5));
    }

    this.emptyProps = new Properties();

    this.weightedRandomProps = new Properties();
    this.weightedRandomProps.setProperty(
        WeightedRandomHostSelector.WEIGHTED_RANDOM_HOST_WEIGHT_PAIRS.name,
        weightPairs());

    this.roundRobinWeightedProps = new Properties();
    RoundRobinHostSelector.setRoundRobinHostWeightPairsProperty(
        this.roundRobinWeightedProps, this.hosts);

    this.randomSelector = new RandomHostSelector();
    this.roundRobinSelector = new RoundRobinHostSelector();
    this.weightedRandomSelector = new WeightedRandomHostSelector();
    this.highestWeightSelector = new HighestWeightHostSelector();
    this.lowestLoadSelector = new LowestLoadHostSelector();
    this.highestLoadSelector = new HighestLoadHostSelector();

    this.leastConnectionsEmptyPools =
        new LeastConnectionsHostSelector(new SlidingExpirationCache<>());

    // Pool entries are keyed by host URL; the values are deliberately not HikariDataSource so the
    // selector walks and skips them. That isolates the per-candidate map copy in getEntries() from
    // the cost of querying a real Hikari pool's MXBean.
    final SlidingExpirationCache<Pair, AutoCloseable> pools = new SlidingExpirationCache<>();
    final long ttl = TimeUnit.MINUTES.toNanos(10);
    for (int i = 0; i < POOL_COUNT; i++) {
      pools.put(Pair.create("instance-" + (i % READER_COUNT) + DOMAIN + ":5432", "pool-" + i),
          () -> { }, ttl);
    }
    this.leastConnectionsManyPools = new LeastConnectionsHostSelector(pools);

    RoundRobinHostSelector.clearCache();
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    // The round-robin cache is static and keyed by host name, so leaving entries behind would let
    // one benchmark class influence another within the same fork.
    RoundRobinHostSelector.clearCache();
  }

  private HostSpec host(
      final String name, final HostRole role, final long weight, final float cpu, final float lag) {
    return new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
        .host(name + DOMAIN)
        .hostId(name)
        .port(5432)
        .role(role)
        .weight(weight)
        .cpuPercent(cpu)
        .lagMs(lag)
        .build();
  }

  private String weightPairs() {
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < READER_COUNT; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("instance-").append(i).append(DOMAIN).append(':').append(i + 1);
    }
    return sb.toString();
  }

  @Benchmark
  public HostSpec random() throws SQLException {
    return this.randomSelector.getHost(this.hosts, HostRole.READER, this.emptyProps);
  }

  @Benchmark
  public HostSpec roundRobin() throws SQLException {
    return this.roundRobinSelector.getHost(this.hosts, HostRole.READER, this.emptyProps);
  }

  @Benchmark
  public HostSpec roundRobinWeighted() throws SQLException {
    return this.roundRobinSelector.getHost(this.hosts, HostRole.READER, this.roundRobinWeightedProps);
  }

  /**
   * Round robin serialises on a {@link software.amazon.jdbc.util.ResourceLock} for the whole
   * selection, including the sort. This is the only way to see that.
   */
  @Benchmark
  @Threads(4)
  public HostSpec contendedRoundRobin() throws SQLException {
    return this.roundRobinSelector.getHost(this.hosts, HostRole.READER, this.emptyProps);
  }

  @Benchmark
  public HostSpec weightedRandomFromHostSpec() throws SQLException {
    return this.weightedRandomSelector.getHost(this.hosts, HostRole.READER, this.emptyProps);
  }

  @Benchmark
  public HostSpec weightedRandomFromProperty() throws SQLException {
    return this.weightedRandomSelector.getHost(this.hosts, HostRole.READER, this.weightedRandomProps);
  }

  @Benchmark
  public HostSpec highestWeight() throws SQLException {
    return this.highestWeightSelector.getHost(this.hosts, HostRole.READER, this.emptyProps);
  }

  @Benchmark
  public HostSpec lowestLoad() throws SQLException {
    return this.lowestLoadSelector.getHost(this.hosts, HostRole.READER, this.emptyProps);
  }

  @Benchmark
  public HostSpec highestLoad() throws SQLException {
    return this.highestLoadSelector.getHost(this.hosts, HostRole.READER, this.emptyProps);
  }

  @Benchmark
  public HostSpec leastConnectionsNoPools() throws SQLException {
    return this.leastConnectionsEmptyPools.getHost(this.hosts, HostRole.READER, this.emptyProps);
  }

  /**
   * With pools registered, {@code getEntries()} copies the pool map once per candidate host. The gap
   * against {@link #leastConnectionsNoPools()} is the cost of those copies.
   */
  @Benchmark
  public HostSpec leastConnectionsTwentyPools() throws SQLException {
    return this.leastConnectionsManyPools.getHost(this.hosts, HostRole.READER, this.emptyProps);
  }

  /** No role filter, so every host is eligible - the widest candidate list. */
  @Benchmark
  public HostSpec randomAnyRole() throws SQLException {
    return this.randomSelector.getHost(this.hosts, null, this.emptyProps);
  }
}
