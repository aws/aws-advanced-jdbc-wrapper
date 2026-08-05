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

import java.util.ArrayList;
import java.util.List;
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
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.benchmarks.support.NoOpEventPublisher;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.hostlistprovider.Topology;
import software.amazon.jdbc.util.storage.CacheMap;
import software.amazon.jdbc.util.storage.ExpirationCache;
import software.amazon.jdbc.util.storage.SlidingExpirationCache;
import software.amazon.jdbc.util.storage.SlidingExpirationCacheWithCleanupThread;
import software.amazon.jdbc.util.storage.StorageServiceImpl;

/**
 * Benchmarks for the driver's caching layer: {@link CacheMap}, {@link ExpirationCache},
 * {@link SlidingExpirationCache}, {@link SlidingExpirationCacheWithCleanupThread} and
 * {@link StorageServiceImpl}.
 *
 * <p>These caches sit in front of topology lookups, monitor registries, blue/green status and
 * connection pools, so they are read on connect, on every host-list refresh and by every monitoring
 * tick. None of them had benchmark coverage.
 *
 * <p>Two properties matter here and neither is visible from a single-threaded average:
 *
 * <ul>
 *   <li><b>Every read mutates.</b> {@code SlidingExpirationCache.get} extends the entry's expiry and
 *       {@code CacheMap.get} runs {@code computeIfPresent}, so reads take a write path on a
 *       {@link java.util.concurrent.ConcurrentHashMap} bin. The {@code contended*} benchmarks run the
 *       same reads on four threads against a single key, which is the shape the driver actually
 *       produces when several connections share one topology entry.
 *   <li><b>Cleanup is amortised onto callers.</b> {@code CacheMap} and {@code SlidingExpirationCache}
 *       sweep the whole map from inside {@code get}/{@code put} once the cleanup deadline passes, so
 *       an occasional call pays for every entry. {@code cleanupSweep} forces that path by setting a
 *       zero cleanup interval, which is why it is reported separately - it is the tail, not the mean.
 * </ul>
 *
 * <p>Caches are sized at {@code ENTRY_COUNT} entries to keep bin collisions and sweep costs
 * representative rather than measuring a one-entry map.
 */
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class StorageBenchmarks {

  private static final int ENTRY_COUNT = 100;
  private static final long FIVE_MINUTES_NANOS = TimeUnit.MINUTES.toNanos(5);
  private static final String HOT_KEY = "key-0";

  private CacheMap<String, String> cacheMap;
  private ExpirationCache<String, String> expirationCache;
  private ExpirationCache<String, String> renewableExpirationCache;
  private SlidingExpirationCache<String, String> slidingCache;
  private SlidingExpirationCacheWithCleanupThread<String, String> slidingCacheWithThread;
  private CacheMap<String, String> sweepingCacheMap;
  private SlidingExpirationCache<String, String> sweepingSlidingCache;
  private StorageServiceImpl storageService;
  private Topology topology;

  private String[] keys;
  private int cursor;

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(StorageBenchmarks.class.getSimpleName())
        .addProfiler(GCProfiler.class)
        .detectJvmArgs()
        .build();

    new Runner(opt).run();
  }

  @Setup(Level.Trial)
  public void setUp() {
    this.keys = new String[ENTRY_COUNT];
    for (int i = 0; i < ENTRY_COUNT; i++) {
      this.keys[i] = "key-" + i;
    }

    this.cacheMap = new CacheMap<>();
    this.expirationCache = new ExpirationCache<>();
    this.renewableExpirationCache = new ExpirationCache<>(true, FIVE_MINUTES_NANOS, null, null);
    this.slidingCache = new SlidingExpirationCache<>();
    this.slidingCacheWithThread = new SlidingExpirationCacheWithCleanupThread<>();

    for (int i = 0; i < ENTRY_COUNT; i++) {
      final String key = this.keys[i];
      final String value = "value-" + i;
      this.cacheMap.put(key, value, FIVE_MINUTES_NANOS);
      this.expirationCache.put(key, value);
      this.renewableExpirationCache.put(key, value);
      this.slidingCache.put(key, value, FIVE_MINUTES_NANOS);
      this.slidingCacheWithThread.put(key, value, FIVE_MINUTES_NANOS);
    }

    // Caches whose cleanup deadline is always in the past, so every access performs a full sweep.
    // Entries are given a long TTL so the sweep walks them without removing anything - the point is
    // to price the sweep, not the eviction.
    this.sweepingCacheMap = new ZeroIntervalCacheMap<>();
    this.sweepingSlidingCache = new SlidingExpirationCache<>();
    this.sweepingSlidingCache.setCleanupIntervalNanos(0);
    for (int i = 0; i < ENTRY_COUNT; i++) {
      this.sweepingCacheMap.put(this.keys[i], "value-" + i, FIVE_MINUTES_NANOS);
      this.sweepingSlidingCache.put(this.keys[i], "value-" + i, FIVE_MINUTES_NANOS);
    }

    // A five-host topology, the size of a default Aurora cluster, is what the topology cache holds.
    final List<HostSpec> hosts = new ArrayList<>(5);
    for (int i = 0; i < 5; i++) {
      hosts.add(new HostSpecBuilder(new SimpleHostAvailabilityStrategy())
          .host("instance-" + i + ".XYZ.us-east-2.rds.amazonaws.com")
          .port(5432)
          .build());
    }
    this.topology = new Topology(hosts);

    this.storageService = new StorageServiceImpl(new NoOpEventPublisher());
    for (int i = 0; i < ENTRY_COUNT; i++) {
      this.storageService.set(this.keys[i], this.topology);
    }

    this.cursor = 0;
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    // StorageServiceImpl and SlidingExpirationCacheWithCleanupThread both own background executors.
    // Leaving them running leaks a thread per fork and lets cleanup work land in the middle of a
    // later measurement.
    this.storageService.releaseResources();
    this.slidingCacheWithThread.clear();
  }

  private String nextKey() {
    final int index = this.cursor++;
    return this.keys[(index & Integer.MAX_VALUE) % ENTRY_COUNT];
  }

  // ---------------------------------------------------------------------------------------------
  // CacheMap
  // ---------------------------------------------------------------------------------------------

  @Benchmark
  public String cacheMapGetHit() {
    return this.cacheMap.get(nextKey());
  }

  @Benchmark
  public String cacheMapGetMiss() {
    return this.cacheMap.get("absent");
  }

  @Benchmark
  public String cacheMapGetWithDefault() {
    return this.cacheMap.get(HOT_KEY, "default", FIVE_MINUTES_NANOS);
  }

  @Benchmark
  public CacheMap<String, String> cacheMapPut() {
    this.cacheMap.put(nextKey(), "value", FIVE_MINUTES_NANOS);
    return this.cacheMap;
  }

  @Benchmark
  @Threads(4)
  public String contendedCacheMapGetHit() {
    return this.cacheMap.get(HOT_KEY);
  }

  // ---------------------------------------------------------------------------------------------
  // ExpirationCache
  // ---------------------------------------------------------------------------------------------

  @Benchmark
  public String expirationCacheGetHit() {
    return this.expirationCache.get(nextKey());
  }

  @Benchmark
  public String expirationCacheGetMiss() {
    return this.expirationCache.get("absent");
  }

  /**
   * A renewable cache rewrites the entry's expiry on read, so this is the read-that-writes variant
   * of {@link #expirationCacheGetHit()}. The gap between the two is the cost of renewal.
   */
  @Benchmark
  public String renewableExpirationCacheGetHit() {
    return this.renewableExpirationCache.get(nextKey());
  }

  @Benchmark
  public boolean expirationCacheExists() {
    return this.expirationCache.exists(HOT_KEY);
  }

  @Benchmark
  public String expirationCacheComputeIfAbsentHit() {
    return this.expirationCache.computeIfAbsent(HOT_KEY, k -> "value");
  }

  @Benchmark
  @Threads(4)
  public String contendedExpirationCacheGetHit() {
    return this.expirationCache.get(HOT_KEY);
  }

  @Benchmark
  @Threads(4)
  public String contendedRenewableExpirationCacheGetHit() {
    return this.renewableExpirationCache.get(HOT_KEY);
  }

  // ---------------------------------------------------------------------------------------------
  // SlidingExpirationCache
  // ---------------------------------------------------------------------------------------------

  @Benchmark
  public String slidingCacheGetHit() {
    return this.slidingCache.get(nextKey(), FIVE_MINUTES_NANOS);
  }

  @Benchmark
  public String slidingCacheGetMiss() {
    return this.slidingCache.get("absent", FIVE_MINUTES_NANOS);
  }

  @Benchmark
  public String slidingCacheComputeIfAbsentHit() {
    return this.slidingCache.computeIfAbsent(HOT_KEY, k -> "value", FIVE_MINUTES_NANOS);
  }

  /**
   * The cleanup-thread variant overrides {@code cleanUp()} to do nothing, so its reads never check
   * the cleanup deadline. The gap against {@link #slidingCacheGetHit()} is what callers of the plain
   * cache pay for that deadline check on every read.
   */
  @Benchmark
  public String slidingCacheWithCleanupThreadGetHit() {
    return this.slidingCacheWithThread.get(nextKey(), FIVE_MINUTES_NANOS);
  }

  @Benchmark
  @Threads(4)
  public String contendedSlidingCacheGetHit() {
    return this.slidingCache.get(HOT_KEY, FIVE_MINUTES_NANOS);
  }

  // ---------------------------------------------------------------------------------------------
  // Amortised cleanup sweeps
  // ---------------------------------------------------------------------------------------------

  /** Prices the full-map sweep that {@link CacheMap#put} runs once its cleanup deadline passes. */
  @Benchmark
  public CacheMap<String, String> cleanupSweepCacheMapPut() {
    this.sweepingCacheMap.put(HOT_KEY, "value", FIVE_MINUTES_NANOS);
    return this.sweepingCacheMap;
  }

  /** Prices the same sweep on {@link SlidingExpirationCache#get}, which sweeps on read as well. */
  @Benchmark
  public String cleanupSweepSlidingCacheGet() {
    return this.sweepingSlidingCache.get(HOT_KEY, FIVE_MINUTES_NANOS);
  }

  // ---------------------------------------------------------------------------------------------
  // StorageService
  // ---------------------------------------------------------------------------------------------

  /**
   * The real topology lookup: a class-keyed cache dispatch, an inner cache read, an
   * {@code isInstance} check and a {@code DataAccessEvent} allocation.
   */
  @Benchmark
  public Topology storageServiceGetTopology() {
    return this.storageService.get(Topology.class, nextKey());
  }

  /** Same lookup with event registration suppressed, which isolates the event allocation cost. */
  @Benchmark
  public Topology storageServiceGetTopologyNoDataAccess() {
    return this.storageService.get(Topology.class, nextKey(), false);
  }

  @Benchmark
  public Topology storageServiceGetMiss() {
    return this.storageService.get(Topology.class, "absent");
  }

  @Benchmark
  public boolean storageServiceExists() {
    return this.storageService.exists(Topology.class, HOT_KEY);
  }

  @Benchmark
  public StorageServiceImpl storageServiceSetTopology() {
    this.storageService.set(nextKey(), this.topology);
    return this.storageService;
  }

  @Benchmark
  @Threads(4)
  public Topology contendedStorageServiceGetTopology() {
    return this.storageService.get(Topology.class, HOT_KEY);
  }

  /**
   * A {@link CacheMap} whose cleanup deadline has always passed, so every mutating call performs a
   * full sweep. {@code cleanupIntervalNanos} is final in the parent, so the deadline is pushed into
   * the past instead of shortening the interval.
   */
  private static class ZeroIntervalCacheMap<K, V> extends CacheMap<K, V> {
    @Override
    protected void cleanUp() {
      this.cleanupTimeNanos.set(Long.MIN_VALUE);
      super.cleanUp();
    }
  }
}
