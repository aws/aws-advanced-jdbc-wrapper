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
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import software.amazon.jdbc.util.RdsUrlType;
import software.amazon.jdbc.util.RdsUtils;

/**
 * Micro-benchmarks for {@link RdsUtils}, the regex-driven endpoint classifier.
 *
 * <p>It runs on connect, on every host-list refresh, and inside {@code ConnectionUrlParser}, so its
 * cost is paid per connection rather than per query - but host-list refreshes are frequent enough
 * under failover and read/write splitting that it is worth knowing the number. It was not measured
 * before.
 *
 * <p>{@code RdsUtils} keeps a static cache of {@link java.util.regex.Matcher} results keyed by host.
 * That makes the cached and uncached costs differ by orders of magnitude, and it means a naive
 * benchmark measures only the cache. Both are measured here:
 *
 * <ul>
 *   <li>{@code cachedHit*} - repeat lookups of one host, the steady state.
 *   <li>{@code uncached*} - a fresh host name per invocation, the cost actually paid the first time
 *       an endpoint is seen. The counter is part of the host name, so these also show how the cache
 *       behaves when it is being filled rather than read.
 * </ul>
 *
 * <p>{@code contendedCachedHit} runs the cached path on several threads to check that the shared
 * static cache does not serialise callers, which a single-threaded benchmark cannot show.
 */
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class RdsUtilsBenchmarks {

  private static final String INSTANCE =
      "instance-1.XYZ.us-east-2.rds.amazonaws.com";
  private static final String WRITER_CLUSTER =
      "my-cluster.cluster-XYZ.us-east-2.rds.amazonaws.com";
  private static final String READER_CLUSTER =
      "my-cluster.cluster-ro-XYZ.us-east-2.rds.amazonaws.com";
  private static final String CUSTOM_CLUSTER =
      "my-custom.cluster-custom-XYZ.us-east-2.rds.amazonaws.com";
  private static final String PROXY =
      "my-proxy.proxy-XYZ.us-east-2.rds.amazonaws.com";
  private static final String NON_RDS = "my-database.example.com";

  private final RdsUtils rdsUtils = new RdsUtils();

  /**
   * Distinguishes host names produced by this benchmark run so a previous run's cache entries (the
   * cache is static and lives for the whole JVM) cannot be mistaken for cache misses.
   */
  private long counter;

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(RdsUtilsBenchmarks.class.getSimpleName())
        .addProfiler(GCProfiler.class)
        .detectJvmArgs()
        .build();

    new Runner(opt).run();
  }

  @Setup(Level.Trial)
  public void setUp() {
    RdsUtils.clearCache();
    this.counter = 0;
    // Prime the cache for the fixed host names so the cachedHit benchmarks never include a miss.
    this.rdsUtils.identifyRdsType(INSTANCE);
    this.rdsUtils.identifyRdsType(WRITER_CLUSTER);
    this.rdsUtils.identifyRdsType(READER_CLUSTER);
    this.rdsUtils.identifyRdsType(CUSTOM_CLUSTER);
    this.rdsUtils.identifyRdsType(PROXY);
    this.rdsUtils.identifyRdsType(NON_RDS);
  }

  @Benchmark
  public RdsUrlType cachedHitIdentifyInstance() {
    return rdsUtils.identifyRdsType(INSTANCE);
  }

  @Benchmark
  public RdsUrlType cachedHitIdentifyWriterCluster() {
    return rdsUtils.identifyRdsType(WRITER_CLUSTER);
  }

  @Benchmark
  public RdsUrlType cachedHitIdentifyReaderCluster() {
    return rdsUtils.identifyRdsType(READER_CLUSTER);
  }

  @Benchmark
  public RdsUrlType cachedHitIdentifyCustomCluster() {
    return rdsUtils.identifyRdsType(CUSTOM_CLUSTER);
  }

  @Benchmark
  public RdsUrlType cachedHitIdentifyProxy() {
    return rdsUtils.identifyRdsType(PROXY);
  }

  /**
   * A non-RDS host matches none of the patterns, so it is the worst case for the classifier: every
   * pattern is tried before it gives up.
   */
  @Benchmark
  public RdsUrlType cachedHitIdentifyNonRds() {
    return rdsUtils.identifyRdsType(NON_RDS);
  }

  @Benchmark
  @Threads(4)
  public RdsUrlType contendedCachedHitIdentifyInstance() {
    return rdsUtils.identifyRdsType(INSTANCE);
  }

  @Benchmark
  public RdsUrlType uncachedIdentifyInstance() {
    return rdsUtils.identifyRdsType("instance-" + (counter++) + ".XYZ.us-east-2.rds.amazonaws.com");
  }

  @Benchmark
  public RdsUrlType uncachedIdentifyNonRds() {
    return rdsUtils.identifyRdsType("host-" + (counter++) + ".example.com");
  }

  @Benchmark
  public String cachedHitGetRdsInstanceId() {
    return rdsUtils.getRdsInstanceId(INSTANCE);
  }

  @Benchmark
  public String cachedHitGetRdsClusterId() {
    return rdsUtils.getRdsClusterId(WRITER_CLUSTER);
  }

  @Benchmark
  public String cachedHitGetRdsRegion() {
    return rdsUtils.getRdsRegion(INSTANCE);
  }

  @Benchmark
  public String cachedHitGetRdsInstanceHostPattern() {
    return rdsUtils.getRdsInstanceHostPattern(INSTANCE);
  }

  @Benchmark
  public boolean cachedHitIsWriterClusterDns() {
    return rdsUtils.isWriterClusterDns(WRITER_CLUSTER);
  }

  @Benchmark
  public boolean cachedHitIsReaderClusterDns() {
    return rdsUtils.isReaderClusterDns(READER_CLUSTER);
  }

  @Benchmark
  public boolean isIPv4Address() {
    return rdsUtils.isIPv4("10.20.30.40");
  }

  @Benchmark
  public boolean isIPv4OnHostName() {
    return rdsUtils.isIPv4(INSTANCE);
  }

  @Benchmark
  public String removePort() {
    return rdsUtils.removePort(INSTANCE + ":5432");
  }

  /**
   * Blue/green routing calls this on every host it evaluates, and unlike the classifier results it
   * is not backed by the matcher cache.
   */
  @Benchmark
  public boolean isGreenInstance() {
    return rdsUtils.isGreenInstance(INSTANCE);
  }
}
