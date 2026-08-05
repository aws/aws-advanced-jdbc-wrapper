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

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
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
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.HostSpecBuilder;
import software.amazon.jdbc.hostavailability.SimpleHostAvailabilityStrategy;
import software.amazon.jdbc.util.ConnectionUrlParser;
import software.amazon.jdbc.util.PropertyUtils;

/**
 * Micro-benchmarks for {@link ConnectionUrlParser} and {@link PropertyUtils}.
 *
 * <p>Everything here runs once per {@code DriverManager.getConnection} call, before any query is
 * sent. That makes it irrelevant to steady-state query throughput but directly relevant to
 * connection-establishment latency, which matters for short-lived connections and for pool warm-up
 * where hundreds of connections are opened at once. None of it was measured before.
 *
 * <p>{@code getHostsFromConnectionUrl} is measured against a single host and against a
 * multi-host list, because it classifies each host through {@link software.amazon.jdbc.util.RdsUtils}
 * and therefore scales with host count rather than URL length.
 */
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ConnectionUrlParserBenchmarks {

  private static final String SINGLE_HOST_URL =
      "jdbc:aws-wrapper:postgresql://my-cluster.cluster-XYZ.us-east-2.rds.amazonaws.com:5432/postgres";
  private static final String MULTI_HOST_URL =
      "jdbc:aws-wrapper:postgresql://instance-1.XYZ.us-east-2.rds.amazonaws.com:5432,"
          + "instance-2.XYZ.us-east-2.rds.amazonaws.com:5432,"
          + "instance-3.XYZ.us-east-2.rds.amazonaws.com:5432,"
          + "instance-4.XYZ.us-east-2.rds.amazonaws.com:5432,"
          + "instance-5.XYZ.us-east-2.rds.amazonaws.com:5432/postgres";
  private static final String URL_WITH_PROPERTIES =
      SINGLE_HOST_URL + "?user=someUser&password=somePassword&wrapperPlugins=failover2,efm2"
          + "&connectTimeout=10000&socketTimeout=30000&ApplicationName=benchmark";

  private final ConnectionUrlParser parser = new ConnectionUrlParser();
  private final Supplier<HostSpecBuilder> hostSpecBuilderSupplier =
      () -> new HostSpecBuilder(new SimpleHostAvailabilityStrategy());

  private Properties sourceProperties;

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(ConnectionUrlParserBenchmarks.class.getSimpleName())
        .addProfiler(GCProfiler.class)
        .detectJvmArgs()
        .build();

    new Runner(opt).run();
  }

  @Setup(Level.Trial)
  public void setUp() {
    this.sourceProperties = new Properties();
    this.sourceProperties.setProperty("user", "someUser");
    this.sourceProperties.setProperty("password", "somePassword");
    this.sourceProperties.setProperty("wrapperPlugins", "failover2,efm2");
    this.sourceProperties.setProperty("connectTimeout", "10000");
    this.sourceProperties.setProperty("socketTimeout", "30000");
  }

  @Benchmark
  public List<HostSpec> getHostsSingleHost() {
    return parser.getHostsFromConnectionUrl(SINGLE_HOST_URL, false, hostSpecBuilderSupplier);
  }

  @Benchmark
  public List<HostSpec> getHostsFiveHosts() {
    return parser.getHostsFromConnectionUrl(MULTI_HOST_URL, false, hostSpecBuilderSupplier);
  }

  @Benchmark
  public List<HostSpec> getHostsFiveHostsSingleWriter() {
    return parser.getHostsFromConnectionUrl(MULTI_HOST_URL, true, hostSpecBuilderSupplier);
  }

  @Benchmark
  public HostSpec parseHostPortPair() {
    return ConnectionUrlParser.parseHostPortPair(
        "instance-1.XYZ.us-east-2.rds.amazonaws.com:5432", hostSpecBuilderSupplier);
  }

  @Benchmark
  public String parseDatabaseFromUrl() {
    return ConnectionUrlParser.parseDatabaseFromUrl(URL_WITH_PROPERTIES);
  }

  @Benchmark
  public String getProtocol() {
    return parser.getProtocol(SINGLE_HOST_URL);
  }

  @Benchmark
  public Properties parsePropertiesFromUrl() {
    final Properties props = new Properties();
    ConnectionUrlParser.parsePropertiesFromUrl(URL_WITH_PROPERTIES, props);
    return props;
  }

  /**
   * Property copying happens several times per connection (URL properties, profile properties,
   * per-plugin copies), so its cost is multiplied even though a single call is trivial.
   */
  @Benchmark
  public Properties copyProperties() {
    return PropertyUtils.copyProperties(sourceProperties);
  }
}
