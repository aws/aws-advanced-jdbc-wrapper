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

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import software.amazon.jdbc.benchmarks.support.BenchmarkServices;
import software.amazon.jdbc.benchmarks.support.RealPluginChain;
import software.amazon.jdbc.util.CoreServicesContainer;
import software.amazon.jdbc.util.Pair;
import software.amazon.jdbc.util.storage.StorageService;

/**
 * Steady-state footprint of a host monitoring plugin under sustained load. Not a JMH benchmark: JMH measures the
 * cost of a call, this measures what stays behind between calls.
 *
 * <p>Runs N wrapper connections to one host, each executing queries at a fixed rate, and every few seconds forces a
 * GC and prints the live heap, the monitoring threads, and the monitors' own bookkeeping sizes taken from
 * {@code MonitorService.getSnapshotState()}. With {@code efm2} the bookkeeping is one weak reference per call for the
 * whole {@code failureDetectionTime} (so {@code rate x connections x 30 s} live objects); with {@code efm3} it is one
 * slot per connection. The optional churn mode replaces connections while running and drops half of the replaced ones
 * without releasing their plugins, the way an application leaks connections, to show whether the plugin's state
 * follows them or is swept.
 *
 * <p>Usage, from the shaded JMH jar:
 * <pre>
 * java -Xmx1g -cp build/libs/benchmarks-&lt;version&gt;-jmh.jar software.amazon.jdbc.benchmarks.EfmSoakHarness \
 *     [plugin=efm2] [connections=64] [durationSec=120] [ratePerConnection=1000] [reportSec=10] [churnEveryCalls=0] \
 *     [pluginProps=]
 * </pre>
 * Arguments are positional. {@code churnEveryCalls=0} disables churn. {@code pluginProps} is a comma-separated
 * {@code key=value} list passed to the plugins, e.g. {@code monitorDisposalTime=30000} to watch leaked state being
 * swept within a short run.
 */
public final class EfmSoakHarness {

  private static final String WRAPPER_THREAD_PREFIX = "AWS Advanced JDBC Wrapper";

  private EfmSoakHarness() {
  }

  public static void main(final String[] args) throws Exception {
    final String plugin = arg(args, 0, "efm2");
    final int connections = Integer.parseInt(arg(args, 1, "64"));
    final int durationSec = Integer.parseInt(arg(args, 2, "120"));
    final int ratePerConnection = Integer.parseInt(arg(args, 3, "1000"));
    final int reportSec = Integer.parseInt(arg(args, 4, "10"));
    final long churnEveryCalls = Long.parseLong(arg(args, 5, "0"));
    final Properties pluginProps = parseProps(arg(args, 6, ""));

    System.out.printf(Locale.ROOT,
        "plugin=%s connections=%d durationSec=%d ratePerConnection=%d reportSec=%d churnEveryCalls=%d pluginProps=%s%n",
        plugin, connections, durationSec, ratePerConnection, reportSec, churnEveryCalls, pluginProps);

    final StorageService sharedStorage = BenchmarkServices.storageService();
    final AtomicBoolean stop = new AtomicBoolean(false);
    final AtomicLong totalCalls = new AtomicLong();
    final AtomicLong replaced = new AtomicLong();
    final AtomicLong leaked = new AtomicLong();
    final CountDownLatch done = new CountDownLatch(connections);
    final List<Thread> workers = new ArrayList<>();

    for (int i = 0; i < connections; i++) {
      final boolean leakOnChurn = i % 2 == 0;
      final Thread worker = new Thread(() -> {
        try {
          runConnection(plugin, pluginProps, sharedStorage, ratePerConnection, churnEveryCalls, leakOnChurn,
              stop, totalCalls, replaced, leaked);
        } catch (final Exception e) {
          e.printStackTrace();
        } finally {
          done.countDown();
        }
      }, "soak-connection-" + i);
      worker.setDaemon(true);
      workers.add(worker);
    }
    workers.forEach(Thread::start);

    System.out.println(
        "elapsedSec,calls,callsPerSec,heapAfterGcMB,wrapperThreads,efmThreads,replaced,leaked,monitorState");
    final long start = System.nanoTime();
    long lastCalls = 0;
    long lastReport = start;
    while (TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) < durationSec) {
      TimeUnit.SECONDS.sleep(reportSec);
      final long now = System.nanoTime();
      final long calls = totalCalls.get();
      final double perSec = (calls - lastCalls) / Math.max(1e-9, (now - lastReport) / 1e9);
      lastCalls = calls;
      lastReport = now;
      report(TimeUnit.NANOSECONDS.toSeconds(now - start), calls, perSec, replaced.get(), leaked.get());
    }

    stop.set(true);
    done.await(30, TimeUnit.SECONDS);
    report(TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start), totalCalls.get(), 0, replaced.get(),
        leaked.get());
    BenchmarkServices.releaseStorage(sharedStorage);
    CoreServicesContainer.getInstance().getMonitorService().stopAndRemoveAll();
  }

  private static void runConnection(
      final String plugin,
      final Properties pluginProps,
      final StorageService sharedStorage,
      final int ratePerConnection,
      final long churnEveryCalls,
      final boolean leakOnChurn,
      final AtomicBoolean stop,
      final AtomicLong totalCalls,
      final AtomicLong replaced,
      final AtomicLong leaked) throws SQLException {

    final long intervalNanos = ratePerConnection <= 0 ? 0 : TimeUnit.SECONDS.toNanos(1) / ratePerConnection;
    RealPluginChain chain = RealPluginChain.build(plugin, pluginProps, sharedStorage);
    long callsOnThisChain = 0;
    long next = System.nanoTime();

    try {
      while (!stop.get()) {
        chain.executeQuery();
        totalCalls.incrementAndGet();
        callsOnThisChain++;

        if (churnEveryCalls > 0 && callsOnThisChain >= churnEveryCalls) {
          if (leakOnChurn) {
            chain.dropWithoutReleasingPlugins();
            leaked.incrementAndGet();
          } else {
            chain.close();
          }
          replaced.incrementAndGet();
          chain = RealPluginChain.build(plugin, pluginProps, sharedStorage);
          callsOnThisChain = 0;
        }

        if (intervalNanos > 0) {
          next += intervalNanos;
          final long wait = next - System.nanoTime();
          if (wait > 0) {
            LockSupport.parkNanos(wait);
          } else if (wait < -TimeUnit.SECONDS.toNanos(1)) {
            // Fell too far behind (GC pause, slow machine): do not try to catch up, keep the rate honest.
            next = System.nanoTime();
          }
        }
      }
    } finally {
      chain.close();
    }
  }

  private static void report(
      final long elapsedSec, final long calls, final double perSec, final long replaced, final long leaked) {
    System.gc();
    System.gc();
    final Runtime rt = Runtime.getRuntime();
    final double heapMb = (rt.totalMemory() - rt.freeMemory()) / (1024.0 * 1024.0);

    int wrapperThreads = 0;
    int efmThreads = 0;
    final ThreadMXBean threads = ManagementFactory.getThreadMXBean();
    for (final ThreadInfo info : threads.getThreadInfo(threads.getAllThreadIds())) {
      if (info == null || !info.getThreadName().startsWith(WRAPPER_THREAD_PREFIX)) {
        continue;
      }
      wrapperThreads++;
      final String name = info.getThreadName().toLowerCase(Locale.ROOT);
      if (name.contains("efm") || name.contains("abort")) {
        efmThreads++;
      }
    }

    System.out.printf(Locale.ROOT, "%d,%d,%.0f,%.1f,%d,%d,%d,%d,%s%n",
        elapsedSec, calls, perSec, heapMb, wrapperThreads, efmThreads, replaced, leaked, monitorState());
  }

  /**
   * Flattens the monitor service snapshot down to the per-monitor bookkeeping counters, summed per name across all
   * monitors so the output does not depend on monitor keys.
   */
  private static String monitorState() {
    final TreeMap<String, Long> sums = new TreeMap<>();
    collect(CoreServicesContainer.getInstance().getMonitorService().getSnapshotState(), sums);
    final StringBuilder sb = new StringBuilder();
    sums.forEach((name, value) -> {
      if (sb.length() > 0) {
        sb.append(' ');
      }
      sb.append(name).append('=').append(value);
    });
    return sb.toString();
  }

  @SuppressWarnings("unchecked")
  private static void collect(final Object node, final TreeMap<String, Long> sums) {
    if (!(node instanceof List)) {
      return;
    }
    for (final Object item : (List<Object>) node) {
      if (!(item instanceof Pair)) {
        continue;
      }
      final Pair<String, Object> pair = (Pair<String, Object>) item;
      final Object value = pair.getValue2();
      if (value instanceof List) {
        collect(value, sums);
      } else if (value instanceof Number && isBookkeeping(pair.getValue1())) {
        sums.merge(pair.getValue1().replace(" (size)", ""), ((Number) value).longValue(), Long::sum);
      }
    }
  }

  private static boolean isBookkeeping(final String name) {
    return name.contains("Contexts") || name.contains("Slots") || name.contains("failureCount")
        || name.contains("consecutiveFailures");
  }

  private static Properties parseProps(final String spec) {
    final Properties props = new Properties();
    for (final String entry : spec.split(",")) {
      final int eq = entry.indexOf('=');
      if (eq > 0) {
        props.setProperty(entry.substring(0, eq).trim(), entry.substring(eq + 1).trim());
      }
    }
    return props;
  }

  private static String arg(final String[] args, final int index, final String defaultValue) {
    return args.length > index ? args[index] : defaultValue;
  }
}
