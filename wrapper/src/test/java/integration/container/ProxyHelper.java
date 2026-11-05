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

package integration.container;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.logging.Logger;
import software.amazon.jdbc.util.TestMessages;
import software.amazon.orchestra.client.NetworkControlClient;

/**
 * Impairs network traffic to a database endpoint, by name.
 *
 * <p>The impairment is applied by the transparent gateway in front of the databases, through its control
 * client. The ~86 call sites across the suite name what they want impaired and say nothing about how, which
 * is what let the mechanism underneath change without touching a single test.
 *
 * <h2>Why this is not the gateway's Toxiproxy-shaped API</h2>
 *
 * <p>It was, for one migration step. The gateway deliberately mimics Toxiproxy's HTTP shape, so the
 * Toxiproxy Java client could drive it unchanged, and that is what got the failover suite passing without
 * touching any test. It was a useful shortcut and a poor destination: the tests still depended on
 * Toxiproxy's jar and data model to talk to a component that is not Toxiproxy, and the intent had to be
 * reversed twice in the reader's head - a bandwidth toxic at rate zero meaning "drop packets", a downstream
 * latency toxic meaning "delay this destination in both directions".
 *
 * <p>{@code NetworkControlClient} says those things directly. The intent survives into the gateway's log
 * lines, and a failure in impairment no longer looks like a Toxiproxy failure.
 */
public class ProxyHelper {

  private static final Logger LOGGER = Logger.getLogger(ProxyHelper.class.getName());

  /** Stops all traffic to and from server. */
  public static void disableAllConnectivity() {
    for (String name : required(
        TestEnvironment.getCurrent().getProxyNames(),
        TestMessages.get("ProxyHelper.allDestinations"))) {
      disableConnectivity(name);
    }
  }

  /** Stops all traffic to and from server. */
  public static void disableConnectivity(String instanceName) {
    requireRegistered(instanceName);
    // Dropped, not rejected. A reset would let the client fail fast, and the failover tests depend on it
    // blocking until its socket timeout - that stall is what they are measuring.
    strictGateway(() -> client().disable(instanceName),
        TestMessages.get("ProxyHelper.disabling", new Object[] {instanceName}));
  }

  /** Allow traffic to and from server. */
  public static void enableAllConnectivity() {
    for (String name : TestEnvironment.getCurrent().getProxyNames()) {
      enableConnectivity(name);
    }
  }

  /** Allow traffic to and from server. */
  public static void enableConnectivity(String instanceName) {
    bestEffortGateway(() -> client().enable(instanceName),
        TestMessages.get("ProxyHelper.enabling", new Object[] {instanceName}));
  }

  public static void setLatency(String instanceName, int latencyMs) {
    requireRegistered(instanceName);
    // No jitter: the tests want a predictable difference between endpoints, and jitter would make a
    // "fastest host" assertion depend on which sample the driver happened to take.
    strictGateway(() -> client().addLatency(instanceName, latencyMs, 0),
        TestMessages.get("ProxyHelper.settingLatency", new Object[] {instanceName, latencyMs}));
  }

  public static void clearAllLatencies() {
    for (String name : TestEnvironment.getCurrent().getProxyNames()) {
      bestEffortGateway(() -> client().removeLatency(name),
          TestMessages.get("ProxyHelper.clearingLatency", new Object[] {name}));
    }
  }

  /**
   * Stops all traffic to one region of a global database.
   *
   * <p>Region-scoped rather than folded into {@link #disableAllConnectivity()}, because "all" means the region
   * the suite connects to and has to keep meaning that - the failover tests depend on it. This is the operation a
   * cross-region test wants: make one region genuinely unreachable and leave the others alone.
   *
   * <p>Every endpoint in the region, instances and both cluster endpoints, so there is no address left for a
   * client to reconnect through. A partially cut region is not a failure mode that occurs in practice and would
   * let a driver appear to survive an outage it never experienced.
   *
   * @param region the AWS region to cut off
   */
  public static void disableRegionConnectivity(final String region) {
    for (String name : required(
        TestEnvironment.getCurrent().getProxyNames(region),
        TestMessages.get("ProxyHelper.region", new Object[] {region}))) {
      disableConnectivity(name);
    }
    LOGGER.finest(() -> TestMessages.get("ProxyHelper.regionUnreachable", new Object[] {region}));
  }

  /**
   * Restores traffic to one region of a global database.
   *
   * @param region the AWS region to restore
   */
  public static void enableRegionConnectivity(final String region) {
    final Collection<String> names = TestEnvironment.getCurrent().getProxyNames(region);
    for (String name : names) {
      enableConnectivity(name);
    }
    LOGGER.finest(() -> TestMessages.get("ProxyHelper.regionReachable", new Object[] {region}));
  }

  private static void requireRegistered(final String name) {
    if (!TestEnvironment.getCurrent().isProxyNameRegistered(name)) {
      throw new IllegalArgumentException(TestMessages.get(
          "ProxyHelper.unknownDestination", new Object[] {name}));
    }
  }

  private static NetworkControlClient client() {
    return NetworkControlClient.fromEnvironment();
  }

  /** Runs an impairment call and fails the test when the gateway rejects it. */
  public static void strictGateway(final GatewayCall call, final String description) {
    try {
      call.run();
      LOGGER.finest(() -> TestMessages.get("ProxyHelper.gatewayAction", new Object[] {description}));
    } catch (IOException e) {
      throw new UncheckedIOException(TestMessages.get(
          "ProxyHelper.gatewayFailure", new Object[] {description, e.getMessage()}), e);
    }
  }

  /** Runs a restore call without replacing the test's original failure during cleanup. */
  public static void bestEffortGateway(final GatewayCall call, final String description) {
    try {
      call.run();
      LOGGER.finest(() -> TestMessages.get("ProxyHelper.gatewayAction", new Object[] {description}));
    } catch (IOException e) {
      LOGGER.finest(() -> TestMessages.get(
          "ProxyHelper.gatewayError", new Object[] {description, e.getMessage()}));
    }
  }

  /** Rejects an empty impairment target set before a no-op can look like a successful outage. */
  public static Collection<String> required(
      final Collection<String> names, final String description) {
    if (names == null || names.isEmpty()) {
      throw new IllegalArgumentException(TestMessages.get(
          "ProxyHelper.noDestinations", new Object[] {description}));
    }
    return names;
  }

  /** A gateway operation that may fail to reach the gateway. */
  public interface GatewayCall {
    void run() throws IOException;
  }
}
