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
import java.util.logging.Logger;
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
    for (String name : TestEnvironment.getCurrent().getProxyNames()) {
      disableConnectivity(name);
    }
  }

  /** Stops all traffic to and from server. */
  public static void disableConnectivity(String instanceName) {
    // Dropped, not rejected. A reset would let the client fail fast, and the failover tests depend on it
    // blocking until its socket timeout - that stall is what they are measuring.
    gateway(() -> client().disable(instanceName), "disabling connectivity to " + instanceName);
  }

  /** Allow traffic to and from server. */
  public static void enableAllConnectivity() {
    for (String name : TestEnvironment.getCurrent().getProxyNames()) {
      enableConnectivity(name);
    }
  }

  /** Allow traffic to and from server. */
  public static void enableConnectivity(String instanceName) {
    gateway(() -> client().enable(instanceName), "enabling connectivity to " + instanceName);
  }

  public static void setLatency(String instanceName, int latencyMs) {
    // No jitter: the tests want a predictable difference between endpoints, and jitter would make a
    // "fastest host" assertion depend on which sample the driver happened to take.
    gateway(() -> client().addLatency(instanceName, latencyMs, 0),
        "setting latency for " + instanceName + " to " + latencyMs + "ms");
  }

  public static void clearAllLatencies() {
    for (String name : TestEnvironment.getCurrent().getProxyNames()) {
      gateway(() -> client().removeLatency(name), "clearing latency for " + name);
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
    for (String name : TestEnvironment.getCurrent().getProxyNames(region)) {
      disableConnectivity(name);
    }
    LOGGER.finest("Gateway: region " + region + " is now unreachable");
  }

  /**
   * Restores traffic to one region of a global database.
   *
   * @param region the AWS region to restore
   */
  public static void enableRegionConnectivity(final String region) {
    for (String name : TestEnvironment.getCurrent().getProxyNames(region)) {
      enableConnectivity(name);
    }
    LOGGER.finest("Gateway: region " + region + " is reachable again");
  }

  private static NetworkControlClient client() {
    return NetworkControlClient.fromEnvironment();
  }

  /**
   * Runs a gateway call, logging rather than throwing on failure.
   *
   * <p>Deliberate, and unchanged from when there were two mechanisms behind this class. These helpers are
   * called from {@code @BeforeEach} and {@code @AfterEach} as well as from test bodies, and a restore that
   * throws during cleanup would replace a real test failure with a teardown one.
   */
  private static void gateway(final GatewayCall call, final String description) {
    try {
      call.run();
      LOGGER.finest("Gateway: " + description);
    } catch (IOException e) {
      LOGGER.finest("Error " + description + ": " + e.getMessage());
    }
  }

  /** A gateway operation that may fail to reach the gateway. */
  private interface GatewayCall {
    void run() throws IOException;
  }
}
