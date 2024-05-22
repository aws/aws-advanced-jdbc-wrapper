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

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import java.io.IOException;
import java.util.logging.Logger;

public class ProxyHelper {

  private static final Logger LOGGER = Logger.getLogger(ProxyHelper.class.getName());

  /** Stops all traffic to and from server. */
  public static void disableAllConnectivity() {
    for (Proxy proxy : TestEnvironment.getCurrent().getProxies()) {
      disableConnectivity(proxy);
    }
  }

  /** Stops all traffic to and from server. */
  public static void disableConnectivity(String instanceName) {
    Proxy proxy = TestEnvironment.getCurrent().getProxy(instanceName);
    if (proxy == null) {
      throw new RuntimeException("Proxy for instance " + instanceName + " not found.");
    }
    disableConnectivity(proxy);
  }

  /** Stops all traffic to and from server. */
  private static void disableConnectivity(Proxy proxy) {
    try {
      proxy
          .toxics()
          .bandwidth(
              "DOWN-STREAM", ToxicDirection.DOWNSTREAM, 0); // from database server towards driver
    } catch (IOException e) {
      LOGGER.finest("Error disabling connectivity DOWN-STREAM: " + e.getMessage());
    }

    try {
      proxy
          .toxics()
          .bandwidth(
              "UP-STREAM", ToxicDirection.UPSTREAM, 0); // from driver towards database server
    } catch (IOException e) {
      LOGGER.finest("Error disabling connectivity UP-STREAM: " + e.getMessage());
    }
    LOGGER.finest("Disabled connectivity to " + proxy.getName());
  }

  /** Allow traffic to and from server. */
  public static void enableAllConnectivity() {
    for (Proxy proxy : TestEnvironment.getCurrent().getProxies()) {
      enableConnectivity(proxy);
    }
  }

  /** Allow traffic to and from server. */
  public static void enableConnectivity(String instanceName) {
    Proxy proxy = TestEnvironment.getCurrent().getProxy(instanceName);
    if (proxy == null) {
      throw new RuntimeException("Proxy for instance " + instanceName + " not found.");
    }
    enableConnectivity(proxy);
  }

  /** Allow traffic to and from server. */
  private static void enableConnectivity(Proxy proxy) {
    try {
      proxy.toxics().getAll().stream()
          .filter(t -> "DOWN-STREAM".equals(t.getName()) || "UP-STREAM".equals(t.getName()))
          .forEach(toxic1 -> {
            try {
              toxic1.remove();
            } catch (IOException e) {
              // ignore
            }
          });
    } catch (IOException ex) {
      LOGGER.finest("Error enabling connectivity: " + ex.getMessage());
    }
    LOGGER.finest("Enabled connectivity to " + proxy.getName());
  }
}
