package software.amazon.jdbc;/*
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

import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;

public class ConnectionProviderManager {

  private static final ReentrantLock connProviderLock = new ReentrantLock();
  private static ConnectionProvider connProvider = null;
  private ConnectionProvider defaultProvider;

  public ConnectionProviderManager(ConnectionProvider defaultProvider) {
    this.defaultProvider = defaultProvider;
  }

  public static void setConnectionProvider(ConnectionProvider connProvider) {
    connProviderLock.lock();
    try {
      ConnectionProviderManager.connProvider = connProvider;
    } finally {
      connProviderLock.unlock();
    }
  }

  public ConnectionProvider getConnectionProvider(
      String driverProtocol, HostSpec host, Properties props) {
    connProviderLock.lock();
    try {
      if (connProvider != null && connProvider.acceptsUrl(driverProtocol, host, props)) {
        return connProvider;
      }
      return defaultProvider;
    } finally {
      connProviderLock.unlock();
    }
  }

  public ConnectionProvider getDefaultProvider() {
    return defaultProvider;
  }

  public static void resetProvider() {
    connProviderLock.lock();
    connProvider = null;
    connProviderLock.unlock();
  }

  public static void releaseResources() {
    connProviderLock.lock();
    try {
      if (connProvider != null) {
        connProvider.releaseResources();
      }
    } finally {
      connProviderLock.unlock();
    }
  }
}
