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

package software.amazon.jdbc.authentication;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

public class AwsCredentialsManager {
  static Supplier<AwsCredentialsProvider> handler = null;
  static int timeout = 0;
  static TimeUnit timeoutUnit;
  static long lastRefreshTimeNano;
  static AwsCredentialsProvider providerCache = null;

  public static synchronized void setCustomHandler(
      Supplier<AwsCredentialsProvider> customHandler) {
    handler = customHandler;
    clearCache();
  }

  public static synchronized void setCustomHandler(Supplier<AwsCredentialsProvider> customHandler,
      int cacheTimeout,
      TimeUnit cacheTimeoutUnit) {
    handler = customHandler;
    timeout = cacheTimeout;
    timeoutUnit = cacheTimeoutUnit;
    providerCache = null;
  }

  public static synchronized void resetCustomHandler() {
    handler = null;
    clearCache();
  }

  public static synchronized void configureCache(int cacheTimeout, TimeUnit cacheTimeoutUnit) {
    timeout = cacheTimeout;
    timeoutUnit = cacheTimeoutUnit;
  }

  private static void clearCache() {
    timeout = 0;
    timeoutUnit = null;
    providerCache = null;
  }

  public static synchronized AwsCredentialsProvider getProvider() {
    long timeSinceLastRefreshNano = System.nanoTime() - lastRefreshTimeNano;
    if (timeout == 0 || providerCache == null || timeSinceLastRefreshNano >= timeoutUnit.toNanos(timeout)) {
      AwsCredentialsProvider provider = handler != null ? handler.get() : getDefaultProvider();
      if (timeout == 0) {
        providerCache = null;
        return provider;
      } else {
        lastRefreshTimeNano = System.nanoTime();
        providerCache = provider;
      }
    }
    return providerCache;
  }

  private static AwsCredentialsProvider getDefaultProvider() {
    return DefaultCredentialsProvider.create();
  }
}
