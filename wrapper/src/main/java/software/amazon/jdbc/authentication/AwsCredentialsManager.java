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
  static Supplier<AwsCredentialsProvider> customSupplier = null;
  static int cacheTimeout = 0;
  static TimeUnit timeoutUnit;
  static long lastRefreshTimeNano;
  static AwsCredentialsProvider providerCache = null;

  public static synchronized void setCustomSupplier(
      Supplier<AwsCredentialsProvider> customSupplier) {
    AwsCredentialsManager.customSupplier = customSupplier;
    clearCache();
  }

  public static synchronized void setCustomSupplier(Supplier<AwsCredentialsProvider> customSupplier,
      int timeout,
      TimeUnit timeoutUnits) {
    AwsCredentialsManager.customSupplier = customSupplier;
    AwsCredentialsManager.cacheTimeout = timeout;
    AwsCredentialsManager.timeoutUnit = timeoutUnits;
    providerCache = null;
  }

  public static synchronized void resetCustomSupplier() {
    customSupplier = null;
    clearCache();
  }

  private static void clearCache() {
    cacheTimeout = 0;
    timeoutUnit = null;
    providerCache = null;
  }

  public static synchronized AwsCredentialsProvider getProvider() {
    long timeSinceLastRefreshNano = System.nanoTime() - lastRefreshTimeNano;
    if (providerCache == null
        || cacheTimeout == 0
        || timeoutUnit == null
        || timeSinceLastRefreshNano > timeoutUnit.toNanos(cacheTimeout)) {
      return refreshProvider();
    } else {
      return providerCache;
    }
  }

  private static AwsCredentialsProvider refreshProvider() {
    AwsCredentialsProvider provider;
    if (customSupplier != null) {
      provider = customSupplier.get();
    } else {
      provider = getDefaultProvider();
    }

    if (cacheTimeout != 0 && timeoutUnit != null) {
      lastRefreshTimeNano = System.nanoTime();
      providerCache = provider;
    }
    return provider;
  }

  private static AwsCredentialsProvider getDefaultProvider() {
    return DefaultCredentialsProvider.create();
  }
}
