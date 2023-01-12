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


public class AwsCredentialsService {
  static Supplier<AwsCredentialsProvider> customSupplier = null;
  static int cacheTimeout = 0;
  static TimeUnit timeoutUnit;
  static long lastRefreshTimeNano;
  static AwsCredentialsProvider PROVIDER_CACHE = null;

  public static void setCustomSupplier(Supplier<AwsCredentialsProvider> customSupplier) {
    AwsCredentialsService.customSupplier = customSupplier;
    clearCache();
  }

  public static void setCustomSupplier(Supplier<AwsCredentialsProvider> customSupplier, int timeout,
      TimeUnit timeoutUnits) {
    AwsCredentialsService.customSupplier = customSupplier;
    AwsCredentialsService.cacheTimeout = timeout;
    AwsCredentialsService.timeoutUnit = timeoutUnits;
    PROVIDER_CACHE = null;
  }

  public static void resetCustomSupplier() {
    customSupplier = null;
    clearCache();
  }

  private static void clearCache() {
    cacheTimeout = 0;
    timeoutUnit = null;
    PROVIDER_CACHE = null;
  }

  public static AwsCredentialsProvider getProvider() {
    if (PROVIDER_CACHE == null || cacheTimeout == 0 || timeoutUnit == null) {
      return refreshProvider();
    }

    long timeSinceLastRefreshNano = System.nanoTime() - lastRefreshTimeNano;
    if (timeSinceLastRefreshNano >= timeoutUnit.toNanos(cacheTimeout)) {
      return refreshProvider();
    } else {
      return PROVIDER_CACHE;
    }
  }

  private static AwsCredentialsProvider refreshProvider() {
    AwsCredentialsProvider provider;
    if (customSupplier != null) {
      provider = customSupplier.get();
    } else {
      provider = getDefaultProvider();
    }

    lastRefreshTimeNano = System.nanoTime();
    PROVIDER_CACHE = provider;
    return provider;
  }

  private static AwsCredentialsProvider getDefaultProvider() {
    return DefaultCredentialsProvider.create();
  }
}
