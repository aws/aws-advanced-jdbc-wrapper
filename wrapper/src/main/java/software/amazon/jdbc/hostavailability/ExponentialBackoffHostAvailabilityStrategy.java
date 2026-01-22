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

package software.amazon.jdbc.hostavailability;

import static software.amazon.jdbc.hostavailability.HostAvailabilityStrategyFactory.HOST_AVAILABILITY_STRATEGY_INITIAL_BACKOFF_TIME;
import static software.amazon.jdbc.hostavailability.HostAvailabilityStrategyFactory.HOST_AVAILABILITY_STRATEGY_MAX_RETRIES;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Properties;
import software.amazon.jdbc.util.Messages;

public class ExponentialBackoffHostAvailabilityStrategy implements HostAvailabilityStrategy {

  public static String NAME = "exponentialBackoff";
  private int maxRetries = 5;
  private int initialBackoffTimeSeconds = 30;
  private int notAvailableCount = 0;
  private Timestamp lastChanged;

  public ExponentialBackoffHostAvailabilityStrategy(Properties props) {
    if (HOST_AVAILABILITY_STRATEGY_MAX_RETRIES.getInteger(props) < 1) {
      throw new IllegalArgumentException(Messages.get("HostAvailabilityStrategy.invalidMaxRetries",
          new Object[] {HOST_AVAILABILITY_STRATEGY_MAX_RETRIES.getInteger(props)}));
    }
    this.maxRetries = HOST_AVAILABILITY_STRATEGY_MAX_RETRIES.getInteger(props);

    if (HOST_AVAILABILITY_STRATEGY_INITIAL_BACKOFF_TIME.getInteger(props) < 1) {
      throw new IllegalArgumentException(Messages.get("HostAvailabilityStrategy.invalidInitialBackoffTime",
          new Object[] {HOST_AVAILABILITY_STRATEGY_INITIAL_BACKOFF_TIME.getInteger(props)}));
    }
    this.initialBackoffTimeSeconds = HOST_AVAILABILITY_STRATEGY_INITIAL_BACKOFF_TIME.getInteger(props);

    lastChanged = Timestamp.from(Instant.now());
  }

  @Override
  public void setHostAvailability(HostAvailability hostAvailability) {
    this.lastChanged = Timestamp.from(Instant.now());
    if (hostAvailability == HostAvailability.AVAILABLE) {
      this.notAvailableCount = 0;
    } else {
      this.notAvailableCount++;
    }
  }

  @Override
  public HostAvailability getHostAvailability(HostAvailability rawHostAvailability) {
    if (rawHostAvailability == HostAvailability.AVAILABLE) {
      return HostAvailability.AVAILABLE;
    }

    if (notAvailableCount >= maxRetries) {
      return HostAvailability.NOT_AVAILABLE;
    }

    final double retryDelayMillis = Math.pow(2, notAvailableCount) * initialBackoffTimeSeconds * 1000;
    final Timestamp earliestRetry = new Timestamp(lastChanged.getTime() + Math.round(retryDelayMillis));
    final Timestamp now = Timestamp.from(Instant.now());
    if (earliestRetry.before(now)) {
      return HostAvailability.AVAILABLE;
    }

    return rawHostAvailability;
  }
}
