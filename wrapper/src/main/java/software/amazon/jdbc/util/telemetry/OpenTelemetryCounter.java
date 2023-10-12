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

package software.amazon.jdbc.util.telemetry;

import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;

public class OpenTelemetryCounter implements TelemetryCounter {

  private final LongCounter counter;

  private final String name;

  private final Meter meter;

  OpenTelemetryCounter(Meter meter, String name) {
    this.name = name;
    this.meter = meter;

    counter = this.meter.counterBuilder(name).build();
  }

  @Override
  public void add(long value) {
    counter.add(value);
  }

  @Override
  public void inc() {
    this.add(1);
  }

  public String getName() {
    return name;
  }

}
