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

public class NullTelemetryFactory implements TelemetryFactory {

  private static final TelemetryContext NULL_TELEMETRY_CONTEXT = new NullTelemetryContext("null");
  private static final TelemetryCounter NULL_TELEMETRY_COUNTER = new NullTelemetryCounter("null");
  private static final TelemetryGauge NULL_TELEMETRY_GAUGE = new NullTelemetryGauge("null");

  @Override
  public TelemetryContext openTelemetryContext(String name, TelemetryTraceLevel traceLevel) {
    return NULL_TELEMETRY_CONTEXT;
  }

  @Override
  public void postCopy(TelemetryContext telemetryContext, TelemetryTraceLevel traceLevel) {
    // do nothing
  }

  @Override
  public TelemetryCounter createCounter(String name) {
    return NULL_TELEMETRY_COUNTER;
  }

  @Override
  public TelemetryGauge createGauge(String name, GaugeCallable<Long> callback) {
    return NULL_TELEMETRY_GAUGE;
  }
}
