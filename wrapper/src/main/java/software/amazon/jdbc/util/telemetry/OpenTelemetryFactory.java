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

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Tracer;

public class OpenTelemetryFactory implements TelemetryFactory {

  private static final String INSTRUMENTATION_NAME = "aws-advanced-jdbc-wrapper";

  /**
   * Max allowed name length for counters and gauges.
   *
   * @see
   * <a href="https://opentelemetry.io/docs/specs/otel/metrics/api/#:~:text=It%20can%20have%20a%20maximum%20length%20of%2063%20characters">More details</a>
   */
  private static final int NAME_MAX_LENGTH = 63;

  private static Tracer tracer;
  private static Meter meter;

  private static OpenTelemetry getOpenTelemetry() {
    return GlobalOpenTelemetry.get();
  }

  public TelemetryContext openTelemetryContext(String name, TelemetryTraceLevel traceLevel) {
    tracer = getOpenTelemetry().getTracer(INSTRUMENTATION_NAME);
    return new OpenTelemetryContext(tracer, name, traceLevel);
  }

  @Override
  public void postCopy(TelemetryContext telemetryContext, TelemetryTraceLevel traceLevel) {
    if (telemetryContext instanceof OpenTelemetryContext) {
      OpenTelemetryContext.postCopy((OpenTelemetryContext) telemetryContext, traceLevel);
    } else {
      throw new RuntimeException("Wrong parameter type: " + telemetryContext.getClass().getName());
    }
  }

  public TelemetryCounter createCounter(String name) {
    if (name == null) {
      throw new IllegalArgumentException("name");
    }
    meter = getOpenTelemetry().getMeter(INSTRUMENTATION_NAME);
    return new OpenTelemetryCounter(meter, trimName(name));
  }

  public TelemetryGauge createGauge(String name, GaugeCallable<Long> callback) {
    if (name == null) {
      throw new IllegalArgumentException("name");
    }
    meter = getOpenTelemetry().getMeter(INSTRUMENTATION_NAME);
    return new OpenTelemetryGauge(meter, trimName(name), callback);
  }

  private String trimName(final String name) {
    return (name.length() > NAME_MAX_LENGTH) ? name.substring(0, NAME_MAX_LENGTH) : name;
  }
}
