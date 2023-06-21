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

  private static OpenTelemetry openTelemetry;
  private static Tracer tracer;
  private static Meter meter;

  private static final Object mutex = new Object();

  private static OpenTelemetry getOpenTelemetry() {
    if (openTelemetry == null) {
      synchronized (mutex) {
        if (openTelemetry == null) {
          openTelemetry = GlobalOpenTelemetry.get();
        }
      }
    }
    return openTelemetry;
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
    meter = getOpenTelemetry().getMeter(INSTRUMENTATION_NAME);
    return new OpenTelemetryCounter(meter, name);
  }

  public TelemetryGauge createGauge(String name, GaugeCallable<Long> callback) {
    meter = getOpenTelemetry().getMeter(INSTRUMENTATION_NAME);
    return new OpenTelemetryGauge(meter, name, callback);
  }

}
