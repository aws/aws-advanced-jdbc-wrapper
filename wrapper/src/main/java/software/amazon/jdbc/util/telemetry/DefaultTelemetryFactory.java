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

import java.util.Properties;
import software.amazon.jdbc.PropertyDefinition;

public class DefaultTelemetryFactory implements TelemetryFactory {

  private final boolean enableTelemetry;
  private final String telemetryTracesBackend;
  private final String telemetryMetricsBackend;
  private final boolean telemetrySubmitToplevel;

  private final TelemetryFactory tracesTelemetryFactory;
  private final TelemetryFactory metricsTelemetryFactory;

  public DefaultTelemetryFactory(final Properties properties) {
    this.enableTelemetry = PropertyDefinition.ENABLE_TELEMETRY.getBoolean(properties);
    this.telemetryTracesBackend = PropertyDefinition.TELEMETRY_TRACES_BACKEND.getString(properties);
    this.telemetryMetricsBackend = PropertyDefinition.TELEMETRY_METRICS_BACKEND.getString(properties);
    this.telemetrySubmitToplevel = PropertyDefinition.TELEMETRY_SUBMIT_TOPLEVEL.getBoolean(properties);

    if (enableTelemetry) {

      if ("otlp".equalsIgnoreCase(telemetryTracesBackend)) {
        this.tracesTelemetryFactory = new OpenTelemetryFactory();
      } else if ("xray".equalsIgnoreCase(telemetryTracesBackend)) {
        this.tracesTelemetryFactory = new XRayTelemetryFactory();
      } else if ("none".equalsIgnoreCase(telemetryTracesBackend)) {
        this.tracesTelemetryFactory = new NullTelemetryFactory();
      } else {
        throw new RuntimeException(
            telemetryTracesBackend + " is not a valid tracing backend. Available options: OTLP, XRAY, NONE.");
      }
    } else {
      this.tracesTelemetryFactory = new NullTelemetryFactory();
    }

    if (enableTelemetry) {
      if ("otlp".equalsIgnoreCase(telemetryMetricsBackend)) {
        this.metricsTelemetryFactory = new OpenTelemetryFactory();
      } else if ("none".equalsIgnoreCase(telemetryMetricsBackend)) {
        this.metricsTelemetryFactory = new NullTelemetryFactory();
      } else {
        throw new RuntimeException(
            telemetryTracesBackend + " is not a valid metrics backend. Available options: OTLP, NONE.");
      }
    } else {
      this.metricsTelemetryFactory = new NullTelemetryFactory();
    }
  }

  @Override
  public TelemetryContext openTelemetryContext(final String name, final TelemetryTraceLevel traceLevel) {
    TelemetryTraceLevel effectiveTraceLevel = traceLevel;
    if (!this.telemetrySubmitToplevel && traceLevel == TelemetryTraceLevel.TOP_LEVEL) {
      effectiveTraceLevel = TelemetryTraceLevel.NESTED;
    }
    return this.tracesTelemetryFactory.openTelemetryContext(name, effectiveTraceLevel);
  }

  @Override
  public void postCopy(TelemetryContext telemetryContext, TelemetryTraceLevel traceLevel) {
    this.tracesTelemetryFactory.postCopy(telemetryContext, traceLevel);
  }

  @Override
  public TelemetryCounter createCounter(final String name) {
    return this.metricsTelemetryFactory.createCounter(name);
  }

  @Override
  public TelemetryGauge createGauge(final String name, final GaugeCallable<Long> callback) {
    return this.metricsTelemetryFactory.createGauge(name, callback);
  }
}
