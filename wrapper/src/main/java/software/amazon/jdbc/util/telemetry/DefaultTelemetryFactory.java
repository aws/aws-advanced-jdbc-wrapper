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

  private static final OpenTelemetryFactory OPEN_TELEMETRY_FACTORY = new OpenTelemetryFactory();
  private static final XRayTelemetryFactory X_RAY_TELEMETRY_FACTORY = new XRayTelemetryFactory();

  private final boolean enableTelemetry;
  private final String telemetryTracesBackend;
  private final String telemetryMetricsBackend;
  private final boolean telemetrySubmitTopLevel;

  private final TelemetryFactory tracesTelemetryFactory;
  private final TelemetryFactory metricsTelemetryFactory;
  private final boolean telemetryInUse;

  public DefaultTelemetryFactory(final Properties properties) {
    this.enableTelemetry = PropertyDefinition.ENABLE_TELEMETRY.getBoolean(properties);
    this.telemetryTracesBackend = PropertyDefinition.TELEMETRY_TRACES_BACKEND.getString(properties);
    this.telemetryMetricsBackend =
        PropertyDefinition.TELEMETRY_METRICS_BACKEND.getString(properties);
    this.telemetrySubmitTopLevel =
        PropertyDefinition.TELEMETRY_SUBMIT_TOPLEVEL.getBoolean(properties);

    if (enableTelemetry) {
      if ("otlp".equalsIgnoreCase(telemetryTracesBackend)) {
        this.tracesTelemetryFactory = OPEN_TELEMETRY_FACTORY;
      } else if ("xray".equalsIgnoreCase(telemetryTracesBackend)) {
        this.tracesTelemetryFactory = X_RAY_TELEMETRY_FACTORY;
      } else if ("none".equalsIgnoreCase(telemetryTracesBackend)) {
        this.tracesTelemetryFactory = null;
      } else {
        throw new RuntimeException(
            telemetryTracesBackend
                + " is not a valid tracing backend. Available options: OTLP, XRAY, NONE.");
      }
    } else {
      this.tracesTelemetryFactory = null;
    }

    if (enableTelemetry) {
      if ("otlp".equalsIgnoreCase(telemetryMetricsBackend)) {
        this.metricsTelemetryFactory = OPEN_TELEMETRY_FACTORY;
      } else if ("none".equalsIgnoreCase(telemetryMetricsBackend)) {
        this.metricsTelemetryFactory = null;
      } else {
        throw new RuntimeException(
            telemetryMetricsBackend
                + " is not a valid metrics backend. Available options: OTLP, NONE.");
      }
    } else {
      this.metricsTelemetryFactory = null;
    }

    this.telemetryInUse =
        this.tracesTelemetryFactory != null || this.metricsTelemetryFactory != null;
  }

  @Override
  public TelemetryContext openTelemetryContext(
      final String name, final TelemetryTraceLevel traceLevel) {
    if (this.tracesTelemetryFactory == null) {
      return null;
    }
    TelemetryTraceLevel effectiveTraceLevel = traceLevel;
    if (!this.telemetrySubmitTopLevel && traceLevel == TelemetryTraceLevel.TOP_LEVEL) {
      effectiveTraceLevel = TelemetryTraceLevel.NESTED;
    }
    return this.tracesTelemetryFactory.openTelemetryContext(name, effectiveTraceLevel);
  }

  @Override
  public void postCopy(TelemetryContext telemetryContext, TelemetryTraceLevel traceLevel) {
    if (this.tracesTelemetryFactory != null) {
      this.tracesTelemetryFactory.postCopy(telemetryContext, traceLevel);
    }
  }

  @Override
  public TelemetryCounter createCounter(final String name) {
    if (this.metricsTelemetryFactory == null) {
      return null;
    }
    return this.metricsTelemetryFactory.createCounter(name);
  }

  @Override
  public TelemetryGauge createGauge(final String name, final GaugeCallable<Long> callback) {
    if (this.metricsTelemetryFactory == null) {
      return null;
    }
    return this.metricsTelemetryFactory.createGauge(name, callback);
  }

  @Override
  public boolean inUse() {
    return this.telemetryInUse;
  }
}
