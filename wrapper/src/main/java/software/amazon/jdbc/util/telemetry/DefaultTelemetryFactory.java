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
import java.util.function.Consumer;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import software.amazon.jdbc.PropertyDefinition;

public class DefaultTelemetryFactory implements TelemetryFactory {
  private final boolean enableTelemetry;
  private final String telemetryTracesBackend;
  private final String telemetryMetricsBackend;
  private final boolean telemetrySubmitToplevel;

  public DefaultTelemetryFactory(Properties properties) {
    this.enableTelemetry = PropertyDefinition.ENABLE_TELEMETRY.getBoolean(properties);
    this.telemetryTracesBackend = PropertyDefinition.TELEMETRY_TRACES_BACKEND.getString(properties);
    this.telemetryMetricsBackend = PropertyDefinition.TELEMETRY_METRICS_BACKEND.getString(properties);
    this.telemetrySubmitToplevel = PropertyDefinition.TELEMETRY_SUBMIT_TOPLEVEL.getBoolean(properties);
  }

  @Override
  public TelemetryContext openTelemetryContext(String name) {
    if (enableTelemetry) {
      if ("otlp".equalsIgnoreCase(telemetryTracesBackend)) {
        return OpenTelemetryFactory.openTelemetryContext(name, telemetrySubmitToplevel);
      } else if ("xray".equalsIgnoreCase(telemetryTracesBackend)) {
        return XRayTelemetryFactory.openTelemetryContext(name, telemetrySubmitToplevel);
      }
    }
    return new NullTelemetryContext(name);
  }

  @Override
  public TelemetryCounter createCounter(String name) {
    if (enableTelemetry) {
      if ("otlp".equalsIgnoreCase(telemetryMetricsBackend)) {
        return OpenTelemetryFactory.createCounter(name);
      }
    }
    return new NullTelemetryCounter(name);
  }

  @Override
  public TelemetryGauge createGauge(String name, Consumer<ObservableLongMeasurement> observation) {
    if (enableTelemetry) {
      if ("otlp".equalsIgnoreCase(telemetryMetricsBackend)) {
        return OpenTelemetryFactory.createGauge(name, observation);
      }
    }
    return new NullTelemetryGauge(name);
  }
}
