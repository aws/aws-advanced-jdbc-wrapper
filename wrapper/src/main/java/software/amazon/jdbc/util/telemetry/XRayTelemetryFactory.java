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

public class XRayTelemetryFactory implements TelemetryFactory {

  @Override
  public TelemetryContext openTelemetryContext(
      final String name,
      final TelemetryTraceLevel traceLevel) {
    return new XRayTelemetryContext(name, traceLevel);
  }

  @Override
  public void postCopy(TelemetryContext telemetryContext, TelemetryTraceLevel traceLevel) {
    if (telemetryContext instanceof XRayTelemetryContext) {
      XRayTelemetryContext.postCopy((XRayTelemetryContext) telemetryContext, traceLevel);
    } else {
      throw new RuntimeException("Wrong parameter type: " + telemetryContext.getClass().getName());
    }
  }

  @Override
  public TelemetryCounter createCounter(String name) {
    throw new RuntimeException("XRay doesn't support metrics.");
  }

  @Override
  public TelemetryGauge createGauge(String name, GaugeCallable<Long> callback) {
    throw new RuntimeException("XRay doesn't support metrics.");
  }

  public boolean isEnabled() {
    return true;
  }
}
