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

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import java.time.Instant;

public class OpenTelemetryContext implements TelemetryContext {

  private Span span;

  private Scope scope;
  private final String name;

  private final Tracer tracer;

  public OpenTelemetryContext(Tracer tracer, String name) {
    this.name = name;
    this.tracer = tracer;

    span = this.tracer.spanBuilder(name).setStartTimestamp(Instant.now()).startSpan();

    scope = span.makeCurrent();
  }

  @Override
  public void setSuccess(boolean success) {
    if (success) {
      span.setStatus(StatusCode.OK);
    } else {
      span.setStatus(StatusCode.ERROR);
    }
  }

  @Override
  public void setAttribute(String key, String value) {
    span.setAttribute(key, value);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void closeContext() {
    if (span != null) {
      span.end();
      span = null;
    }
    if (scope != null) {
      scope.close();
      scope = null;
    }
  }

  @Override
  public String toString() {
    return span.toString();
  }

  @Override
  public void close() {
    closeContext();
  }
}
