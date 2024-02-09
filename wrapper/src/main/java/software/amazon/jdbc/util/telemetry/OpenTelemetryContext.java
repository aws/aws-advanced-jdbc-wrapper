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

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.trace.ReadableSpan;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class OpenTelemetryContext implements TelemetryContext {

  private static final Logger LOGGER = Logger.getLogger(OpenTelemetryContext.class.getName());

  private Span span;

  private Scope scope;
  private final String name;

  private final Tracer tracer;

  public OpenTelemetryContext(
      final Tracer tracer,
      final String name,
      final TelemetryTraceLevel traceLevel) {
    this(tracer, name, traceLevel, getEpochNanos(Instant.now()));
  }

  private OpenTelemetryContext(
      final Tracer tracer,
      final String name,
      final TelemetryTraceLevel traceLevel,
      final long startTimeNanos) {

    this.name = name;
    this.tracer = tracer;

    boolean isRoot = Context.current() == Context.root();

    TelemetryTraceLevel effectiveTraceLevel = traceLevel;
    if (isRoot && traceLevel == TelemetryTraceLevel.NESTED) {
      effectiveTraceLevel = TelemetryTraceLevel.NO_TRACE;
    }

    switch (effectiveTraceLevel) {
      case FORCE_TOP_LEVEL:
      case TOP_LEVEL:
        span = this.tracer.spanBuilder(name)
            .setNoParent()
            .setStartTimestamp(startTimeNanos, TimeUnit.NANOSECONDS)
            .startSpan();
        if (!isRoot) {
          this.setAttribute(
              TelemetryConst.PARENT_TRACE_ANNOTATION,
              span.getSpanContext().getTraceId());
        }
        this.setAttribute(TelemetryConst.TRACE_NAME_ANNOTATION, name);
        scope = span.makeCurrent();
        LOGGER.finest(() -> String.format(
            "[OTLP] Telemetry '%s' trace ID: %s", name, span.getSpanContext().getTraceId()));

        break;

      case NESTED:
        span = this.tracer.spanBuilder(name)
            .setStartTimestamp(startTimeNanos, TimeUnit.NANOSECONDS)
            .startSpan();
        this.setAttribute(TelemetryConst.TRACE_NAME_ANNOTATION, name);
        scope = span.makeCurrent();
        break;

      case NO_TRACE:
        // do not post this context
        break;

      default:
        break;
    }
  }

  @Override
  public void setSuccess(boolean success) {
    if (span != null) {
      if (success) {
        span.setStatus(StatusCode.OK);
      } else {
        span.setStatus(StatusCode.ERROR);
      }
    }
  }

  @Override
  public void setAttribute(String key, String value) {
    if (span != null) {
      span.setAttribute(key, value);
    }
  }

  @Override
  public void setException(Exception exception) {
    if (span != null && exception != null) {
      span.setAttribute(
          TelemetryConst.EXCEPTION_TYPE_ANNOTATION,
          exception.getClass().getSimpleName());
      span.setAttribute(
          TelemetryConst.EXCEPTION_MESSAGE_ANNOTATION,
          exception.getMessage());
      span.recordException(exception);
    }
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return super.toString() + " " + (span == null ? "no span" : span.toString());
  }

  public static void postCopy(
      final OpenTelemetryContext telemetryContext,
      final TelemetryTraceLevel traceLevel) {

    if (traceLevel == TelemetryTraceLevel.NO_TRACE) {
      return;
    }

    if (traceLevel == TelemetryTraceLevel.FORCE_TOP_LEVEL
        || traceLevel == TelemetryTraceLevel.TOP_LEVEL) {

      // post a copy context in a separate lambda/thread, so it's not connected to a current context.
      CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
        OpenTelemetryContext copy = clone(telemetryContext, traceLevel);
        copy.closeContext();
      });

      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }

    } else {

      // Post a copy context in the same thread and in the same context.
      OpenTelemetryContext copy = clone(telemetryContext, traceLevel);
      copy.closeContext();
    }
  }

  private static OpenTelemetryContext clone(
      final OpenTelemetryContext telemetryContext,
      final TelemetryTraceLevel traceLevel) {

    if (!(telemetryContext.span instanceof ReadableSpan)) {
      // can't get required data from the span
      throw new RuntimeException("Can't use this telemetry context to make a copy.");
    }

    ReadableSpan readableSpan = (ReadableSpan) telemetryContext.span;
    long startTime = readableSpan.toSpanData().getStartEpochNanos();

    OpenTelemetryContext copy = new OpenTelemetryContext(
        telemetryContext.tracer,
        TelemetryConst.COPY_TRACE_NAME_PREFIX + telemetryContext.getName(),
        traceLevel,
        startTime);

    Map<AttributeKey<?>, Object> attributes = readableSpan.toSpanData().getAttributes().asMap();
    for (Map.Entry<AttributeKey<?>, Object> entry : attributes.entrySet()) {
      if (entry.getValue() != null && !TelemetryConst.TRACE_NAME_ANNOTATION.equals(entry.getKey().getKey())) {
        copy.setAttribute(entry.getKey().getKey(), entry.getValue().toString());
      }
    }
    copy.span.setStatus(readableSpan.toSpanData().getStatus().getStatusCode());
    copy.setAttribute(
        TelemetryConst.SOURCE_TRACE_ANNOTATION,
        telemetryContext.span.getSpanContext().getTraceId());

    return copy;
  }

  private static long getEpochNanos(Instant instant) {
    return TimeUnit.SECONDS.toNanos(instant.getEpochSecond()) + instant.getNano();
  }

  @Override
  public void closeContext() {
    if (span != null) {
      span.end();
    }
    if (scope != null) {
      scope.close();
    }
  }

}
