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

import com.amazonaws.xray.AWSXRay;
import com.amazonaws.xray.entities.Entity;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

public class XRayTelemetryContext implements TelemetryContext {

  private static final Logger LOGGER = Logger.getLogger(XRayTelemetryContext.class.getName());

  private Entity traceEntity;
  private final String name;

  public XRayTelemetryContext(final String name, final TelemetryTraceLevel traceLevel) {
    this.name = name;

    Entity parentEntity = AWSXRay.getTraceEntity();

    TelemetryTraceLevel effectiveTraceLevel = traceLevel;
    if (parentEntity == null && traceLevel == TelemetryTraceLevel.NESTED) {
      effectiveTraceLevel = TelemetryTraceLevel.NO_TRACE;
    }

    switch (effectiveTraceLevel) {
      case FORCE_TOP_LEVEL:
      case TOP_LEVEL:
        this.traceEntity = AWSXRay.beginSegment(name);
        if (parentEntity != null) {
          this.setAttribute(TelemetryConst.PARENT_TRACE_ANNOTATION, parentEntity.getTraceId().toString());
        }
        this.setAttribute(TelemetryConst.TRACE_NAME_ANNOTATION, name);
        LOGGER.finest(() -> String.format("[XRay] Telemetry '%s' trace ID: %s", name, this.traceEntity.getTraceId()));
        break;

      case NESTED:
        this.traceEntity = AWSXRay.beginSubsegment(name);
        this.setAttribute(TelemetryConst.TRACE_NAME_ANNOTATION, name);
        break;

      case NO_TRACE:
        // do not post this trace
        break;

      default:
        break;
    }
  }

  public static void postCopy(
      final XRayTelemetryContext telemetryContext,
      final TelemetryTraceLevel traceLevel) {

    if (traceLevel == TelemetryTraceLevel.NO_TRACE) {
      return;
    }

    if (traceLevel == TelemetryTraceLevel.FORCE_TOP_LEVEL
        || traceLevel == TelemetryTraceLevel.TOP_LEVEL) {

      // post a copy context in a separate lambda/thread, so it's not connected to a current context.
      CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
        XRayTelemetryContext copy = clone(telemetryContext, traceLevel);
        copy.closeContext();
      });

      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }

    } else {

      // post a copy context in the same thread and in the same context.
      XRayTelemetryContext copy = clone(telemetryContext, traceLevel);
      copy.closeContext();
    }
  }

  private static XRayTelemetryContext clone(
      final XRayTelemetryContext telemetryContext,
      final TelemetryTraceLevel traceLevel) {

    XRayTelemetryContext copy = new XRayTelemetryContext(
        TelemetryConst.COPY_TRACE_NAME_PREFIX + telemetryContext.getName(), traceLevel);

    copy.traceEntity.setStartTime(telemetryContext.traceEntity.getStartTime());
    copy.traceEntity.setEndTime(telemetryContext.traceEntity.getEndTime());

    Map<String, Object> annotations = telemetryContext.traceEntity.getAnnotations();
    for (Map.Entry<String, Object> entry : annotations.entrySet()) {
      if (entry.getValue() != null && !TelemetryConst.TRACE_NAME_ANNOTATION.equals(entry.getKey())) {
        copy.setAttribute(entry.getKey(), entry.getValue().toString());
      }
    }
    copy.traceEntity.setError(telemetryContext.traceEntity.isError());
    copy.setAttribute(
        TelemetryConst.SOURCE_TRACE_ANNOTATION,
        telemetryContext.traceEntity.getTraceId().toString());

    if (telemetryContext.traceEntity.getParent() != null) {
      if (traceLevel == TelemetryTraceLevel.NESTED) {
        copy.traceEntity.setParent(telemetryContext.traceEntity.getParent());
      }
      if (traceLevel == TelemetryTraceLevel.FORCE_TOP_LEVEL
          || traceLevel == TelemetryTraceLevel.TOP_LEVEL) {
        copy.setAttribute(
            TelemetryConst.PARENT_TRACE_ANNOTATION,
            telemetryContext.traceEntity.getParent().getTraceId().toString());
      }
    }

    return copy;
  }

  @Override
  public void setSuccess(boolean success) {
    if (this.traceEntity != null) {
      this.traceEntity.setError(!success);
    }
  }

  @Override
  public void setAttribute(String key, String value) {
    if (this.traceEntity != null) {
      this.traceEntity.putAnnotation(key, value);
    }
  }

  @Override
  public void setException(Exception exception) {
    if (this.traceEntity != null && exception != null) {
      setAttribute("exceptionType", exception.getClass().getSimpleName());
      if (exception.getMessage() != null) {
        setAttribute("exceptionMessage", exception.getMessage());
      }
    }
  }

  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public String toString() {
    return this.traceEntity == null ? "null" : this.traceEntity.getId();
  }

  @Override
  public void closeContext() {
    try {
      if (this.traceEntity != null) {
        this.traceEntity.close();
      }
    } catch (Exception e) {
      // Ignore
    }
  }

}
