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

package integration.orchestra;

import software.amazon.orchestra.contract.TelemetryBackend;

/**
 * Role tokens for the two telemetry backends the wrapper's plugins export to.
 *
 * <p>Two rather than one because the harness publishes them separately -
 * {@code setTracesTelemetryInfo} and {@code setMetricsTelemetryInfo} - and a test can have one without the
 * other: {@code TELEMETRY_TRACES_ENABLED} and {@code TELEMETRY_METRICS_ENABLED} are independent features.
 * Orchestra's own {@code TelemetryBackend} is a single role, so a composition carrying both needs a token
 * per purpose, exactly as the four caches do.
 *
 * <p>Named for what a backend carries rather than for which product it is, which is the distinction
 * {@code TelemetryBackendState.kind()} already makes. A role called {@code XRay} would bake today's pairing
 * into every lookup, and the pairing is not fixed: the harness sends X-Ray segments to its OTLP collector
 * rather than to a daemon, and Orchestra ships both instruments precisely so the choice can vary. The
 * in-container code asks for "where do traces go", so that is what the role says.
 *
 * <p>Both extend Orchestra's {@code TelemetryBackend}, so anything acting on every telemetry backend in a
 * composition without caring which is which still sees them.
 *
 * <p>In the {@code test} source set rather than {@code orchestraTest} for the same reason as
 * {@link CacheRoles}: the context identifies roles by fully-qualified class name, so the host that binds a
 * role and the in-container code that looks it up have to name the same token.
 */
public final class TelemetryRoles {

  private TelemetryRoles() {
  }

  /** Where the wrapper's traces go, read by {@code TestEnvironmentInfo.getTracesTelemetryInfo}. */
  public interface Traces extends TelemetryBackend {
  }

  /** Where the wrapper's metrics go, read by {@code TestEnvironmentInfo.getMetricsTelemetryInfo}. */
  public interface Metrics extends TelemetryBackend {
  }
}
