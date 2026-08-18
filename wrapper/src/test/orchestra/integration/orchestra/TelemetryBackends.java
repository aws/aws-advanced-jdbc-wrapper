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

import integration.TestEnvironmentFeatures;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Set;
import software.amazon.orchestra.EnvCompositionBuilder;
import software.amazon.orchestra.instruments.docker.OtlpCollectorInstrumentDefinition;
import software.amazon.orchestra.instruments.docker.XRayDaemonInstrumentDefinition;

/**
 * The telemetry backends a run provisions, from {@code -Dorchestra-telemetry}.
 *
 * <p>Shared by both runners rather than private to each, like {@code JvmVariation.requested()}: the property
 * is read in two places - to decide which features the shape publishes, and to decide which instruments join
 * the composition - and in two runners. Four copies of a switch statement is four chances for a composition
 * to provision a backend it does not declare, which the in-container side reads as a missing endpoint.
 *
 * <h2>Why this defaults to on</h2>
 *
 * <p>The only capability here that does, because it is the only one the harness has on by default and the
 * only one whose coverage is not a suite of its own. The harness's switches are
 * {@code test-no-traces-telemetry} and {@code test-no-metrics-telemetry}, both defaulting to false, and no CI
 * workflow passes either - so every legacy integration run connects with {@code telemetryTracesBackend=xray}
 * and {@code telemetryMetricsBackend=otlp} and opens an X-Ray segment around every test. Nothing asserts on
 * the telemetry that results; the coverage is that all ~250 tests exercise the wrapper's real telemetry
 * plugins rather than no-op ones. Defaulting this off would have retired the harness while quietly dropping
 * that from the whole suite - a gap that stays invisible because everything still passes.
 *
 * <p>Affordable in a way the matrix axes are not: two local containers, no AWS resource, nothing exported to
 * the account, and one composition rather than more slots. Traces and metrics are independent capabilities
 * rather than alternatives, so they join the same composition instead of multiplying it.
 */
public final class TelemetryBackends {

  private TelemetryBackends() {
  }

  /**
   * Returns the telemetry features this run asked for.
   *
   * <p>A list of what to include, like the engine, driver and JVM axes, rather than the harness's pair of
   * negations: {@code -Dorchestra-telemetry=none} turns both off, {@code =traces} or {@code =metrics} keeps
   * one.
   *
   * @return the features to publish, possibly empty
   * @throws IllegalArgumentException if a token is not a backend this task provisions
   */
  public static EnumSet<TestEnvironmentFeatures> requested() {
    final String requested = System.getProperty("orchestra-telemetry", "traces,metrics").trim();
    final EnumSet<TestEnvironmentFeatures> enabled = EnumSet.noneOf(TestEnvironmentFeatures.class);

    if (requested.isEmpty() || "none".equalsIgnoreCase(requested)) {
      return enabled;
    }

    for (final String name : requested.split(",")) {
      final String token = name.trim().toLowerCase(Locale.ROOT);
      if (token.isEmpty()) {
        continue;
      }

      switch (token) {
        case "traces":
          enabled.add(TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED);
          break;
        case "metrics":
          enabled.add(TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED);
          break;
        default:
          throw new IllegalArgumentException(
              "orchestra-telemetry=" + requested + " names '" + token + "', which is not a telemetry "
                  + "backend this task provisions. Supported: traces, metrics, or none to disable both.");
      }
    }
    return enabled;
  }

  /**
   * Adds a backend for each telemetry feature the composition declares.
   *
   * <p>Driven by the feature set rather than by re-reading the property, so a composition cannot declare
   * telemetry it did not provision. That failure would surface in the container as "the composition declares
   * traces telemetry but its backend published no reachable endpoint", one layer away from the cause.
   *
   * <p>Bound per purpose rather than to Orchestra's single {@code TelemetryBackend} role, because the harness
   * publishes traces and metrics separately and a run can have one without the other. See
   * {@link TelemetryRoles}.
   *
   * <p>Two containers where the harness runs one. The harness sets {@code USE_OTLP_CONTAINER_FOR_TRACES} and
   * gives its collector an {@code awsxray} receiver on 2000, so segments and metrics both arrive there; its
   * X-Ray daemon branch is dead code. The daemon is what Orchestra ships for traces, and the difference is
   * invisible to the container, which either way sees a UDP X-Ray endpoint on 2000 and an OTLP endpoint on
   * 4317 - the same two values that dead branch names.
   *
   * @param builder the composition being assembled
   * @param features the features this run publishes
   */
  public static void addTo(
      final EnvCompositionBuilder builder, final Set<TestEnvironmentFeatures> features) {

    if (features.contains(TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED)) {
      builder.addInstrument(TelemetryRoles.Traces.class, new XRayDaemonInstrumentDefinition());
    }

    if (features.contains(TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED)) {
      builder.addInstrument(TelemetryRoles.Metrics.class, new OtlpCollectorInstrumentDefinition());
    }
  }
}
