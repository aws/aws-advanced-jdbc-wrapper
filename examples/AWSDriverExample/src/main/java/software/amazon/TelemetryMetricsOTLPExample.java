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

package software.amazon;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import software.amazon.jdbc.PropertyDefinition;

public class TelemetryMetricsOTLPExample {

  // User configures connection properties here
  public static final String POSTGRESQL_CONNECTION_STRING =
      "jdbc:aws-wrapper:postgresql://atlas-postgres.cluster-czygpppufgy4.us-east-2.rds.amazonaws.com:5432/postgres";

  private static final String SQL_DBLIST = "select datname from pg_database;";
  private static final String SQL_SLEEP = "select pg_sleep(20);";
  private static final String SQL_TABLELIST = "select * from information_schema.tables where table_schema='public';";

  private final OpenTelemetry openTelemetry;
  private final OtlpGrpcSpanExporter spanExporter;
  private final OtlpGrpcMetricExporter metricExporter;
  private final SdkTracerProvider tracerProvider;
  private final SdkMeterProvider meterProvider;

  public TelemetryMetricsOTLPExample() {
    spanExporter = OtlpGrpcSpanExporter.builder().setEndpoint(System.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")).build();
    metricExporter = OtlpGrpcMetricExporter.builder().setEndpoint(System.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")).build();

    tracerProvider = SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(spanExporter)).build();
    meterProvider = SdkMeterProvider.builder()
        .registerMetricReader(PeriodicMetricReader.builder(metricExporter).setInterval(15, TimeUnit.SECONDS).build())
        .build();

    openTelemetry = OpenTelemetrySdk.builder()
        .setTracerProvider(tracerProvider)
        .setMeterProvider(meterProvider)
        .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
        .buildAndRegisterGlobal();
  }

  public void runExampleQuery(Properties properties) throws SQLException {
    try (final Connection conn = DriverManager.getConnection(POSTGRESQL_CONNECTION_STRING, properties);
         final Statement statement = conn.createStatement();
         final ResultSet rs = statement.executeQuery(SQL_SLEEP)) {
      System.out.println(Util.getResult(rs));
    }
  }

  public static void main(String[] args) throws SQLException {
    final TelemetryTracingXRayExample example = new TelemetryTracingXRayExample();

    final Properties properties = new Properties();
    properties.setProperty(PropertyDefinition.PLUGINS.name, "dataCache, efm, failover");
    properties.setProperty(PropertyDefinition.USER.name, MetricsUtil.USERNAME);
    properties.setProperty(PropertyDefinition.PASSWORD.name, MetricsUtil.PASSWORD);

    properties.setProperty(PropertyDefinition.ENABLE_TELEMETRY.name, String.valueOf(true));
    properties.setProperty(PropertyDefinition.TELEMETRY_SUBMIT_TOPLEVEL.name, String.valueOf(true));
    // Traces: Available values are XRAY, OTLP and NONE
    properties.setProperty(PropertyDefinition.TELEMETRY_TRACES_BACKEND.name, "NONE");
    // Metrics: Available values are OTLP and NONE
    properties.setProperty(PropertyDefinition.TELEMETRY_METRICS_BACKEND.name, "OTLP");

    System.out.println("-- starting metrics e2e test");

    System.out.println("-- env vars");
    System.out.println("AWS_REGION: " + System.getenv("AWS_REGION"));
    System.out.println("OTEL_METRICS_EXPORTER: " + System.getenv("OTEL_METRICS_EXPORTER"));
    System.out.println("OTEL_TRACES_EXPORTER: " + System.getenv("OTEL_TRACES_EXPORTER"));
    System.out.println("OTEL_LOGS_EXPORTER: " + System.getenv("OTEL_LOGS_EXPORTER"));
    System.out.println("OTEL_EXPORTER_OTLP_ENDPOINT: " + System.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"));
    System.out.println("OTEL_RESOURCE_ATTRIBUTES: " + System.getenv("OTEL_RESOURCE_ATTRIBUTES"));

    System.out.println("-- running application");
    example.runExampleQuery1(properties);
    System.out.println("-- end of application");
  }
}
