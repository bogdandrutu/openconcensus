/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.exporter.otlp;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.Labels;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc.TraceServiceFutureStub;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Exports spans using OTLP via gRPC, using OpenTelemetry's protobuf model.
 *
 * <p>Configuration options for {@link OtlpGrpcSpanExporter} can be read from system properties,
 * environment variables, or {@link java.util.Properties} objects.
 *
 * <p>For system properties and {@link java.util.Properties} objects, {@link OtlpGrpcSpanExporter}
 * will look for the following names:
 *
 * <ul>
 *   <li>{@code otel.exporter.otlp.span.timeout}: to set the max waiting time allowed to send each
 *       span batch.
 *   <li>{@code otel.exporter.otlp.span.endpoint}: to set the endpoint to connect to.
 *   <li>{@code otel.exporter.otlp.span.insecure}: whether to enable client transport security for
 *       the connection.
 *   <li>{@code otel.exporter.otlp.span.headers}: the headers associated with the requests.
 * </ul>
 *
 * <p>For environment variables, {@link OtlpGrpcSpanExporter} will look for the following names:
 *
 * <ul>
 *   <li>{@code OTEL_EXPORTER_OTLP_SPAN_TIMEOUT}: to set the max waiting time allowed to send each
 *       span batch.
 *   <li>{@code OTEL_EXPORTER_OTLP_SPAN_ENDPOINT}: to set the endpoint to connect to.
 *   <li>{@code OTEL_EXPORTER_OTLP_SPAN_INSECURE}: whether to enable client transport security for
 *       the connection.
 *   <li>{@code OTEL_EXPORTER_OTLP_SPAN_HEADERS}: the headers associated with the requests.
 * </ul>
 *
 * <p>In both cases, if a property is missing, the name without "span" is used to resolve the value.
 */
@ThreadSafe
public final class OtlpGrpcSpanExporter implements SpanExporter {

  public static final String DEFAULT_ENDPOINT = "localhost:4317";
  public static final long DEFAULT_DEADLINE_MS = TimeUnit.SECONDS.toMillis(10);

  private static final Logger logger = Logger.getLogger(OtlpGrpcSpanExporter.class.getName());
  private static final String EXPORTER_NAME = OtlpGrpcSpanExporter.class.getSimpleName();

  private static final Labels EXPORTER_NAME_LABELS = Labels.of("exporter", EXPORTER_NAME);
  private static final Labels EXPORT_SUCCESS_LABELS =
      Labels.of("exporter", EXPORTER_NAME, "success", "true");
  private static final Labels EXPORT_FAILURE_LABELS =
      Labels.of("exporter", EXPORTER_NAME, "success", "false");

  private final TraceServiceFutureStub traceService;
  private final ManagedChannel managedChannel;
  private final long deadlineMs;

  private final LongCounter spansSeen =
      GlobalOpenTelemetry.getMeter("io.opentelemetry.exporters.otlp")
          .longCounterBuilder("spansSeenByExporter")
          .build();

  private final LongCounter spansExported =
      GlobalOpenTelemetry.getMeter("io.opentelemetry.exporters.otlp")
          .longCounterBuilder("spansExportedByExporter")
          .build();

  /**
   * Creates a new OTLP gRPC Span Reporter with the given name, using the given channel.
   *
   * @param channel the channel to use when communicating with the OpenTelemetry Collector.
   * @param deadlineMs max waiting time for the collector to process each span batch. When set to 0
   *     or to a negative value, the exporter will wait indefinitely.
   */
  OtlpGrpcSpanExporter(ManagedChannel channel, long deadlineMs) {
    this.managedChannel = channel;
    this.deadlineMs = deadlineMs;
    this.traceService = TraceServiceGrpc.newFutureStub(channel);
  }

  /**
   * Submits all the given spans in a single batch to the OpenTelemetry collector.
   *
   * @param spans the list of sampled Spans to be exported.
   * @return the result of the operation
   */
  @Override
  public CompletableResultCode export(Collection<SpanData> spans) {
    spansSeen.add(spans.size(), EXPORTER_NAME_LABELS);
    ExportTraceServiceRequest exportTraceServiceRequest =
        ExportTraceServiceRequest.newBuilder()
            .addAllResourceSpans(SpanAdapter.toProtoResourceSpans(spans))
            .build();

    final CompletableResultCode result = new CompletableResultCode();

    TraceServiceFutureStub exporter;
    if (deadlineMs > 0) {
      exporter = traceService.withDeadlineAfter(deadlineMs, TimeUnit.MILLISECONDS);
    } else {
      exporter = traceService;
    }

    Futures.addCallback(
        exporter.export(exportTraceServiceRequest),
        new FutureCallback<ExportTraceServiceResponse>() {
          @Override
          public void onSuccess(@Nullable ExportTraceServiceResponse response) {
            spansExported.add(spans.size(), EXPORT_SUCCESS_LABELS);
            result.succeed();
          }

          @Override
          public void onFailure(Throwable t) {
            spansExported.add(spans.size(), EXPORT_FAILURE_LABELS);
            logger.log(Level.WARNING, "Failed to export spans. Error message: " + t.getMessage());
            logger.log(Level.FINEST, "Failed to export spans. Details follow: " + t);
            result.fail();
          }
        },
        MoreExecutors.directExecutor());
    return result;
  }

  /**
   * The OTLP exporter does not batch spans, so this method will immediately return with success.
   *
   * @return always Success
   */
  @Override
  public CompletableResultCode flush() {
    return CompletableResultCode.ofSuccess();
  }

  /**
   * Returns a new builder instance for this exporter.
   *
   * @return a new builder instance for this exporter.
   */
  public static OtlpGrpcSpanExporterBuilder builder() {
    return new OtlpGrpcSpanExporterBuilder();
  }

  /**
   * Returns a new {@link OtlpGrpcSpanExporter} reading the configuration values from the
   * environment and from system properties. System properties override values defined in the
   * environment. If a configuration value is missing, it uses the default value.
   *
   * @return a new {@link OtlpGrpcSpanExporter} instance.
   */
  public static OtlpGrpcSpanExporter getDefault() {
    return builder().readEnvironmentVariables().readSystemProperties().build();
  }

  /**
   * Initiates an orderly shutdown in which preexisting calls continue but new calls are immediately
   * cancelled.
   */
  @Override
  public CompletableResultCode shutdown() {
    final CompletableResultCode result = new CompletableResultCode();
    managedChannel.notifyWhenStateChanged(
        ConnectivityState.SHUTDOWN,
        new Runnable() {
          @Override
          public void run() {
            result.succeed();
          }
        });
    managedChannel.shutdown();
    return result;
  }
}
