/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.trace;

import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.config.TraceConfig;
import io.opentelemetry.sdk.trace.export.SpanExporter;

/**
 * "Management" interface for the Tracing SDK. This interface exposes methods for configuring the
 * Tracing SDK, as well as several lifecycle methods.
 */
public interface TracerSdkManagement {

  /**
   * Returns the active {@code TraceConfig}.
   *
   * @return the active {@code TraceConfig}.
   */
  TraceConfig getActiveTraceConfig();

  /**
   * Updates the active {@link TraceConfig}.
   *
   * <p>Note: To update the {@link TraceConfig} associated with this instance you should use the
   * {@link TraceConfig#toBuilder()} method on the {@link TraceConfig} returned from {@link
   * #getActiveTraceConfig()}, make the changes desired to the {@link TraceConfig.Builder} instance,
   * then use this method with the resulting {@link TraceConfig} instance.
   *
   * @param traceConfig the new active {@code TraceConfig}.
   * @see TraceConfig
   */
  void updateActiveTraceConfig(TraceConfig traceConfig);

  /**
   * Adds a new {@code SpanProcessor} to this {@code Tracer}.
   *
   * <p>Any registered processor cause overhead, consider to use an async/batch processor especially
   * for span exporting, and export to multiple backends using the {@link
   * io.opentelemetry.sdk.trace.export.SpanExporter#composite(SpanExporter...)}.
   *
   * @param spanProcessor the new {@code SpanProcessor} to be added.
   */
  void addSpanProcessor(SpanProcessor spanProcessor);

  /**
   * Attempts to stop all the activity for this {@link Tracer}. Calls {@link
   * SpanProcessor#shutdown()} for all registered {@link SpanProcessor}s.
   *
   * <p>This operation may block until all the Spans are processed. Must be called before turning
   * off the main application to ensure all data are processed and exported.
   *
   * <p>After this is called all the newly created {@code Span}s will be no-op.
   */
  void shutdown();

  /**
   * Requests the active span processor to process all span events that have not yet been processed
   * and returns a {@link CompletableResultCode} which is completed when the flush is finished.
   *
   * @see SpanProcessor#forceFlush()
   */
  CompletableResultCode forceFlush();
}
