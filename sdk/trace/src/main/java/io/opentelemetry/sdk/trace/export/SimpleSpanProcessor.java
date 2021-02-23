/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.trace.export;

import static java.util.Objects.requireNonNull;

import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.ReadWriteSpan;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An implementation of the {@link SpanProcessor} that converts the {@link ReadableSpan} to {@link
 * SpanData} and passes it directly to the configured exporter. This processor should only be used
 * where the exporter(s) are able to handle multiple exports simultaneously, as there is no back
 * pressure consideration here.
 */
public final class SimpleSpanProcessor implements SpanProcessor {

  private static final Logger logger = Logger.getLogger(SimpleSpanProcessor.class.getName());

  private final SpanExporter spanExporter;
  private final boolean sampled;
  private final AtomicBoolean isShutdown = new AtomicBoolean(false);

  /**
   * Returns a new {@link SimpleSpanProcessor} which exports spans to the {@link SpanExporter}
   * synchronously.
   */
  public static SpanProcessor create(SpanExporter exporter) {
    requireNonNull(exporter, "exporter");
    return new SimpleSpanProcessor(exporter, /* sampled= */ true);
  }

  SimpleSpanProcessor(SpanExporter spanExporter, boolean sampled) {
    this.spanExporter = requireNonNull(spanExporter, "spanExporter");
    this.sampled = sampled;
  }

  @Override
  public void onStart(Context parentContext, ReadWriteSpan span) {
    // Do nothing.
  }

  @Override
  public boolean isStartRequired() {
    return false;
  }

  @Override
  public void onEnd(ReadableSpan span) {
    if (sampled && !span.getSpanContext().isSampled()) {
      return;
    }
    try {
      List<SpanData> spans = Collections.singletonList(span.toSpanData());
      final CompletableResultCode result = spanExporter.export(spans);
      result.whenComplete(
          () -> {
            if (!result.isSuccess()) {
              logger.log(Level.FINE, "Exporter failed");
            }
          });
    } catch (RuntimeException e) {
      logger.log(Level.WARNING, "Exporter threw an Exception", e);
    }
  }

  @Override
  public boolean isEndRequired() {
    return true;
  }

  @Override
  public CompletableResultCode shutdown() {
    if (isShutdown.getAndSet(true)) {
      return CompletableResultCode.ofSuccess();
    }
    return spanExporter.shutdown();
  }
}
