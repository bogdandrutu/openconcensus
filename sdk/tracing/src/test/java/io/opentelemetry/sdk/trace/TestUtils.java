/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.trace;

import io.opentelemetry.common.AttributeValue;
import io.opentelemetry.common.Attributes;
import io.opentelemetry.sdk.trace.config.TraceConfig;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.trace.Span;
import io.opentelemetry.trace.Span.Kind;
import io.opentelemetry.trace.SpanId;
import io.opentelemetry.trace.Status;
import io.opentelemetry.trace.TraceId;
import io.opentelemetry.trace.Tracer;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Common utilities for unit tests. */
public final class TestUtils {

  private TestUtils() {}

  /**
   * Generates some random attributes used for testing.
   *
   * @return some {@link io.opentelemetry.common.Attributes}
   */
  static Attributes generateRandomAttributes() {
    return Attributes.of(
        UUID.randomUUID().toString(),
        AttributeValue.stringAttributeValue(UUID.randomUUID().toString()));
  }

  /**
   * Create a very basic SpanData instance, suitable for testing. It has the bare minimum viable
   * data.
   *
   * @return A SpanData instance.
   */
  public static SpanData makeBasicSpan() {
    return TestSpanData.newBuilder()
        .setHasEnded(true)
        .setTraceId(TraceId.getInvalid())
        .setSpanId(SpanId.getInvalid())
        .setName("span")
        .setKind(Kind.SERVER)
        .setStartEpochNanos(TimeUnit.SECONDS.toNanos(100) + 100)
        .setStatus(Status.OK)
        .setEndEpochNanos(TimeUnit.SECONDS.toNanos(200) + 200)
        .setTotalRecordedLinks(0)
        .setTotalRecordedEvents(0)
        .build();
  }

  /**
   * Create a very basic SpanData instance, suitable for testing. It has the bare minimum viable
   * data.
   *
   * @return A SpanData instance.
   */
  public static Span.Builder startSpanWithSampler(
      TracerSdkProvider tracerSdkFactory, Tracer tracer, String spanName, Sampler sampler) {
    return startSpanWithSampler(
        tracerSdkFactory, tracer, spanName, sampler, Collections.emptyMap());
  }

  /**
   * Create a very basic SpanData instance, suitable for testing. It has the bare minimum viable
   * data.
   *
   * @return A SpanData instance.
   */
  public static Span.Builder startSpanWithSampler(
      TracerSdkProvider tracerSdkFactory,
      Tracer tracer,
      String spanName,
      Sampler sampler,
      Map<String, AttributeValue> attributes) {
    TraceConfig originalConfig = tracerSdkFactory.getActiveTraceConfig();
    tracerSdkFactory.updateActiveTraceConfig(
        originalConfig.toBuilder().setSampler(sampler).build());
    try {
      Span.Builder builder = tracer.spanBuilder(spanName);
      for (Map.Entry<String, AttributeValue> entry : attributes.entrySet()) {
        builder.setAttribute(entry.getKey(), entry.getValue());
      }

      return builder;
    } finally {
      tracerSdkFactory.updateActiveTraceConfig(originalConfig);
    }
  }
}
