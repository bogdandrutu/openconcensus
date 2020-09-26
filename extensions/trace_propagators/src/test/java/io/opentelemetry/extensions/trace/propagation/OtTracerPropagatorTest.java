/*
 * Copyright 2020, OpenTelemetry Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.opentelemetry.extensions.trace.propagation;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.Context;
import io.opentelemetry.context.propagation.TextMapPropagator.Getter;
import io.opentelemetry.context.propagation.TextMapPropagator.Setter;
import io.opentelemetry.trace.Span;
import io.opentelemetry.trace.SpanId;
import io.opentelemetry.trace.TraceFlags;
import io.opentelemetry.trace.TraceId;
import io.opentelemetry.trace.TraceState;
import io.opentelemetry.trace.TracingContextUtils;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class OtTracerPropagatorTest {

  private static final TraceState TRACE_STATE_DEFAULT = TraceState.builder().build();
  private static final String TRACE_ID = "ff000000000000000000000000000041";
  private static final String SHORT_TRACE_ID = "ff00000000000000";
  private static final String SHORT_TRACE_ID_FULL = "0000000000000000ff00000000000000";
  private static final String SPAN_ID = "ff00000000000041";
  private static final byte SAMPLED_TRACE_OPTIONS = TraceFlags.getSampled();
  private static final Setter<Map<String, String>> setter = Map::put;
  private static final Getter<Map<String, String>> getter = Map::get;
  private final OtTracerPropagator propagator = OtTracerPropagator.getInstance();

  @Test
  void inject_invalidContext() {
    Map<String, String> carrier = new LinkedHashMap<>();
    propagator.inject(
        TracingContextUtils.withSpan(
            Span.getPropagated(
                TraceId.getInvalid(),
                SpanId.getInvalid(),
                SAMPLED_TRACE_OPTIONS,
                TraceState.builder().set("foo", "bar").build()),
            Context.current()),
        carrier,
        setter);
    assertThat(carrier).isEmpty();
  }

  @Test
  void inject_SampledContext() {
    Map<String, String> carrier = new LinkedHashMap<>();
    propagator.inject(
        TracingContextUtils.withSpan(
            Span.getPropagated(TRACE_ID, SPAN_ID, SAMPLED_TRACE_OPTIONS, TRACE_STATE_DEFAULT),
            Context.current()),
        carrier,
        setter);
    assertThat(carrier).containsEntry(OtTracerPropagator.TRACE_ID_HEADER, TRACE_ID);
    assertThat(carrier).containsEntry(OtTracerPropagator.SPAN_ID_HEADER, SPAN_ID);
    assertThat(carrier).containsEntry(OtTracerPropagator.SAMPLED_HEADER, "true");
  }

  @Test
  void inject_SampledContext_nullCarrierUsage() {
    final Map<String, String> carrier = new LinkedHashMap<>();
    propagator.inject(
        TracingContextUtils.withSpan(
            Span.getPropagated(TRACE_ID, SPAN_ID, SAMPLED_TRACE_OPTIONS, TRACE_STATE_DEFAULT),
            Context.current()),
        null,
        (Setter<Map<String, String>>) (ignored, key, value) -> carrier.put(key, value));
    assertThat(carrier).containsEntry(OtTracerPropagator.TRACE_ID_HEADER, TRACE_ID);
    assertThat(carrier).containsEntry(OtTracerPropagator.SPAN_ID_HEADER, SPAN_ID);
    assertThat(carrier).containsEntry(OtTracerPropagator.SAMPLED_HEADER, "true");
  }

  @Test
  void inject_NotSampledContext() {
    Map<String, String> carrier = new LinkedHashMap<>();
    propagator.inject(
        TracingContextUtils.withSpan(
            Span.getPropagated(TRACE_ID, SPAN_ID, TraceFlags.getDefault(), TRACE_STATE_DEFAULT),
            Context.current()),
        carrier,
        setter);
    assertThat(carrier).containsEntry(OtTracerPropagator.TRACE_ID_HEADER, TRACE_ID);
    assertThat(carrier).containsEntry(OtTracerPropagator.SPAN_ID_HEADER, SPAN_ID);
    assertThat(carrier).containsEntry(OtTracerPropagator.SAMPLED_HEADER, "false");
  }

  @Test
  void extract_Nothing() {
    // Context remains untouched.
    assertThat(
            propagator.extract(Context.current(), Collections.<String, String>emptyMap(), Map::get))
        .isSameAs(Context.current());
  }

  @Test
  void extract_SampledContext_Int() {
    Map<String, String> carrier = new LinkedHashMap<>();
    carrier.put(OtTracerPropagator.TRACE_ID_HEADER, TRACE_ID);
    carrier.put(OtTracerPropagator.SPAN_ID_HEADER, SPAN_ID);
    carrier.put(OtTracerPropagator.SAMPLED_HEADER, Common.TRUE_INT);

    assertThat(TracingContextUtils.getSpan(propagator.extract(Context.current(), carrier, getter)))
        .isEqualTo(
            Span.getPropagated(TRACE_ID, SPAN_ID, SAMPLED_TRACE_OPTIONS, TRACE_STATE_DEFAULT));
  }

  @Test
  void extract_SampledContext_Bool() {
    Map<String, String> carrier = new LinkedHashMap<>();
    carrier.put(OtTracerPropagator.TRACE_ID_HEADER, TRACE_ID);
    carrier.put(OtTracerPropagator.SPAN_ID_HEADER, SPAN_ID);
    carrier.put(OtTracerPropagator.SAMPLED_HEADER, "true");

    assertThat(TracingContextUtils.getSpan(propagator.extract(Context.current(), carrier, getter)))
        .isEqualTo(
            Span.getPropagated(TRACE_ID, SPAN_ID, SAMPLED_TRACE_OPTIONS, TRACE_STATE_DEFAULT));
  }

  @Test
  void extract_NotSampledContext() {
    Map<String, String> carrier = new LinkedHashMap<>();
    carrier.put(OtTracerPropagator.TRACE_ID_HEADER, TRACE_ID);
    carrier.put(OtTracerPropagator.SPAN_ID_HEADER, SPAN_ID);
    carrier.put(OtTracerPropagator.SAMPLED_HEADER, Common.FALSE_INT);

    assertThat(TracingContextUtils.getSpan(propagator.extract(Context.current(), carrier, getter)))
        .isEqualTo(
            Span.getPropagated(TRACE_ID, SPAN_ID, TraceFlags.getDefault(), TRACE_STATE_DEFAULT));
  }

  @Test
  void extract_SampledContext_Int_Short_TraceId() {
    Map<String, String> carrier = new LinkedHashMap<>();
    carrier.put(OtTracerPropagator.TRACE_ID_HEADER, SHORT_TRACE_ID);
    carrier.put(OtTracerPropagator.SPAN_ID_HEADER, SPAN_ID);
    carrier.put(OtTracerPropagator.SAMPLED_HEADER, Common.TRUE_INT);

    assertThat(TracingContextUtils.getSpan(propagator.extract(Context.current(), carrier, getter)))
        .isEqualTo(
            Span.getPropagated(
                SHORT_TRACE_ID_FULL, SPAN_ID, SAMPLED_TRACE_OPTIONS, TRACE_STATE_DEFAULT));
  }

  @Test
  void extract_SampledContext_Bool_Short_TraceId() {
    Map<String, String> carrier = new LinkedHashMap<>();
    carrier.put(OtTracerPropagator.TRACE_ID_HEADER, SHORT_TRACE_ID);
    carrier.put(OtTracerPropagator.SPAN_ID_HEADER, SPAN_ID);
    carrier.put(OtTracerPropagator.SAMPLED_HEADER, "true");

    assertThat(TracingContextUtils.getSpan(propagator.extract(Context.current(), carrier, getter)))
        .isEqualTo(
            Span.getPropagated(
                SHORT_TRACE_ID_FULL, SPAN_ID, SAMPLED_TRACE_OPTIONS, TRACE_STATE_DEFAULT));
  }

  @Test
  void extract_NotSampledContext_Short_TraceId() {
    Map<String, String> carrier = new LinkedHashMap<>();
    carrier.put(OtTracerPropagator.TRACE_ID_HEADER, SHORT_TRACE_ID);
    carrier.put(OtTracerPropagator.SPAN_ID_HEADER, SPAN_ID);
    carrier.put(OtTracerPropagator.SAMPLED_HEADER, Common.FALSE_INT);

    assertThat(TracingContextUtils.getSpan(propagator.extract(Context.current(), carrier, getter)))
        .isEqualTo(
            Span.getPropagated(
                SHORT_TRACE_ID_FULL, SPAN_ID, TraceFlags.getDefault(), TRACE_STATE_DEFAULT));
  }

  @Test
  void extract_InvalidTraceId() {
    Map<String, String> invalidHeaders = new LinkedHashMap<>();
    invalidHeaders.put(OtTracerPropagator.TRACE_ID_HEADER, "abcdefghijklmnopabcdefghijklmnop");
    invalidHeaders.put(OtTracerPropagator.SPAN_ID_HEADER, SPAN_ID);
    invalidHeaders.put(OtTracerPropagator.SAMPLED_HEADER, Common.TRUE_INT);
    assertThat(
            TracingContextUtils.getSpan(
                propagator.extract(Context.current(), invalidHeaders, getter)))
        .isSameAs(Span.getInvalid());
  }

  @Test
  void extract_InvalidTraceId_Size() {
    Map<String, String> invalidHeaders = new LinkedHashMap<>();
    invalidHeaders.put(OtTracerPropagator.TRACE_ID_HEADER, TRACE_ID + "00");
    invalidHeaders.put(OtTracerPropagator.SPAN_ID_HEADER, SPAN_ID);
    invalidHeaders.put(OtTracerPropagator.SAMPLED_HEADER, Common.TRUE_INT);
    assertThat(
            TracingContextUtils.getSpan(
                propagator.extract(Context.current(), invalidHeaders, getter)))
        .isSameAs(Span.getInvalid());
  }

  @Test
  void extract_InvalidSpanId() {
    Map<String, String> invalidHeaders = new LinkedHashMap<>();
    invalidHeaders.put(OtTracerPropagator.TRACE_ID_HEADER, TRACE_ID);
    invalidHeaders.put(OtTracerPropagator.SPAN_ID_HEADER, "abcdefghijklmnop");
    invalidHeaders.put(OtTracerPropagator.SAMPLED_HEADER, Common.TRUE_INT);
    assertThat(
            TracingContextUtils.getSpan(
                propagator.extract(Context.current(), invalidHeaders, getter)))
        .isSameAs(Span.getInvalid());
  }

  @Test
  void extract_InvalidSpanId_Size() {
    Map<String, String> invalidHeaders = new LinkedHashMap<>();
    invalidHeaders.put(OtTracerPropagator.TRACE_ID_HEADER, TRACE_ID);
    invalidHeaders.put(OtTracerPropagator.SPAN_ID_HEADER, "abcdefghijklmnop" + "00");
    invalidHeaders.put(OtTracerPropagator.SAMPLED_HEADER, Common.TRUE_INT);
    assertThat(
            TracingContextUtils.getSpan(
                propagator.extract(Context.current(), invalidHeaders, getter)))
        .isSameAs(Span.getInvalid());
  }

  @Test
  void extract_emptyCarrier() {
    Map<String, String> emptyHeaders = new HashMap<>();
    assertThat(
            TracingContextUtils.getSpan(
                propagator.extract(Context.current(), emptyHeaders, getter)))
        .isEqualTo(Span.getInvalid());
  }
}
