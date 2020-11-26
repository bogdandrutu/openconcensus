/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.api.trace.propagation;

import static io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator.TRACE_PARENT;
import static io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator.TRACE_STATE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanId;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceId;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.context.propagation.TextMapPropagator.Getter;
import io.opentelemetry.context.propagation.TextMapPropagator.Setter;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link W3CTraceContextPropagator}. */
class W3CTraceContextPropagatorTest {

  private static final TraceState TRACE_STATE_DEFAULT = TraceState.builder().build();
  private static final TraceState TRACE_STATE_NOT_DEFAULT =
      TraceState.builder().set("foo", "bar").set("bar", "baz").build();
  private static final String TRACE_ID_BASE16 = "ff000000000000000000000000000041";
  private static final String SPAN_ID_BASE16 = "ff00000000000041";
  private static final byte SAMPLED_TRACE_OPTIONS = TraceFlags.getSampled();
  private static final String TRACEPARENT_HEADER_SAMPLED =
      "00-" + TRACE_ID_BASE16 + "-" + SPAN_ID_BASE16 + "-01";
  private static final String TRACEPARENT_HEADER_NOT_SAMPLED =
      "00-" + TRACE_ID_BASE16 + "-" + SPAN_ID_BASE16 + "-00";
  private static final Setter<Map<String, String>> setter = Map::put;
  private static final Getter<Map<String, String>> getter =
      new Getter<Map<String, String>>() {
        @Override
        public Iterable<String> keys(Map<String, String> carrier) {
          return carrier.keySet();
        }

        @Nullable
        @Override
        public String get(Map<String, String> carrier, String key) {
          return carrier.get(key);
        }
      };
  // Encoding preserves the order which is the reverse order of adding.
  private static final String TRACESTATE_NOT_DEFAULT_ENCODING = "bar=baz,foo=bar";
  private static final String TRACESTATE_NOT_DEFAULT_ENCODING_WITH_SPACES =
      "bar=baz   ,    foo=bar";
  private final TextMapPropagator w3cTraceContextPropagator =
      W3CTraceContextPropagator.getInstance();

  private static SpanContext getSpanContext(Context context) {
    return Span.fromContext(context).getSpanContext();
  }

  private static Context withSpanContext(SpanContext spanContext, Context context) {
    return context.with(Span.wrap(spanContext));
  }

  @Test
  void inject_Nothing() {
    Map<String, String> carrier = new LinkedHashMap<>();
    w3cTraceContextPropagator.inject(Context.current(), carrier, setter);
    assertThat(carrier).hasSize(0);
  }

  @Test
  void inject_NullCarrierUsage() {
    final Map<String, String> carrier = new LinkedHashMap<>();
    Context context =
        withSpanContext(
            SpanContext.create(
                TRACE_ID_BASE16, SPAN_ID_BASE16, SAMPLED_TRACE_OPTIONS, TRACE_STATE_DEFAULT),
            Context.current());
    w3cTraceContextPropagator.inject(
        context,
        null,
        (Setter<Map<String, String>>) (ignored, key, value) -> carrier.put(key, value));
    assertThat(carrier).containsExactly(entry(TRACE_PARENT, TRACEPARENT_HEADER_SAMPLED));
  }

  @Test
  void inject_invalidContext() {
    Map<String, String> carrier = new LinkedHashMap<>();
    w3cTraceContextPropagator.inject(
        withSpanContext(
            SpanContext.create(
                TraceId.getInvalid(),
                SpanId.getInvalid(),
                SAMPLED_TRACE_OPTIONS,
                TraceState.builder().set("foo", "bar").build()),
            Context.current()),
        carrier,
        setter);
    assertThat(carrier).hasSize(0);
  }

  @Test
  void inject_SampledContext() {
    Map<String, String> carrier = new LinkedHashMap<>();
    Context context =
        withSpanContext(
            SpanContext.create(
                TRACE_ID_BASE16, SPAN_ID_BASE16, SAMPLED_TRACE_OPTIONS, TRACE_STATE_DEFAULT),
            Context.current());
    w3cTraceContextPropagator.inject(context, carrier, setter);
    assertThat(carrier).containsExactly(entry(TRACE_PARENT, TRACEPARENT_HEADER_SAMPLED));
  }

  @Test
  void inject_NotSampledContext() {
    Map<String, String> carrier = new LinkedHashMap<>();
    Context context =
        withSpanContext(
            SpanContext.create(
                TRACE_ID_BASE16, SPAN_ID_BASE16, TraceFlags.getDefault(), TRACE_STATE_DEFAULT),
            Context.current());
    w3cTraceContextPropagator.inject(context, carrier, setter);
    assertThat(carrier).containsExactly(entry(TRACE_PARENT, TRACEPARENT_HEADER_NOT_SAMPLED));
  }

  @Test
  void inject_SampledContext_WithTraceState() {
    Map<String, String> carrier = new LinkedHashMap<>();
    Context context =
        withSpanContext(
            SpanContext.create(
                TRACE_ID_BASE16, SPAN_ID_BASE16, SAMPLED_TRACE_OPTIONS, TRACE_STATE_NOT_DEFAULT),
            Context.current());
    w3cTraceContextPropagator.inject(context, carrier, setter);
    assertThat(carrier)
        .containsExactly(
            entry(TRACE_PARENT, TRACEPARENT_HEADER_SAMPLED),
            entry(TRACE_STATE, TRACESTATE_NOT_DEFAULT_ENCODING));
  }

  @Test
  void inject_NotSampledContext_WithTraceState() {
    Map<String, String> carrier = new LinkedHashMap<>();
    Context context =
        withSpanContext(
            SpanContext.create(
                TRACE_ID_BASE16, SPAN_ID_BASE16, TraceFlags.getDefault(), TRACE_STATE_NOT_DEFAULT),
            Context.current());
    w3cTraceContextPropagator.inject(context, carrier, setter);
    assertThat(carrier)
        .containsExactly(
            entry(TRACE_PARENT, TRACEPARENT_HEADER_NOT_SAMPLED),
            entry(TRACE_STATE, TRACESTATE_NOT_DEFAULT_ENCODING));
  }

  @Test
  void extract_Nothing() {
    // Context remains untouched.
    assertThat(
            w3cTraceContextPropagator.extract(
                Context.current(), Collections.<String, String>emptyMap(), getter))
        .isSameAs(Context.current());
  }

  @Test
  void extract_SampledContext() {
    Map<String, String> carrier = new LinkedHashMap<>();
    carrier.put(TRACE_PARENT, TRACEPARENT_HEADER_SAMPLED);
    assertThat(
            getSpanContext(w3cTraceContextPropagator.extract(Context.current(), carrier, getter)))
        .isEqualTo(
            SpanContext.createFromRemoteParent(
                TRACE_ID_BASE16, SPAN_ID_BASE16, SAMPLED_TRACE_OPTIONS, TRACE_STATE_DEFAULT));
  }

  @Test
  void extract_NullCarrier() {
    Map<String, String> carrier = new LinkedHashMap<>();
    carrier.put(TRACE_PARENT, TRACEPARENT_HEADER_SAMPLED);
    assertThat(
            getSpanContext(w3cTraceContextPropagator.extract(Context.current(), carrier, getter)))
        .isEqualTo(
            SpanContext.createFromRemoteParent(
                TRACE_ID_BASE16, SPAN_ID_BASE16, SAMPLED_TRACE_OPTIONS, TRACE_STATE_DEFAULT));
  }

  @Test
  void extractAndInject_MoreFlags() {
    String traceParent = "00-" + TRACE_ID_BASE16 + "-" + SPAN_ID_BASE16 + "-03";
    Map<String, String> extractCarrier = new LinkedHashMap<>();
    extractCarrier.put(TRACE_PARENT, traceParent);
    Context context = w3cTraceContextPropagator.extract(Context.current(), extractCarrier, getter);
    Map<String, String> injectCarrier = new LinkedHashMap<>();
    w3cTraceContextPropagator.inject(context, injectCarrier, setter);
    assertThat(extractCarrier).isEqualTo(injectCarrier);
  }

  @Test
  void extract_NotSampledContext() {
    Map<String, String> carrier = new LinkedHashMap<>();
    carrier.put(TRACE_PARENT, TRACEPARENT_HEADER_NOT_SAMPLED);
    assertThat(
            getSpanContext(w3cTraceContextPropagator.extract(Context.current(), carrier, getter)))
        .isEqualTo(
            SpanContext.createFromRemoteParent(
                TRACE_ID_BASE16, SPAN_ID_BASE16, TraceFlags.getDefault(), TRACE_STATE_DEFAULT));
  }

  @Test
  void extract_SampledContext_WithTraceState() {
    Map<String, String> carrier = new LinkedHashMap<>();
    carrier.put(TRACE_PARENT, TRACEPARENT_HEADER_SAMPLED);
    carrier.put(TRACE_STATE, TRACESTATE_NOT_DEFAULT_ENCODING);
    assertThat(
            getSpanContext(w3cTraceContextPropagator.extract(Context.current(), carrier, getter)))
        .isEqualTo(
            SpanContext.createFromRemoteParent(
                TRACE_ID_BASE16, SPAN_ID_BASE16, SAMPLED_TRACE_OPTIONS, TRACE_STATE_NOT_DEFAULT));
  }

  @Test
  void extract_NotSampledContext_WithTraceState() {
    Map<String, String> carrier = new LinkedHashMap<>();
    carrier.put(TRACE_PARENT, TRACEPARENT_HEADER_NOT_SAMPLED);
    carrier.put(TRACE_STATE, TRACESTATE_NOT_DEFAULT_ENCODING);
    assertThat(
            getSpanContext(w3cTraceContextPropagator.extract(Context.current(), carrier, getter)))
        .isEqualTo(
            SpanContext.createFromRemoteParent(
                TRACE_ID_BASE16, SPAN_ID_BASE16, TraceFlags.getDefault(), TRACE_STATE_NOT_DEFAULT));
  }

  @Test
  void extract_NotSampledContext_NextVersion() {
    Map<String, String> carrier = new LinkedHashMap<>();
    carrier.put(TRACE_PARENT, "01-" + TRACE_ID_BASE16 + "-" + SPAN_ID_BASE16 + "-00-02");
    assertThat(
            getSpanContext(w3cTraceContextPropagator.extract(Context.current(), carrier, getter)))
        .isEqualTo(
            SpanContext.createFromRemoteParent(
                TRACE_ID_BASE16, SPAN_ID_BASE16, TraceFlags.getDefault(), TRACE_STATE_DEFAULT));
  }

  @Test
  void extract_NotSampledContext_EmptyTraceState() {
    Map<String, String> carrier = new LinkedHashMap<>();
    carrier.put(TRACE_PARENT, TRACEPARENT_HEADER_NOT_SAMPLED);
    carrier.put(TRACE_STATE, "");
    assertThat(
            getSpanContext(w3cTraceContextPropagator.extract(Context.current(), carrier, getter)))
        .isEqualTo(
            SpanContext.createFromRemoteParent(
                TRACE_ID_BASE16, SPAN_ID_BASE16, TraceFlags.getDefault(), TRACE_STATE_DEFAULT));
  }

  @Test
  void extract_NotSampledContext_TraceStateWithSpaces() {
    Map<String, String> carrier = new LinkedHashMap<>();
    carrier.put(TRACE_PARENT, TRACEPARENT_HEADER_NOT_SAMPLED);
    carrier.put(TRACE_STATE, TRACESTATE_NOT_DEFAULT_ENCODING_WITH_SPACES);
    assertThat(
            getSpanContext(w3cTraceContextPropagator.extract(Context.current(), carrier, getter)))
        .isEqualTo(
            SpanContext.createFromRemoteParent(
                TRACE_ID_BASE16, SPAN_ID_BASE16, TraceFlags.getDefault(), TRACE_STATE_NOT_DEFAULT));
  }

  @Test
  void extract_EmptyHeader() {
    Map<String, String> invalidHeaders = new LinkedHashMap<>();
    invalidHeaders.put(TRACE_PARENT, "");
    assertThat(
            getSpanContext(
                w3cTraceContextPropagator.extract(Context.current(), invalidHeaders, getter)))
        .isSameAs(SpanContext.getInvalid());
  }

  @Test
  void extract_InvalidTraceId() {
    Map<String, String> invalidHeaders = new LinkedHashMap<>();
    invalidHeaders.put(
        TRACE_PARENT, "00-" + "abcdefghijklmnopabcdefghijklmnop" + "-" + SPAN_ID_BASE16 + "-01");
    assertThat(
            getSpanContext(
                w3cTraceContextPropagator.extract(Context.current(), invalidHeaders, getter)))
        .isSameAs(SpanContext.getInvalid());
  }

  @Test
  void extract_InvalidTraceId_Size() {
    Map<String, String> invalidHeaders = new LinkedHashMap<>();
    invalidHeaders.put(TRACE_PARENT, "00-" + TRACE_ID_BASE16 + "00-" + SPAN_ID_BASE16 + "-01");
    assertThat(
            getSpanContext(
                w3cTraceContextPropagator.extract(Context.current(), invalidHeaders, getter)))
        .isSameAs(SpanContext.getInvalid());
  }

  @Test
  void extract_InvalidSpanId() {
    Map<String, String> invalidHeaders = new HashMap<>();
    invalidHeaders.put(TRACE_PARENT, "00-" + TRACE_ID_BASE16 + "-" + "abcdefghijklmnop" + "-01");
    assertThat(
            getSpanContext(
                w3cTraceContextPropagator.extract(Context.current(), invalidHeaders, getter)))
        .isSameAs(SpanContext.getInvalid());
  }

  @Test
  void extract_InvalidSpanId_Size() {
    Map<String, String> invalidHeaders = new HashMap<>();
    invalidHeaders.put(TRACE_PARENT, "00-" + TRACE_ID_BASE16 + "-" + SPAN_ID_BASE16 + "00-01");
    assertThat(
            getSpanContext(
                w3cTraceContextPropagator.extract(Context.current(), invalidHeaders, getter)))
        .isSameAs(SpanContext.getInvalid());
  }

  @Test
  void extract_InvalidTraceFlags() {
    Map<String, String> invalidHeaders = new HashMap<>();
    invalidHeaders.put(TRACE_PARENT, "00-" + TRACE_ID_BASE16 + "-" + SPAN_ID_BASE16 + "-gh");
    assertThat(
            getSpanContext(
                w3cTraceContextPropagator.extract(Context.current(), invalidHeaders, getter)))
        .isSameAs(SpanContext.getInvalid());
  }

  @Test
  void extract_InvalidTraceFlags_Size() {
    Map<String, String> invalidHeaders = new HashMap<>();
    invalidHeaders.put(TRACE_PARENT, "00-" + TRACE_ID_BASE16 + "-" + SPAN_ID_BASE16 + "-0100");
    assertThat(
            getSpanContext(
                w3cTraceContextPropagator.extract(Context.current(), invalidHeaders, getter)))
        .isSameAs(SpanContext.getInvalid());
  }

  @Test
  void extract_InvalidTracestate_EntriesDelimiter() {
    Map<String, String> invalidHeaders = new HashMap<>();
    invalidHeaders.put(TRACE_PARENT, "00-" + TRACE_ID_BASE16 + "-" + SPAN_ID_BASE16 + "-01");
    invalidHeaders.put(TRACE_STATE, "foo=bar;test=test");
    assertThat(
            getSpanContext(
                w3cTraceContextPropagator.extract(Context.current(), invalidHeaders, getter)))
        .isEqualTo(
            SpanContext.createFromRemoteParent(
                TRACE_ID_BASE16, SPAN_ID_BASE16, SAMPLED_TRACE_OPTIONS, TRACE_STATE_DEFAULT));
  }

  @Test
  void extract_InvalidTracestate_KeyValueDelimiter() {
    Map<String, String> invalidHeaders = new HashMap<>();
    invalidHeaders.put(TRACE_PARENT, "00-" + TRACE_ID_BASE16 + "-" + SPAN_ID_BASE16 + "-01");
    invalidHeaders.put(TRACE_STATE, "foo=bar,test-test");
    assertThat(
            getSpanContext(
                w3cTraceContextPropagator.extract(Context.current(), invalidHeaders, getter)))
        .isEqualTo(
            SpanContext.createFromRemoteParent(
                TRACE_ID_BASE16, SPAN_ID_BASE16, SAMPLED_TRACE_OPTIONS, TRACE_STATE_DEFAULT));
  }

  @Test
  void extract_InvalidTracestate_OneString() {
    Map<String, String> invalidHeaders = new HashMap<>();
    invalidHeaders.put(TRACE_PARENT, "00-" + TRACE_ID_BASE16 + "-" + SPAN_ID_BASE16 + "-01");
    invalidHeaders.put(TRACE_STATE, "test-test");
    assertThat(
            getSpanContext(
                w3cTraceContextPropagator.extract(Context.current(), invalidHeaders, getter)))
        .isEqualTo(
            SpanContext.createFromRemoteParent(
                TRACE_ID_BASE16, SPAN_ID_BASE16, SAMPLED_TRACE_OPTIONS, TRACE_STATE_DEFAULT));
  }

  @Test
  void extract_InvalidVersion_ff() {
    Map<String, String> invalidHeaders = new HashMap<>();
    invalidHeaders.put(TRACE_PARENT, "ff-" + TRACE_ID_BASE16 + "-" + SPAN_ID_BASE16 + "-01");
    assertThat(
            getSpanContext(
                w3cTraceContextPropagator.extract(Context.current(), invalidHeaders, getter)))
        .isSameAs(SpanContext.getInvalid());
  }

  @Test
  void extract_InvalidTraceparent_extraTrailing() {
    Map<String, String> invalidHeaders = new HashMap<>();
    invalidHeaders.put(TRACE_PARENT, "00-" + TRACE_ID_BASE16 + "-" + SPAN_ID_BASE16 + "-00-01");
    assertThat(
            getSpanContext(
                w3cTraceContextPropagator.extract(Context.current(), invalidHeaders, getter)))
        .isSameAs(SpanContext.getInvalid());
  }

  @Test
  void extract_ValidTraceparent_nextVersion_extraTrailing() {
    Map<String, String> invalidHeaders = new HashMap<>();
    invalidHeaders.put(TRACE_PARENT, "01-" + TRACE_ID_BASE16 + "-" + SPAN_ID_BASE16 + "-00-01");
    assertThat(
            getSpanContext(
                w3cTraceContextPropagator.extract(Context.current(), invalidHeaders, getter)))
        .isEqualTo(
            SpanContext.createFromRemoteParent(
                TRACE_ID_BASE16, SPAN_ID_BASE16, TraceFlags.getDefault(), TRACE_STATE_DEFAULT));
  }

  @Test
  void fieldsList() {
    assertThat(w3cTraceContextPropagator.fields()).containsExactly(TRACE_PARENT, TRACE_STATE);
  }

  @Test
  void headerNames() {
    assertThat(TRACE_PARENT).isEqualTo("traceparent");
    assertThat(TRACE_STATE).isEqualTo("tracestate");
  }

  @Test
  void extract_emptyCarrier() {
    Map<String, String> emptyHeaders = new HashMap<>();
    assertThat(
            getSpanContext(
                w3cTraceContextPropagator.extract(Context.current(), emptyHeaders, getter)))
        .isSameAs(SpanContext.getInvalid());
  }
}
