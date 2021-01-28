/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.extension.trace.propagation;

import static io.opentelemetry.extension.trace.propagation.Common.MAX_TRACE_ID_LENGTH;

import io.opentelemetry.api.baggage.Baggage;
import io.opentelemetry.api.baggage.BaggageBuilder;
import io.opentelemetry.api.baggage.BaggageEntryMetadata;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceId;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapPropagator;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/**
 * Implementation of the Lightstep propagation protocol. Context is propagated through 3 headers,
 * ot-tracer-traceid, ot-tracer-span-id, and ot-tracer-sampled. Baggage is not supported in this
 * implementation. IDs are sent as hex strings and sampled is sent as true or false. See <a
 * href=https://github.com/lightstep/lightstep-tracer-java-common/blob/master/common/src/main/java/com/lightstep/tracer/shared/TextMapPropagator.java>Lightstep
 * TextMapPropagator</a>.
 */
@Immutable
public final class OtTracerPropagator implements TextMapPropagator {

  static final String TRACE_ID_HEADER = "ot-tracer-traceid";
  static final String SPAN_ID_HEADER = "ot-tracer-spanid";
  static final String SAMPLED_HEADER = "ot-tracer-sampled";
  static final String PREFIX_BAGGAGE_HEADER = "ot-baggage-";
  private static final Collection<String> FIELDS =
      Collections.unmodifiableList(Arrays.asList(TRACE_ID_HEADER, SPAN_ID_HEADER, SAMPLED_HEADER));

  private static final OtTracerPropagator INSTANCE = new OtTracerPropagator();

  private OtTracerPropagator() {
    // singleton
  }

  public static OtTracerPropagator getInstance() {
    return INSTANCE;
  }

  @Override
  public Collection<String> fields() {
    return FIELDS;
  }

  @Override
  public <C> void inject(Context context, C carrier, Setter<C> setter) {
    if (context == null || setter == null) {
      return;
    }
    final SpanContext spanContext = Span.fromContext(context).getSpanContext();
    if (!spanContext.isValid()) {
      return;
    }
    // Lightstep trace id MUST be 64-bits therefore OpenTelemetry trace id is truncated to 64-bits
    // by retaining least significant (right-most) bits.
    setter.set(
        carrier,
        TRACE_ID_HEADER,
        spanContext.getTraceIdAsHexString().substring(TraceId.getHexLength() / 2));
    setter.set(carrier, SPAN_ID_HEADER, spanContext.getSpanIdAsHexString());
    setter.set(carrier, SAMPLED_HEADER, String.valueOf(spanContext.isSampled()));

    Baggage baggage = Baggage.fromContext(context);
    if (!baggage.isEmpty()) {
      baggage.forEach(
          (key, value, metadata) -> {
            // Metadata is not supported by OpenTracing
            setter.set(carrier, PREFIX_BAGGAGE_HEADER + key, value);
          });
    }
  }

  @Override
  public <C> Context extract(Context context, @Nullable C carrier, Getter<C> getter) {
    if (context == null || getter == null) {
      // TODO Other propagators throw exceptions here
      return context;
    }
    String incomingTraceId = getter.get(carrier, TRACE_ID_HEADER);
    String traceId =
        incomingTraceId == null
            ? TraceId.getInvalid()
            : StringUtils.padLeft(incomingTraceId, MAX_TRACE_ID_LENGTH);
    String spanId = getter.get(carrier, SPAN_ID_HEADER);
    String sampled = getter.get(carrier, SAMPLED_HEADER);
    SpanContext spanContext = buildSpanContext(traceId, spanId, sampled);
    if (!spanContext.isValid()) {
      return context;
    }

    Context extractedContext = context.with(Span.wrap(spanContext));

    if (carrier != null) {
      BaggageBuilder baggageBuilder = Baggage.builder();
      for (String key : getter.keys(carrier)) {
        if (!key.startsWith(PREFIX_BAGGAGE_HEADER)) {
          continue;
        }
        String value = getter.get(carrier, key);
        if (value == null) {
          continue;
        }
        int beginningOfMetadata = value.indexOf(";");
        String metadata = "";
        if (beginningOfMetadata > 0) {
          metadata = value.substring(beginningOfMetadata + 1);
          value = value.substring(0, beginningOfMetadata);
        }
        baggageBuilder.put(
            key.replace(OtTracerPropagator.PREFIX_BAGGAGE_HEADER, ""),
            value,
            BaggageEntryMetadata.create(metadata.trim()));
      }
      Baggage baggage = baggageBuilder.build();
      if (!baggage.isEmpty()) {
        extractedContext = extractedContext.with(baggage);
      }
    }

    return extractedContext;
  }

  static SpanContext buildSpanContext(String traceId, String spanId, String sampled) {
    if (!Common.isTraceIdValid(traceId) || !Common.isSpanIdValid(spanId)) {
      return SpanContext.getInvalid();
    }
    return Common.buildSpanContext(traceId, spanId, sampled);
  }
}
