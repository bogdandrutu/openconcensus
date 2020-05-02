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

package io.opentelemetry.contrib.trace.propagation;

import static io.opentelemetry.contrib.trace.propagation.B3Propagator.COMBINED_HEADER;
import static io.opentelemetry.contrib.trace.propagation.B3Propagator.COMBINED_HEADER_DELIMITER;
import static io.opentelemetry.contrib.trace.propagation.B3Propagator.MAX_SPAN_ID_LENGTH;
import static io.opentelemetry.contrib.trace.propagation.B3Propagator.MAX_TRACE_ID_LENGTH;
import static io.opentelemetry.contrib.trace.propagation.B3Propagator.SAMPLED_HEADER;
import static io.opentelemetry.contrib.trace.propagation.B3Propagator.SPAN_ID_HEADER;
import static io.opentelemetry.contrib.trace.propagation.B3Propagator.TRACE_ID_HEADER;
import static io.opentelemetry.contrib.trace.propagation.B3Propagator.TRUE_INT;

import io.grpc.Context;
import io.opentelemetry.context.propagation.HttpTextFormat;
import io.opentelemetry.internal.StringUtils;
import io.opentelemetry.trace.DefaultSpan;
import io.opentelemetry.trace.SpanContext;
import io.opentelemetry.trace.SpanId;
import io.opentelemetry.trace.TraceFlags;
import io.opentelemetry.trace.TraceId;
import io.opentelemetry.trace.TraceState;
import io.opentelemetry.trace.TracingContextUtils;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.concurrent.Immutable;

@Immutable
final class B3PropagatorExtractor {
  private static final TraceFlags SAMPLED_FLAGS = TraceFlags.builder().setIsSampled(true).build();
  private static final TraceFlags NOT_SAMPLED_FLAGS =
      TraceFlags.builder().setIsSampled(false).build();
  private static final Logger logger = Logger.getLogger(B3PropagatorExtractor.class.getName());

  private final boolean isSingleHeader;

  B3PropagatorExtractor(boolean isSingleHeader) {
    this.isSingleHeader = isSingleHeader;
  }

  <C> Context extract(Context context, C carrier, HttpTextFormat.Getter<C> getter) {
    Objects.requireNonNull(carrier, "carrier");
    Objects.requireNonNull(getter, "getter");

    SpanContext spanContext;

    if (isSingleHeader) {
      spanContext = getSpanContextFromSingleHeader(carrier, getter);
    } else {
      spanContext = getSpanContextFromMultipleHeaders(carrier, getter);
    }

    return TracingContextUtils.withSpan(DefaultSpan.create(spanContext), context);
  }

  @SuppressWarnings("StringSplitter")
  private static <C> SpanContext getSpanContextFromSingleHeader(
      C carrier, HttpTextFormat.Getter<C> getter) {
    String value = getter.get(carrier, COMBINED_HEADER);
    if (StringUtils.isNullOrEmpty(value)) {
      logger.info(
          "Missing or empty combined header: "
              + COMBINED_HEADER
              + ". Returning INVALID span context.");
      return SpanContext.getInvalid();
    }

    // must have between 2 and 4 hyphen delimieted parts:
    //   traceId-spanId-sampled-parentSpanId (last two are optional)
    // NOTE: we do not use parentSpanId
    String[] parts = value.split(COMBINED_HEADER_DELIMITER);
    if (parts.length < 2 || parts.length > 4) {
      logger.info(
          "Invalid combined header '" + COMBINED_HEADER + ". Returning INVALID span context.");
      return SpanContext.getInvalid();
    }

    String traceId = parts[0];
    if (!isTraceIdValid(traceId)) {
      logger.info(
          "Invalid TraceId in B3 header: " + COMBINED_HEADER + ". Returning INVALID span context.");
      return SpanContext.getInvalid();
    }

    String spanId = parts[1];
    if (!isSpanIdValid(spanId)) {
      logger.info(
          "Invalid SpanId in B3 header: " + COMBINED_HEADER + ". Returning INVALID span context.");
      return SpanContext.getInvalid();
    }

    String sampled = parts.length >= 3 ? parts[2] : null;

    return buildSpanContext(traceId, spanId, sampled);
  }

  private static <C> SpanContext getSpanContextFromMultipleHeaders(
      C carrier, HttpTextFormat.Getter<C> getter) {
    String traceId = getter.get(carrier, TRACE_ID_HEADER);
    if (!isTraceIdValid(traceId)) {
      logger.info(
          "Invalid TraceId in B3 header: "
              + TRACE_ID_HEADER
              + "'. Returning INVALID span context.");
      return SpanContext.getInvalid();
    }

    String spanId = getter.get(carrier, SPAN_ID_HEADER);
    if (!isSpanIdValid(spanId)) {
      logger.info(
          "Invalid SpanId in B3 header: " + SPAN_ID_HEADER + "'. Returning INVALID span context.");
      return SpanContext.getInvalid();
    }

    String sampled = getter.get(carrier, SAMPLED_HEADER);
    return buildSpanContext(traceId, spanId, sampled);
  }

  private static SpanContext buildSpanContext(String traceId, String spanId, String sampled) {
    try {
      TraceFlags traceFlags =
          TRUE_INT.equals(sampled) || Boolean.parseBoolean(sampled) // accept either "1" or "true"
              ? SAMPLED_FLAGS
              : NOT_SAMPLED_FLAGS;

      return SpanContext.createFromRemoteParent(
          TraceId.fromLowerBase16(StringUtils.padLeft(traceId, MAX_TRACE_ID_LENGTH), 0),
          SpanId.fromLowerBase16(spanId, 0),
          traceFlags,
          TraceState.getDefault());
    } catch (Exception e) {
      logger.log(Level.INFO, "Error parsing B3 header. Returning INVALID span context.", e);
      return SpanContext.getInvalid();
    }
  }

  private static boolean isTraceIdValid(String value) {
    return !(StringUtils.isNullOrEmpty(value) || value.length() > MAX_TRACE_ID_LENGTH);
  }

  private static boolean isSpanIdValid(String value) {
    return !(StringUtils.isNullOrEmpty(value) || value.length() > MAX_SPAN_ID_LENGTH);
  }
}
