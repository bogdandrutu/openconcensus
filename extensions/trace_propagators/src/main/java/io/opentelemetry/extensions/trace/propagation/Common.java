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

import io.opentelemetry.trace.SpanContext;
import io.opentelemetry.trace.SpanId;
import io.opentelemetry.trace.TraceFlags;
import io.opentelemetry.trace.TraceId;
import io.opentelemetry.trace.TraceState;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.concurrent.Immutable;

/**
 * This set of common propagator utils is currently only used by the OtTracerPropagator and the
 * B3Propagator.
 */
@Immutable
final class Common {
  private static final Logger logger = Logger.getLogger(Common.class.getName());

  static final String TRUE_INT = "1";
  static final String FALSE_INT = "0";
  static final int MAX_TRACE_ID_LENGTH = TraceId.getHexLength();
  static final int MAX_SPAN_ID_LENGTH = SpanId.getHexLength();
  static final int MIN_TRACE_ID_LENGTH = TraceId.getSize();
  private static final TraceFlags SAMPLED_FLAGS = TraceFlags.builder().setIsSampled(true).build();
  private static final TraceFlags NOT_SAMPLED_FLAGS =
      TraceFlags.builder().setIsSampled(false).build();

  private Common() {}

  static SpanContext buildSpanContext(String traceId, String spanId, String sampled) {
    try {
      TraceFlags traceFlags =
          TRUE_INT.equals(sampled) || Boolean.parseBoolean(sampled) // accept either "1" or "true"
              ? SAMPLED_FLAGS
              : NOT_SAMPLED_FLAGS;

      return SpanContext.createFromRemoteParent(
          StringUtils.padLeft(traceId, MAX_TRACE_ID_LENGTH),
          spanId,
          traceFlags,
          TraceState.getDefault());
    } catch (Exception e) {
      logger.log(Level.INFO, "Error parsing header. Returning INVALID span context.", e);
      return SpanContext.getInvalid();
    }
  }

  static boolean isTraceIdValid(String value) {
    return !(StringUtils.isNullOrEmpty(value)
        || (value.length() != MIN_TRACE_ID_LENGTH && value.length() != MAX_TRACE_ID_LENGTH)
        || !TraceId.isValid(StringUtils.padLeft(value, TraceId.getHexLength())));
  }

  static boolean isSpanIdValid(String value) {
    return !StringUtils.isNullOrEmpty(value) && SpanId.isValid(value);
  }
}
