/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.trace;

import io.opentelemetry.api.trace.SpanId;
import io.opentelemetry.api.trace.TraceId;

/** Interface used by the {@link TracerSdk} to generate new {@link SpanId}s and {@link TraceId}s. */
public interface IdsGenerator {
  /**
   * Generates a new valid {@code SpanId}.
   *
   * @return a new valid {@code SpanId}.
   */
  String generateSpanId();

  /**
   * Generates a new valid {@code TraceId}.
   *
   * @return a new valid {@code TraceId}.
   */
  String generateTraceId();
}
