/*
 * Copyright 2019, OpenTelemetry Authors
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

package io.opentelemetry.sdk.trace;

import io.opentelemetry.sdk.common.InstrumentationLibraryInfo;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.trace.Span;
import io.opentelemetry.trace.TraceState;

/** The extend Span interface used by the SDK. */
public interface ReadableSpan {

  /** Returns the trace ID of this {@link Span}. */
  String getTraceIdAsHexString();

  String getSpanIdAsHexString();

  byte getTraceFlags();

  TraceState getTraceState();

  boolean isSampled();

  /**
   * Returns the name of the {@code Span}.
   *
   * <p>The name can be changed during the lifetime of the Span by using the {@link
   * Span#updateName(String)} so this value cannot be cached.
   *
   * @return the name of the {@code Span}.
   */
  String getName();

  /**
   * This converts this instance into an immutable SpanData instance, for use in export.
   *
   * @return an immutable {@link SpanData} instance.
   */
  SpanData toSpanData();

  /**
   * Returns the instrumentation library specified when creating the tracer which produced this
   * span.
   *
   * @return an instance of {@link InstrumentationLibraryInfo} describing the instrumentation
   *     library
   */
  InstrumentationLibraryInfo getInstrumentationLibraryInfo();

  /**
   * Returns whether this Span has already been ended.
   *
   * @return {@code true} if the span has already been ended, {@code false} if not.
   */
  boolean hasEnded();

  /**
   * Returns the latency of the {@code Span} in nanos. If still active then returns now() - start
   * time.
   *
   * @return the latency of the {@code Span} in nanos.
   */
  long getLatencyNanos();
}
