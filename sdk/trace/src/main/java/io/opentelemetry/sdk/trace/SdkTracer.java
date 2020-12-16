/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.trace;

import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.common.InstrumentationLibraryInfo;

/** {@link SdkTracer} is SDK implementation of {@link Tracer}. */
final class SdkTracer implements Tracer {
  private final TracerSharedState sharedState;
  private final InstrumentationLibraryInfo instrumentationLibraryInfo;

  SdkTracer(TracerSharedState sharedState, InstrumentationLibraryInfo instrumentationLibraryInfo) {
    this.sharedState = sharedState;
    this.instrumentationLibraryInfo = instrumentationLibraryInfo;
  }

  @Override
  public SpanBuilder spanBuilder(String spanName) {
    if (sharedState.isStopped()) {
      return Tracer.getDefault().spanBuilder(spanName);
    }
    return new SdkSpanBuilder(
        spanName,
        instrumentationLibraryInfo,
        sharedState.getActiveSpanProcessor(),
        sharedState.getActiveTraceConfig(),
        sharedState.getResource(),
        sharedState.getIdGenerator(),
        sharedState.getClock());
  }

  /**
   * Returns the instrumentation library specified when creating the tracer.
   *
   * @return an instance of {@link InstrumentationLibraryInfo}
   */
  InstrumentationLibraryInfo getInstrumentationLibraryInfo() {
    return instrumentationLibraryInfo;
  }
}
