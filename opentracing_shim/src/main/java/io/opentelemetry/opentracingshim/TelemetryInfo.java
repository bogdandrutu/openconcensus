/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.opentracingshim;

import io.opentelemetry.baggage.Baggage;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.trace.Tracer;

/**
 * Utility class that holds a Tracer, a BaggageManager, and related objects that are core part of
 * the OT Shim layer.
 */
final class TelemetryInfo {

  private final Tracer tracer;
  private final Baggage emptyBaggage;
  private final ContextPropagators propagators;
  private final SpanContextShimTable spanContextTable;

  TelemetryInfo(Tracer tracer, ContextPropagators propagators) {
    this.tracer = tracer;
    this.propagators = propagators;
    this.emptyBaggage = Baggage.empty();
    this.spanContextTable = new SpanContextShimTable();
  }

  Tracer tracer() {
    return tracer;
  }

  SpanContextShimTable spanContextTable() {
    return spanContextTable;
  }

  Baggage emptyBaggage() {
    return emptyBaggage;
  }

  ContextPropagators propagators() {
    return propagators;
  }
}
