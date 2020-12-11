/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.api;

import io.opentelemetry.api.trace.TracerProvider;
import io.opentelemetry.context.propagation.ContextPropagators;

/**
 * A builder of an implementation of the OpenTelemetry API. Generally used to reconfigure SDK
 * implementations.
 */
public interface OpenTelemetryBuilder<T extends OpenTelemetryBuilder<T>> {

  /** Sets the {@link TracerProvider} to use. */
  T setTracerProvider(TracerProvider tracerProvider);

  /** Sets the {@link ContextPropagators} to use. */
  T setPropagators(ContextPropagators propagators);

  /**
   * Returns a new {@link OpenTelemetry} based on the configuration in this {@link
   * OpenTelemetryBuilder}.
   */
  OpenTelemetry build();
}
