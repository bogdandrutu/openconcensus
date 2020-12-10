/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.metrics;

import io.opentelemetry.sdk.metrics.data.MetricData;
import java.util.List;

abstract class AbstractAsynchronousInstrument extends AbstractInstrument {
  private final AsynchronousInstrumentAccumulator accumulator;

  AbstractAsynchronousInstrument(
      InstrumentDescriptor descriptor, AsynchronousInstrumentAccumulator accumulator) {
    super(descriptor);
    this.accumulator = accumulator;
  }

  @Override
  public final List<MetricData> collectAll() {
    return accumulator.collectAll();
  }
}
