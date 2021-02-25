/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.metrics.aggregator;

import io.opentelemetry.sdk.common.InstrumentationLibraryInfo;
import io.opentelemetry.sdk.metrics.common.InstrumentDescriptor;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.resources.Resource;

final class HistogramAggregatorFactory implements AggregatorFactory {
  private final ImmutableDoubleArray boundaries;
  private final AggregationTemporality temporality;

  HistogramAggregatorFactory(double[] boundaries, AggregationTemporality temporality) {
    this.boundaries = ImmutableDoubleArray.copyOf(boundaries);
    this.temporality = temporality;

    for (int i = 1; i < this.boundaries.length(); ++i) {
      if (Double.compare(this.boundaries.get(i - 1), this.boundaries.get(i)) >= 0) {
        throw new IllegalArgumentException(
            "invalid bucket boundary: "
                + this.boundaries.get(i - 1)
                + " >= "
                + this.boundaries.get(i));
      }
    }
    if (this.boundaries.length() > 0) {
      if (this.boundaries.get(0) == Double.NEGATIVE_INFINITY) {
        throw new IllegalArgumentException("invalid bucket boundary: -Inf");
      }
      if (this.boundaries.get(this.boundaries.length() - 1) == Double.POSITIVE_INFINITY) {
        throw new IllegalArgumentException("invalid bucket boundary: +Inf");
      }
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> Aggregator<T> create(
      Resource resource,
      InstrumentationLibraryInfo instrumentationLibraryInfo,
      InstrumentDescriptor descriptor) {
    final boolean stateful = this.temporality == AggregationTemporality.CUMULATIVE;
    switch (descriptor.getValueType()) {
      case LONG:
      case DOUBLE:
        return (Aggregator<T>)
            new DoubleHistogramAggregator(
                resource, instrumentationLibraryInfo, descriptor, this.boundaries, stateful);
    }
    throw new IllegalArgumentException("Invalid instrument value type");
  }
}
