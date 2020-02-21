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

package io.opentelemetry.sdk.metrics.view;

import io.opentelemetry.sdk.metrics.aggregator.AggregatorFactory;
import io.opentelemetry.sdk.metrics.aggregator.DoubleSumAggregator;
import io.opentelemetry.sdk.metrics.aggregator.LongSumAggregator;
import io.opentelemetry.sdk.metrics.aggregator.NoopAggregator;
import io.opentelemetry.sdk.metrics.common.InstrumentType;
import io.opentelemetry.sdk.metrics.common.InstrumentValueType;
import io.opentelemetry.sdk.metrics.data.MetricData.Descriptor.Type;
import javax.annotation.concurrent.Immutable;

public class Aggregations {

  /**
   * Returns an {@code Aggregation} that calculates sum of recorded measurements.
   *
   * @return an {@code Aggregation} that calculates sum of recorded measurements.
   * @since 0.1.0
   */
  public static Aggregation sum() {
    return Sum.INSTANCE;
  }

  /**
   * Returns an {@code Aggregation} that calculates count of recorded measurements (the number of
   * recorded measurements).
   *
   * @return an {@code Aggregation} that calculates count of recorded measurements (the number of
   *     recorded * measurements).
   * @since 0.1.0
   */
  public static Aggregation count() {
    return Count.INSTANCE;
  }

  /**
   * Returns an {@code Aggregation} that calculates distribution stats on recorded measurements.
   * Distribution includes sum, count, histogram, and sum of squared deviations.
   *
   * <p>The boundaries for the buckets in the underlying histogram needs to be sorted.
   *
   * @param bucketBoundaries bucket boundaries to use for distribution.
   * @return an {@code Aggregation} that calculates distribution stats on recorded measurements.
   * @since 0.1.0
   */
  public static Aggregation distributionWithExplicitBounds(Double... bucketBoundaries) {
    return new Distribution(bucketBoundaries);
  }

  /**
   * Returns an {@code Aggregation} that calculates the last value of all recorded measurements.
   *
   * @return an {@code Aggregation} that calculates the last value of all recorded measurements.
   * @since 0.1.0
   */
  public static Aggregation lastValue() {
    return LastValue.INSTANCE;
  }

  @Immutable
  private enum Sum implements Aggregation {
    INSTANCE;

    @Override
    public AggregatorFactory getAggregatorFactory(InstrumentValueType instrumentValueType) {
      return instrumentValueType == InstrumentValueType.LONG
          ? LongSumAggregator.getFactory()
          : DoubleSumAggregator.getFactory();
    }

    @Override
    public Type getDescriptorType(
        InstrumentType instrumentType, InstrumentValueType instrumentValueType) {
      switch (instrumentType) {
        case COUNTER_MONOTONIC:
        case MEASURE_ABSOLUTE:
        case OBSERVER_MONOTONIC:
          return instrumentValueType == InstrumentValueType.LONG
              ? Type.MONOTONIC_LONG
              : Type.MONOTONIC_DOUBLE;
        case COUNTER_NON_MONOTONIC:
        case MEASURE_NON_ABSOLUTE:
        case OBSERVER_NON_MONOTONIC:
          return instrumentValueType == InstrumentValueType.LONG
              ? Type.NON_MONOTONIC_LONG
              : Type.NON_MONOTONIC_DOUBLE;
      }
      throw new IllegalArgumentException("Unsupported instrument/value types");
    }

    @Override
    public boolean availableForInstrument(InstrumentType instrumentType) {
      // Available for all instruments.
      return true;
    }
  }

  @Immutable
  private enum Count implements Aggregation {
    INSTANCE;

    @Override
    public AggregatorFactory getAggregatorFactory(InstrumentValueType instrumentValueType) {
      // TODO: Implement count aggregator and use it here.
      return NoopAggregator.getFactory();
    }

    @Override
    public Type getDescriptorType(
        InstrumentType instrumentType, InstrumentValueType instrumentValueType) {
      return Type.MONOTONIC_LONG;
    }

    @Override
    public boolean availableForInstrument(InstrumentType instrumentType) {
      // Available for all instruments.
      return true;
    }
  }

  @Immutable
  private static final class Distribution implements Aggregation {
    private final AggregatorFactory factory;

    Distribution(Double... bucketBoundaries) {
      // TODO: Implement distribution aggregator and use it here.
      this.factory = NoopAggregator.getFactory();
    }

    @Override
    public AggregatorFactory getAggregatorFactory(InstrumentValueType instrumentValueType) {
      return factory;
    }

    @Override
    public Type getDescriptorType(
        InstrumentType instrumentType, InstrumentValueType instrumentValueType) {
      throw new UnsupportedOperationException("Implement this");
    }

    @Override
    public boolean availableForInstrument(InstrumentType instrumentType) {
      throw new UnsupportedOperationException("Implement this");
    }
  }

  @Immutable
  private enum LastValue implements Aggregation {
    INSTANCE;

    @Override
    public AggregatorFactory getAggregatorFactory(InstrumentValueType instrumentValueType) {
      // TODO: Implement LastValue aggregator and use it here.
      return NoopAggregator.getFactory();
    }

    @Override
    public Type getDescriptorType(
        InstrumentType instrumentType, InstrumentValueType instrumentValueType) {
      throw new UnsupportedOperationException("Implement this");
    }

    @Override
    public boolean availableForInstrument(InstrumentType instrumentType) {
      throw new UnsupportedOperationException("Implement this");
    }
  }

  private Aggregations() {}
}
