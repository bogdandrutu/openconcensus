/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.api.common.Labels;
import io.opentelemetry.sdk.common.InstrumentationLibraryInfo;
import io.opentelemetry.sdk.internal.TestClock;
import io.opentelemetry.sdk.metrics.aggregation.Aggregations;
import io.opentelemetry.sdk.metrics.aggregator.Aggregator;
import io.opentelemetry.sdk.metrics.common.InstrumentDescriptor;
import io.opentelemetry.sdk.metrics.common.InstrumentType;
import io.opentelemetry.sdk.metrics.common.InstrumentValueType;
import io.opentelemetry.sdk.resources.Resource;
import org.junit.jupiter.api.Test;

public class SynchronousInstrumentAccumulatorTest {
  private static final InstrumentDescriptor DESCRIPTOR =
      InstrumentDescriptor.create(
          "name", "description", "unit", InstrumentType.COUNTER, InstrumentValueType.DOUBLE);
  private final MeterProviderSharedState providerSharedState =
      MeterProviderSharedState.create(TestClock.create(), Resource.getEmpty());
  private final MeterSharedState meterSharedState =
      MeterSharedState.create(InstrumentationLibraryInfo.create("test", "1.0"));

  @Test
  void sameAggregator_ForSameLabelSet() {
    SynchronousInstrumentAccumulator accumulator =
        new SynchronousInstrumentAccumulator(
            InstrumentProcessor.getCumulativeAllLabels(
                DESCRIPTOR, providerSharedState, meterSharedState, Aggregations.count()));
    Aggregator aggregator = accumulator.bind(Labels.of("K", "V"));
    Aggregator duplicateAggregator = accumulator.bind(Labels.of("K", "V"));
    try {
      assertThat(duplicateAggregator).isSameAs(aggregator);
      accumulator.collectAll();
      Aggregator anotherDuplicateAggregator = accumulator.bind(Labels.of("K", "V"));
      try {
        assertThat(anotherDuplicateAggregator).isEqualTo(aggregator);
      } finally {
        anotherDuplicateAggregator.release();
      }
    } finally {
      aggregator.release();
      aggregator.release();
    }
  }
}
