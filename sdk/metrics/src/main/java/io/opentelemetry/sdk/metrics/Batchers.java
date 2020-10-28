/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.metrics;

import io.opentelemetry.api.common.Labels;
import io.opentelemetry.sdk.common.Clock;
import io.opentelemetry.sdk.common.InstrumentationLibraryInfo;
import io.opentelemetry.sdk.metrics.aggregator.Aggregator;
import io.opentelemetry.sdk.metrics.aggregator.AggregatorFactory;
import io.opentelemetry.sdk.metrics.aggregator.NoopAggregator;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.MetricData.Point;
import io.opentelemetry.sdk.metrics.view.Aggregation;
import io.opentelemetry.sdk.resources.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A collection of available Batchers. */
final class Batchers {

  static Batcher getNoop() {
    return Noop.INSTANCE;
  }

  /**
   * Create a Batcher that uses the "cumulative" Temporality and uses all labels for aggregation.
   * "Cumulative" means that all metrics that are generated will be considered for the lifetime of
   * the Instrument being aggregated.
   */
  static Batcher getCumulativeAllLabels(
      InstrumentDescriptor descriptor,
      MeterProviderSharedState meterProviderSharedState,
      MeterSharedState meterSharedState,
      Aggregation aggregation) {
    return new AllLabels(
        descriptor,
        aggregation,
        meterProviderSharedState.getResource(),
        meterSharedState.getInstrumentationLibraryInfo(),
        meterProviderSharedState.getClock(),
        /* delta= */ false);
  }

  /**
   * Create a Batcher that uses the "delta" Temporality and uses all labels for aggregation. "Delta"
   * means that all metrics that are generated are only for the most recent collection interval.
   */
  static Batcher getDeltaAllLabels(
      InstrumentDescriptor descriptor,
      MeterProviderSharedState meterProviderSharedState,
      MeterSharedState meterSharedState,
      Aggregation aggregation) {
    return new AllLabels(
        descriptor,
        aggregation,
        meterProviderSharedState.getResource(),
        meterSharedState.getInstrumentationLibraryInfo(),
        meterProviderSharedState.getClock(),
        /* delta= */ true);
  }

  private static final class Noop implements Batcher {
    private static final Noop INSTANCE = new Noop();

    @Override
    public Aggregator getAggregator() {
      return NoopAggregator.getFactory().getAggregator();
    }

    @Override
    public void batch(Labels labelSet, Aggregator aggregator, boolean mappedAggregator) {}

    @Override
    public List<MetricData> completeCollectionCycle() {
      return Collections.emptyList();
    }
  }

  private static final class AllLabels implements Batcher {
    private final InstrumentDescriptor descriptor;
    private final Aggregation aggregation;
    private final Resource resource;
    private final InstrumentationLibraryInfo instrumentationLibraryInfo;
    private final Clock clock;
    private final AggregatorFactory aggregatorFactory;
    private Map<Labels, Aggregator> aggregatorMap;
    private long startEpochNanos;
    private final boolean delta;

    private AllLabels(
        InstrumentDescriptor descriptor,
        Aggregation aggregation,
        Resource resource,
        InstrumentationLibraryInfo instrumentationLibraryInfo,
        Clock clock,
        boolean delta) {
      this.descriptor = descriptor;
      this.aggregation = aggregation;
      this.resource = resource;
      this.instrumentationLibraryInfo = instrumentationLibraryInfo;
      this.clock = clock;
      this.aggregatorFactory = aggregation.getAggregatorFactory(descriptor.getValueType());
      this.delta = delta;
      this.aggregatorMap = new HashMap<>();
      startEpochNanos = clock.now();
    }

    @Override
    public final Aggregator getAggregator() {
      return aggregatorFactory.getAggregator();
    }

    @Override
    public final void batch(Labels labelSet, Aggregator aggregator, boolean unmappedAggregator) {
      Aggregator currentAggregator = aggregatorMap.get(labelSet);
      if (currentAggregator == null) {
        // This aggregator is not mapped, we can use this instance.
        if (unmappedAggregator) {
          aggregatorMap.put(labelSet, aggregator);
          return;
        }
        currentAggregator = aggregatorFactory.getAggregator();
        aggregatorMap.put(labelSet, currentAggregator);
      }
      aggregator.mergeToAndReset(currentAggregator);
    }

    @Override
    public final List<MetricData> completeCollectionCycle() {
      List<Point> points = new ArrayList<>(aggregatorMap.size());
      long epochNanos = clock.now();
      for (Map.Entry<Labels, Aggregator> entry : aggregatorMap.entrySet()) {
        Point point = entry.getValue().toPoint(startEpochNanos, epochNanos, entry.getKey());
        if (point != null) {
          points.add(point);
        }
      }
      if (delta) {
        startEpochNanos = epochNanos;
        aggregatorMap = new HashMap<>();
      }
      return Collections.singletonList(
          MetricData.create(
              resource,
              instrumentationLibraryInfo,
              descriptor.getName(),
              descriptor.getDescription(),
              aggregation.getUnit(descriptor.getUnit()),
              aggregation.getDescriptorType(descriptor.getType(), descriptor.getValueType()),
              points));
    }
  }

  private Batchers() {}
}
