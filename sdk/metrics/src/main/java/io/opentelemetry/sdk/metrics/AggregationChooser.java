/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.metrics;

import io.opentelemetry.sdk.metrics.common.InstrumentDescriptor;
import io.opentelemetry.sdk.metrics.common.InstrumentType;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.view.AggregationConfiguration;
import io.opentelemetry.sdk.metrics.view.Aggregations;
import io.opentelemetry.sdk.metrics.view.InstrumentSelector;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

class AggregationChooser {
  private static final LinkedHashMap<Pattern, AggregationConfiguration> EMPTY_CONFIG =
      new LinkedHashMap<>();
  private static final AggregationConfiguration CUMULATIVE_SUM =
      AggregationConfiguration.create(
          Aggregations.sum(), MetricData.AggregationTemporality.CUMULATIVE);
  private static final AggregationConfiguration DELTA_SUMMARY =
      AggregationConfiguration.create(
          Aggregations.minMaxSumCount(), MetricData.AggregationTemporality.DELTA);
  private static final AggregationConfiguration CUMULATIVE_LAST_VALUE =
      AggregationConfiguration.create(
          Aggregations.lastValue(), MetricData.AggregationTemporality.CUMULATIVE);
  private static final AggregationConfiguration DELTA_LAST_VALUE =
      AggregationConfiguration.create(
          Aggregations.lastValue(), MetricData.AggregationTemporality.DELTA);

  private final ReentrantLock collectLock = new ReentrantLock();
  private volatile EnumMap<InstrumentType, LinkedHashMap<Pattern, AggregationConfiguration>>
      configuration;

  AggregationChooser() {
    this.configuration = new EnumMap<>(InstrumentType.class);

    configuration.put(InstrumentType.COUNTER, EMPTY_CONFIG);
    configuration.put(InstrumentType.UP_DOWN_COUNTER, EMPTY_CONFIG);
    configuration.put(InstrumentType.VALUE_RECORDER, EMPTY_CONFIG);
    configuration.put(InstrumentType.SUM_OBSERVER, EMPTY_CONFIG);
    configuration.put(InstrumentType.UP_DOWN_SUM_OBSERVER, EMPTY_CONFIG);
    configuration.put(InstrumentType.VALUE_OBSERVER, EMPTY_CONFIG);
  }

  AggregationConfiguration chooseAggregation(InstrumentDescriptor descriptor) {
    LinkedHashMap<Pattern, AggregationConfiguration> configPerType =
        configuration.get(descriptor.getType());
    for (Map.Entry<Pattern, AggregationConfiguration> entry : configPerType.entrySet()) {
      // if it matches everything, return it right away...
      if (entry.getKey().matcher(descriptor.getName()).matches()) {
        return entry.getValue();
      }
    }

    return getDefaultSpecification(descriptor);
  }

  private static AggregationConfiguration getDefaultSpecification(InstrumentDescriptor descriptor) {
    switch (descriptor.getType()) {
      case COUNTER:
      case UP_DOWN_COUNTER:
        return CUMULATIVE_SUM;
      case VALUE_RECORDER:
        return DELTA_SUMMARY;
      case VALUE_OBSERVER:
        return DELTA_LAST_VALUE;
      case SUM_OBSERVER:
      case UP_DOWN_SUM_OBSERVER:
        return CUMULATIVE_LAST_VALUE;
    }
    throw new IllegalArgumentException("Unknown descriptor type: " + descriptor.getType());
  }

  void addView(InstrumentSelector selector, AggregationConfiguration specification) {
    collectLock.lock();
    try {
      EnumMap<InstrumentType, LinkedHashMap<Pattern, AggregationConfiguration>> newConfiguration =
          new EnumMap<>(configuration);
      newConfiguration.put(
          selector.getInstrumentType(),
          newLinkedHashMap(
              selector.getInstrumentNamePattern(),
              specification,
              newConfiguration.get(selector.getInstrumentType())));
      configuration = newConfiguration;
    } finally {
      collectLock.unlock();
    }
  }

  private static LinkedHashMap<Pattern, AggregationConfiguration> newLinkedHashMap(
      Pattern pattern,
      AggregationConfiguration aggregationConfiguration,
      LinkedHashMap<Pattern, AggregationConfiguration> parentConfiguration) {
    LinkedHashMap<Pattern, AggregationConfiguration> result = new LinkedHashMap<>();
    result.put(pattern, aggregationConfiguration);
    result.putAll(parentConfiguration);
    return result;
  }
}
