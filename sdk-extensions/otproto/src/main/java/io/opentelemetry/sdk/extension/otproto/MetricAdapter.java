/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.otproto;

import static io.opentelemetry.proto.metrics.v1.AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE;
import static io.opentelemetry.proto.metrics.v1.AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA;
import static io.opentelemetry.proto.metrics.v1.AggregationTemporality.AGGREGATION_TEMPORALITY_UNSPECIFIED;

import io.opentelemetry.api.common.Labels;
import io.opentelemetry.proto.common.v1.StringKeyValue;
import io.opentelemetry.proto.metrics.v1.AggregationTemporality;
import io.opentelemetry.proto.metrics.v1.DoubleDataPoint;
import io.opentelemetry.proto.metrics.v1.DoubleGauge;
import io.opentelemetry.proto.metrics.v1.DoubleHistogram;
import io.opentelemetry.proto.metrics.v1.DoubleHistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.DoubleSum;
import io.opentelemetry.proto.metrics.v1.InstrumentationLibraryMetrics;
import io.opentelemetry.proto.metrics.v1.IntDataPoint;
import io.opentelemetry.proto.metrics.v1.IntGauge;
import io.opentelemetry.proto.metrics.v1.IntSum;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.sdk.common.InstrumentationLibraryInfo;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.resources.Resource;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Converter from SDK {@link MetricData} to OTLP {@link ResourceMetrics}. */
public final class MetricAdapter {

  /** Converts the provided {@link MetricData} to {@link ResourceMetrics}. */
  public static List<ResourceMetrics> toProtoResourceMetrics(Collection<MetricData> metricData) {
    Map<Resource, Map<InstrumentationLibraryInfo, List<Metric>>> resourceAndLibraryMap =
        groupByResourceAndLibrary(metricData);
    List<ResourceMetrics> resourceMetrics = new ArrayList<>(resourceAndLibraryMap.size());
    for (Map.Entry<Resource, Map<InstrumentationLibraryInfo, List<Metric>>> entryResource :
        resourceAndLibraryMap.entrySet()) {
      List<InstrumentationLibraryMetrics> instrumentationLibraryMetrics =
          new ArrayList<>(entryResource.getValue().size());
      for (Map.Entry<InstrumentationLibraryInfo, List<Metric>> entryLibrary :
          entryResource.getValue().entrySet()) {
        instrumentationLibraryMetrics.add(
            InstrumentationLibraryMetrics.newBuilder()
                .setInstrumentationLibrary(
                    CommonAdapter.toProtoInstrumentationLibrary(entryLibrary.getKey()))
                .addAllMetrics(entryLibrary.getValue())
                .build());
      }
      resourceMetrics.add(
          ResourceMetrics.newBuilder()
              .setResource(ResourceAdapter.toProtoResource(entryResource.getKey()))
              .addAllInstrumentationLibraryMetrics(instrumentationLibraryMetrics)
              .build());
    }
    return resourceMetrics;
  }

  private static Map<Resource, Map<InstrumentationLibraryInfo, List<Metric>>>
      groupByResourceAndLibrary(Collection<MetricData> metricDataList) {
    Map<Resource, Map<InstrumentationLibraryInfo, List<Metric>>> result = new HashMap<>();
    for (MetricData metricData : metricDataList) {
      if (metricData.isEmpty()) {
        // If no points available then ignore.
        continue;
      }

      Resource resource = metricData.getResource();
      Map<InstrumentationLibraryInfo, List<Metric>> libraryInfoListMap =
          result.get(metricData.getResource());
      if (libraryInfoListMap == null) {
        libraryInfoListMap = new HashMap<>();
        result.put(resource, libraryInfoListMap);
      }
      List<Metric> metricList =
          libraryInfoListMap.computeIfAbsent(
              metricData.getInstrumentationLibraryInfo(), k -> new ArrayList<>());
      metricList.add(toProtoMetric(metricData));
    }
    return result;
  }

  // fall through comment isn't working for some reason.
  @SuppressWarnings("fallthrough")
  static Metric toProtoMetric(MetricData metricData) {
    Metric.Builder builder =
        Metric.newBuilder()
            .setName(metricData.getName())
            .setDescription(metricData.getDescription())
            .setUnit(metricData.getUnit());

    switch (metricData.getType()) {
      case LONG_SUM:
        MetricData.LongSumData longSumData = metricData.getLongSumData();
        builder.setIntSum(
            IntSum.newBuilder()
                .setIsMonotonic(longSumData.isMonotonic())
                .setAggregationTemporality(
                    mapToTemporality(longSumData.getAggregationTemporality()))
                .addAllDataPoints(toIntDataPoints(longSumData.getPoints()))
                .build());
        break;
      case DOUBLE_SUM:
        MetricData.DoubleSumData doubleSumData = metricData.getDoubleSumData();
        builder.setDoubleSum(
            DoubleSum.newBuilder()
                .setIsMonotonic(doubleSumData.isMonotonic())
                .setAggregationTemporality(
                    mapToTemporality(doubleSumData.getAggregationTemporality()))
                .addAllDataPoints(toDoubleDataPoints(doubleSumData.getPoints()))
                .build());
        break;
      case SUMMARY:
        MetricData.DoubleSummaryData doubleSummaryData = metricData.getDoubleSummaryData();
        builder.setDoubleHistogram(
            DoubleHistogram.newBuilder()
                // TODO: This is a bug, but preserve the logic and fix it later.
                .setAggregationTemporality(AGGREGATION_TEMPORALITY_DELTA)
                .addAllDataPoints(toSummaryDataPoints(doubleSummaryData.getPoints()))
                .build());
        break;
      case LONG_GAUGE:
        MetricData.LongGaugeData longGaugeData = metricData.getLongGaugeData();
        builder.setIntGauge(
            IntGauge.newBuilder()
                .addAllDataPoints(toIntDataPoints(longGaugeData.getPoints()))
                .build());
        break;
      case DOUBLE_GAUGE:
        MetricData.DoubleGaugeData doubleGaugeData = metricData.getDoubleGaugeData();
        builder.setDoubleGauge(
            DoubleGauge.newBuilder()
                .addAllDataPoints(toDoubleDataPoints(doubleGaugeData.getPoints()))
                .build());
        break;
    }
    return builder.build();
  }

  private static AggregationTemporality mapToTemporality(
      MetricData.AggregationTemporality temporality) {
    switch (temporality) {
      case CUMULATIVE:
        return AGGREGATION_TEMPORALITY_CUMULATIVE;
      case DELTA:
        return AGGREGATION_TEMPORALITY_DELTA;
    }
    return AGGREGATION_TEMPORALITY_UNSPECIFIED;
  }

  static List<IntDataPoint> toIntDataPoints(Collection<MetricData.Point> points) {
    List<IntDataPoint> result = new ArrayList<>(points.size());
    for (MetricData.Point point : points) {
      MetricData.LongPoint longPoint = (MetricData.LongPoint) point;
      IntDataPoint.Builder builder =
          IntDataPoint.newBuilder()
              .setStartTimeUnixNano(longPoint.getStartEpochNanos())
              .setTimeUnixNano(longPoint.getEpochNanos())
              .setValue(longPoint.getValue());
      Collection<StringKeyValue> labels = toProtoLabels(longPoint.getLabels());
      if (!labels.isEmpty()) {
        builder.addAllLabels(labels);
      }
      result.add(builder.build());
    }
    return result;
  }

  static Collection<DoubleDataPoint> toDoubleDataPoints(Collection<MetricData.Point> points) {
    List<DoubleDataPoint> result = new ArrayList<>(points.size());
    for (MetricData.Point point : points) {
      MetricData.DoublePoint doublePoint = (MetricData.DoublePoint) point;
      DoubleDataPoint.Builder builder =
          DoubleDataPoint.newBuilder()
              .setStartTimeUnixNano(point.getStartEpochNanos())
              .setTimeUnixNano(point.getEpochNanos())
              .setValue(doublePoint.getValue());
      Collection<StringKeyValue> labels = toProtoLabels(point.getLabels());
      if (!labels.isEmpty()) {
        builder.addAllLabels(labels);
      }
      result.add(builder.build());
    }
    return result;
  }

  static List<DoubleHistogramDataPoint> toSummaryDataPoints(Collection<MetricData.Point> points) {
    List<DoubleHistogramDataPoint> result = new ArrayList<>(points.size());
    for (MetricData.Point point : points) {
      MetricData.DoubleSummaryPoint doubleSummaryPoint = (MetricData.DoubleSummaryPoint) point;
      DoubleHistogramDataPoint.Builder builder =
          DoubleHistogramDataPoint.newBuilder()
              .setStartTimeUnixNano(point.getStartEpochNanos())
              .setTimeUnixNano(point.getEpochNanos())
              .setCount(doubleSummaryPoint.getCount())
              .setSum(doubleSummaryPoint.getSum());
      List<StringKeyValue> labels = toProtoLabels(point.getLabels());
      if (!labels.isEmpty()) {
        builder.addAllLabels(labels);
      }
      // Not calling directly addAllPercentileValues because that generates couple of unnecessary
      // allocations if empty list.
      if (!doubleSummaryPoint.getPercentileValues().isEmpty()) {
        addBucketValues(doubleSummaryPoint.getPercentileValues(), builder);
      }
      result.add(builder.build());
    }
    return result;
  }

  // TODO: Consider to pass the Builder and directly add values.
  @SuppressWarnings("MixedMutabilityReturnType")
  static void addBucketValues(
      List<MetricData.ValueAtPercentile> valueAtPercentiles,
      DoubleHistogramDataPoint.Builder builder) {

    for (MetricData.ValueAtPercentile valueAtPercentile : valueAtPercentiles) {
      // TODO(jkwatson): Value of histogram should be long?
      builder.addBucketCounts((long) valueAtPercentile.getValue());
      builder.addExplicitBounds(valueAtPercentile.getPercentile());
    }

    // No recordings past the highest percentile (e.g., [highest percentile, +infinity]).
    builder.addBucketCounts(0);
  }

  @SuppressWarnings("MixedMutabilityReturnType")
  static List<StringKeyValue> toProtoLabels(Labels labels) {
    if (labels.isEmpty()) {
      return Collections.emptyList();
    }
    final List<StringKeyValue> result = new ArrayList<>(labels.size());
    labels.forEach(
        (key, value) ->
            result.add(StringKeyValue.newBuilder().setKey(key).setValue(value).build()));
    return result;
  }

  private MetricAdapter() {}
}
