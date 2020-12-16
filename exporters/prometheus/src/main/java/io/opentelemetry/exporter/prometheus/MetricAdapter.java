/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.exporter.prometheus;

import static io.prometheus.client.Collector.doubleToGoString;

import io.opentelemetry.api.common.Labels;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.MetricData.DoublePoint;
import io.opentelemetry.sdk.metrics.data.MetricData.DoubleSummaryPoint;
import io.opentelemetry.sdk.metrics.data.MetricData.LongPoint;
import io.opentelemetry.sdk.metrics.data.MetricData.Point;
import io.opentelemetry.sdk.metrics.data.MetricData.ValueAtPercentile;
import io.prometheus.client.Collector;
import io.prometheus.client.Collector.MetricFamilySamples;
import io.prometheus.client.Collector.MetricFamilySamples.Sample;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Util methods to convert OpenTelemetry Metrics data models to Prometheus data models.
 *
 * <p>Each OpenTelemetry {@link MetricData} will be converted to a Prometheus {@link
 * MetricFamilySamples}, and each {@code Point} of the {@link MetricData} will be converted to
 * Prometheus {@link Sample}s.
 *
 * <p>{@code DoublePoint}, {@code LongPoint} will be converted to a single {@link Sample}. {@code
 * Summary} will be converted to two {@link Sample}s (sum and count) plus the number of Percentile
 * values {@code Sample}s
 *
 * <p>Please note that Prometheus Metric and Label name can only have alphanumeric characters and
 * underscore. All other characters will be sanitized by underscores.
 */
final class MetricAdapter {

  static final String SAMPLE_SUFFIX_COUNT = "_count";
  static final String SAMPLE_SUFFIX_SUM = "_sum";
  static final String LABEL_NAME_QUANTILE = "quantile";

  // Converts a MetricData to a Prometheus MetricFamilySamples.
  static MetricFamilySamples toMetricFamilySamples(MetricData metricData) {
    String cleanMetricName = cleanMetricName(metricData.getName());
    Collector.Type type = toMetricFamilyType(metricData);

    return new MetricFamilySamples(
        cleanMetricName,
        type,
        metricData.getDescription(),
        toSamples(cleanMetricName, metricData.getType(), getPoints(metricData)));
  }

  private static String cleanMetricName(String descriptorMetricName) {
    return Collector.sanitizeMetricName(descriptorMetricName);
  }

  static Collector.Type toMetricFamilyType(MetricData metricData) {
    switch (metricData.getType()) {
      case LONG_GAUGE:
      case DOUBLE_GAUGE:
        return Collector.Type.GAUGE;
      case LONG_SUM:
        MetricData.LongSumData longSumData = metricData.getLongSumData();
        if (longSumData.isMonotonic()
            && longSumData.getAggregationTemporality()
                == MetricData.AggregationTemporality.CUMULATIVE) {
          return Collector.Type.COUNTER;
        }
        return Collector.Type.GAUGE;
      case DOUBLE_SUM:
        MetricData.DoubleSumData doubleSumData = metricData.getDoubleSumData();
        if (doubleSumData.isMonotonic()
            && doubleSumData.getAggregationTemporality()
                == MetricData.AggregationTemporality.CUMULATIVE) {
          return Collector.Type.COUNTER;
        }
        return Collector.Type.GAUGE;
      case SUMMARY:
        return Collector.Type.SUMMARY;
    }
    return Collector.Type.UNTYPED;
  }

  private static final Function<String, String> sanitizer = new LabelNameSanitizer();

  // Converts a list of points from MetricData to a list of Prometheus Samples.
  static List<Sample> toSamples(String name, MetricData.Type type, Collection<Point> points) {
    final List<Sample> samples = new ArrayList<>(estimateNumSamples(points.size(), type));

    for (Point point : points) {
      List<String> labelNames = Collections.emptyList();
      List<String> labelValues = Collections.emptyList();
      Labels labels = point.getLabels();
      if (labels.size() != 0) {
        labelNames = new ArrayList<>(labels.size());
        labelValues = new ArrayList<>(labels.size());

        labels.forEach(new Consumer(labelNames, labelValues));
      }

      switch (type) {
        case DOUBLE_SUM:
        case DOUBLE_GAUGE:
          DoublePoint doublePoint = (DoublePoint) point;
          samples.add(new Sample(name, labelNames, labelValues, doublePoint.getValue()));
          break;
        case LONG_SUM:
        case LONG_GAUGE:
          LongPoint longPoint = (LongPoint) point;
          samples.add(new Sample(name, labelNames, labelValues, longPoint.getValue()));
          break;
        case SUMMARY:
          addSummarySamples((DoubleSummaryPoint) point, name, labelNames, labelValues, samples);
          break;
      }
    }
    return samples;
  }

  private static final class Consumer implements BiConsumer<String, String> {
    final List<String> labelNames;
    final List<String> labelValues;

    private Consumer(List<String> labelNames, List<String> labelValues) {
      this.labelNames = labelNames;
      this.labelValues = labelValues;
    }

    @Override
    public void accept(String labelName, String value) {
      String sanitizedLabelName = sanitizer.apply(labelName);
      labelNames.add(sanitizedLabelName);
      labelValues.add(value == null ? "" : value);
    }
  }

  private static void addSummarySamples(
      DoubleSummaryPoint doubleSummaryPoint,
      String name,
      List<String> labelNames,
      List<String> labelValues,
      List<Sample> samples) {
    samples.add(
        new Sample(
            name + SAMPLE_SUFFIX_COUNT, labelNames, labelValues, doubleSummaryPoint.getCount()));
    samples.add(
        new Sample(name + SAMPLE_SUFFIX_SUM, labelNames, labelValues, doubleSummaryPoint.getSum()));
    List<ValueAtPercentile> valueAtPercentiles = doubleSummaryPoint.getPercentileValues();
    List<String> labelNamesWithQuantile = new ArrayList<>(labelNames.size());
    labelNamesWithQuantile.addAll(labelNames);
    labelNamesWithQuantile.add(LABEL_NAME_QUANTILE);
    for (ValueAtPercentile valueAtPercentile : valueAtPercentiles) {
      List<String> labelValuesWithQuantile = new ArrayList<>(labelValues.size());
      labelValuesWithQuantile.addAll(labelValues);
      labelValuesWithQuantile.add(doubleToGoString(valueAtPercentile.getPercentile()));
      samples.add(
          new Sample(
              name, labelNamesWithQuantile, labelValuesWithQuantile, valueAtPercentile.getValue()));
    }
  }

  private static int estimateNumSamples(int numPoints, MetricData.Type type) {
    if (type == MetricData.Type.SUMMARY) {
      // count + sum + estimated 2 percentiles (default MinMaxSumCount aggregator).
      return numPoints * 4;
    }
    return numPoints;
  }

  private static Collection<MetricData.Point> getPoints(MetricData metricData) {
    switch (metricData.getType()) {
      case DOUBLE_GAUGE:
        return metricData.getDoubleGaugeData().getPoints();
      case DOUBLE_SUM:
        return metricData.getDoubleSumData().getPoints();
      case LONG_GAUGE:
        return metricData.getLongGaugeData().getPoints();
      case LONG_SUM:
        return metricData.getLongSumData().getPoints();
      case SUMMARY:
        return metricData.getDoubleSummaryData().getPoints();
    }
    return Collections.emptyList();
  }

  private MetricAdapter() {}
}
