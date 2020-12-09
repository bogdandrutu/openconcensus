/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.metrics;

import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.Labels;
import io.opentelemetry.api.metrics.BatchRecorder;
import io.opentelemetry.sdk.common.InstrumentationLibraryInfo;
import io.opentelemetry.sdk.internal.TestClock;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.MetricData.DoublePoint;
import io.opentelemetry.sdk.metrics.data.MetricData.LongPoint;
import io.opentelemetry.sdk.resources.Resource;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link BatchRecorderSdk}. */
class BatchRecorderSdkTest {
  private static final Resource RESOURCE =
      Resource.create(Attributes.of(stringKey("resource_key"), "resource_value"));
  private static final InstrumentationLibraryInfo INSTRUMENTATION_LIBRARY_INFO =
      InstrumentationLibraryInfo.create("io.opentelemetry.sdk.metrics.BatchRecorderSdkTest", null);
  private final TestClock testClock = TestClock.create();
  private final MeterProviderSharedState meterProviderSharedState =
      MeterProviderSharedState.create(testClock, RESOURCE);
  private final MeterSdk testSdk =
      new MeterSdk(meterProviderSharedState, INSTRUMENTATION_LIBRARY_INFO, new ViewRegistry());

  @Test
  void batchRecorder_badLabelSet() {
    assertThrows(
        IllegalArgumentException.class,
        () -> testSdk.newBatchRecorder("key").record(),
        "key/value");
  }

  @Test
  void batchRecorder() {
    DoubleCounterSdk doubleCounter = testSdk.doubleCounterBuilder("testDoubleCounter").build();
    LongCounterSdk longCounter = testSdk.longCounterBuilder("testLongCounter").build();
    DoubleUpDownCounterSdk doubleUpDownCounter =
        testSdk.doubleUpDownCounterBuilder("testDoubleUpDownCounter").build();
    LongUpDownCounterSdk longUpDownCounter =
        testSdk.longUpDownCounterBuilder("testLongUpDownCounter").build();
    DoubleValueRecorderSdk doubleValueRecorder =
        testSdk.doubleValueRecorderBuilder("testDoubleValueRecorder").build();
    LongValueRecorderSdk longValueRecorder =
        testSdk.longValueRecorderBuilder("testLongValueRecorder").build();
    Labels labelSet = Labels.of("key", "value");

    BatchRecorder batchRecorder = testSdk.newBatchRecorder("key", "value");

    batchRecorder
        .put(longCounter, 12)
        .put(doubleUpDownCounter, -12.1d)
        .put(longUpDownCounter, -12)
        .put(doubleCounter, 12.1d)
        .put(doubleCounter, 12.1d)
        .put(longValueRecorder, 13)
        .put(doubleValueRecorder, 13.1d);

    // until record() is called, nothing should be recorded.
    Collection<MetricData> preRecord = testSdk.collectAll();
    preRecord.forEach(metricData -> assertThat(metricData.getPoints()).isEmpty());

    batchRecorder.record();

    assertBatchRecordings(
        doubleCounter,
        longCounter,
        doubleUpDownCounter,
        longUpDownCounter,
        doubleValueRecorder,
        longValueRecorder,
        labelSet,
        /* shouldHaveDeltas=*/ true);

    // a second record, with no recordings added should not change any of the values.
    batchRecorder.record();
    assertBatchRecordings(
        doubleCounter,
        longCounter,
        doubleUpDownCounter,
        longUpDownCounter,
        doubleValueRecorder,
        longValueRecorder,
        labelSet,
        /* shouldHaveDeltas=*/ false);
  }

  private void assertBatchRecordings(
      DoubleCounterSdk doubleCounter,
      LongCounterSdk longCounter,
      DoubleUpDownCounterSdk doubleUpDownCounter,
      LongUpDownCounterSdk longUpDownCounter,
      DoubleValueRecorderSdk doubleValueRecorder,
      LongValueRecorderSdk longValueRecorder,
      Labels labelSet,
      boolean shouldHaveDeltas) {
    assertThat(doubleCounter.collectAll())
        .containsExactly(
            MetricData.create(
                RESOURCE,
                INSTRUMENTATION_LIBRARY_INFO,
                "testDoubleCounter",
                "",
                "1",
                MetricData.Type.DOUBLE_SUM,
                Collections.singletonList(
                    DoublePoint.create(testClock.now(), testClock.now(), labelSet, 24.2d))));
    assertThat(longCounter.collectAll())
        .containsExactly(
            MetricData.create(
                RESOURCE,
                INSTRUMENTATION_LIBRARY_INFO,
                "testLongCounter",
                "",
                "1",
                MetricData.Type.LONG_SUM,
                Collections.singletonList(
                    LongPoint.create(testClock.now(), testClock.now(), labelSet, 12))));
    assertThat(doubleUpDownCounter.collectAll())
        .containsExactly(
            MetricData.create(
                RESOURCE,
                INSTRUMENTATION_LIBRARY_INFO,
                "testDoubleUpDownCounter",
                "",
                "1",
                MetricData.Type.NON_MONOTONIC_DOUBLE_SUM,
                Collections.singletonList(
                    DoublePoint.create(testClock.now(), testClock.now(), labelSet, -12.1d))));
    assertThat(longUpDownCounter.collectAll())
        .containsExactly(
            MetricData.create(
                RESOURCE,
                INSTRUMENTATION_LIBRARY_INFO,
                "testLongUpDownCounter",
                "",
                "1",
                MetricData.Type.NON_MONOTONIC_LONG_SUM,
                Collections.singletonList(
                    LongPoint.create(testClock.now(), testClock.now(), labelSet, -12))));

    assertThat(doubleValueRecorder.collectAll())
        .containsExactly(
            MetricData.create(
                RESOURCE,
                INSTRUMENTATION_LIBRARY_INFO,
                "testDoubleValueRecorder",
                "",
                "1",
                MetricData.Type.SUMMARY,
                shouldHaveDeltas
                    ? Collections.singletonList(
                        MetricData.SummaryPoint.create(
                            testClock.now(),
                            testClock.now(),
                            labelSet,
                            1,
                            13.1d,
                            Arrays.asList(
                                MetricData.ValueAtPercentile.create(0.0, 13.1),
                                MetricData.ValueAtPercentile.create(100.0, 13.1))))
                    : emptyList()));
    assertThat(longValueRecorder.collectAll())
        .containsExactly(
            MetricData.create(
                RESOURCE,
                INSTRUMENTATION_LIBRARY_INFO,
                "testLongValueRecorder",
                "",
                "1",
                MetricData.Type.SUMMARY,
                shouldHaveDeltas
                    ? Collections.singletonList(
                        MetricData.SummaryPoint.create(
                            testClock.now(),
                            testClock.now(),
                            labelSet,
                            1,
                            13,
                            Arrays.asList(
                                MetricData.ValueAtPercentile.create(0.0, 13),
                                MetricData.ValueAtPercentile.create(100.0, 13))))
                    : emptyList()));
  }
}
