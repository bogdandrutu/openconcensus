/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.metrics;

import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static io.opentelemetry.sdk.testing.assertj.metrics.MetricAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.common.Labels;
import io.opentelemetry.sdk.common.InstrumentationLibraryInfo;
import io.opentelemetry.sdk.internal.TestClock;
import io.opentelemetry.sdk.resources.Resource;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link DoubleValueObserverSdk}. */
class DoubleValueObserverSdkTest {
  private static final long SECOND_NANOS = 1_000_000_000;
  private static final Resource RESOURCE =
      Resource.create(Attributes.of(stringKey("resource_key"), "resource_value"));
  private static final InstrumentationLibraryInfo INSTRUMENTATION_LIBRARY_INFO =
      InstrumentationLibraryInfo.create(DoubleValueObserverSdkTest.class.getName(), null);
  private final TestClock testClock = TestClock.create();
  private final SdkMeterProvider sdkMeterProvider =
      SdkMeterProvider.builder().setClock(testClock).setResource(RESOURCE).build();
  private final Meter sdkMeter = sdkMeterProvider.get(getClass().getName());

  @Test
  void collectMetrics_NoCallback() {
    sdkMeter
        .doubleValueObserverBuilder("testObserver")
        .setDescription("My own DoubleValueObserver")
        .setUnit("ms")
        .build();
    assertThat(sdkMeterProvider.collectAllMetrics()).isEmpty();
  }

  @Test
  void collectMetrics_NoRecords() {
    sdkMeter
        .doubleValueObserverBuilder("testObserver")
        .setDescription("My own DoubleValueObserver")
        .setUnit("ms")
        .setUpdater(result -> {})
        .build();
    assertThat(sdkMeterProvider.collectAllMetrics()).isEmpty();
  }

  @Test
  @SuppressWarnings("unchecked")
  void collectMetrics_WithOneRecord() {
    sdkMeter
        .doubleValueObserverBuilder("testObserver")
        .setDescription("My own DoubleValueObserver")
        .setUnit("ms")
        .setUpdater(result -> result.observe(12.1d, Labels.of("k", "v")))
        .build();
    testClock.advanceNanos(SECOND_NANOS);
    assertThat(sdkMeterProvider.collectAllMetrics())
        .satisfiesExactly(
            metric ->
                assertThat(metric)
                    .hasResource(RESOURCE)
                    .hasInstrumentationLibrary(INSTRUMENTATION_LIBRARY_INFO)
                    .hasName("testObserver")
                    .hasDescription("My own DoubleValueObserver")
                    .hasUnit("ms")
                    .hasDoubleGauge()
                    .points()
                    .satisfiesExactlyInAnyOrder(
                        point ->
                            assertThat(point)
                                .hasStartEpochNanos(0)
                                .hasEpochNanos(testClock.now())
                                .hasAttributes(Attributes.builder().put("k", "v").build())
                                .hasValue(12.1d)));
    testClock.advanceNanos(SECOND_NANOS);
    assertThat(sdkMeterProvider.collectAllMetrics())
        .satisfiesExactly(
            metric ->
                assertThat(metric)
                    .hasResource(RESOURCE)
                    .hasInstrumentationLibrary(INSTRUMENTATION_LIBRARY_INFO)
                    .hasName("testObserver")
                    .hasDescription("My own DoubleValueObserver")
                    .hasUnit("ms")
                    .hasDoubleGauge()
                    .points()
                    .satisfiesExactlyInAnyOrder(
                        point ->
                            assertThat(point)
                                .hasStartEpochNanos(0)
                                .hasEpochNanos(testClock.now())
                                .hasAttributes(Attributes.builder().put("k", "v").build())
                                .hasValue(12.1d)));
  }
}
