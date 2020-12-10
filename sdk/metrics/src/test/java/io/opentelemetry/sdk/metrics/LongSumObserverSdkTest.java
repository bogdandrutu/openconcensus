/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.metrics;

import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.Labels;
import io.opentelemetry.sdk.common.InstrumentationLibraryInfo;
import io.opentelemetry.sdk.internal.TestClock;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.MetricData.LongPoint;
import io.opentelemetry.sdk.resources.Resource;
import java.util.Collections;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link LongSumObserverSdk}. */
class LongSumObserverSdkTest {
  private static final long SECOND_NANOS = 1_000_000_000;
  private static final Resource RESOURCE =
      Resource.create(Attributes.of(stringKey("resource_key"), "resource_value"));
  private static final InstrumentationLibraryInfo INSTRUMENTATION_LIBRARY_INFO =
      InstrumentationLibraryInfo.create(
          "io.opentelemetry.sdk.metrics.LongSumObserverSdkTest", null);
  private final TestClock testClock = TestClock.create();
  private final MeterProviderSharedState meterProviderSharedState =
      MeterProviderSharedState.create(testClock, RESOURCE);
  private final MeterSdk testSdk =
      new MeterSdk(meterProviderSharedState, INSTRUMENTATION_LIBRARY_INFO);

  @Test
  void collectMetrics_NoCallback() {
    LongSumObserverSdk longSumObserver =
        testSdk
            .longSumObserverBuilder("testObserver")
            .setDescription("My own LongSumObserver")
            .setUnit("ms")
            .build();
    assertThat(longSumObserver.collectAll()).isEmpty();
  }

  @Test
  void collectMetrics_NoRecords() {
    LongSumObserverSdk longSumObserver =
        testSdk
            .longSumObserverBuilder("testObserver")
            .setDescription("My own LongSumObserver")
            .setUnit("ms")
            .setCallback(result -> {})
            .build();
    assertThat(longSumObserver.collectAll())
        .containsExactly(
            MetricData.create(
                RESOURCE,
                INSTRUMENTATION_LIBRARY_INFO,
                "testObserver",
                "My own LongSumObserver",
                "ms",
                MetricData.Type.LONG_SUM,
                Collections.emptyList()));
  }

  @Test
  void collectMetrics_WithOneRecord() {
    LongSumObserverSdk longSumObserver =
        testSdk
            .longSumObserverBuilder("testObserver")
            .setCallback(result -> result.observe(12, Labels.of("k", "v")))
            .build();
    testClock.advanceNanos(SECOND_NANOS);
    assertThat(longSumObserver.collectAll())
        .containsExactly(
            MetricData.create(
                RESOURCE,
                INSTRUMENTATION_LIBRARY_INFO,
                "testObserver",
                "",
                "1",
                MetricData.Type.LONG_SUM,
                Collections.singletonList(
                    LongPoint.create(
                        testClock.now() - SECOND_NANOS,
                        testClock.now(),
                        Labels.of("k", "v"),
                        12))));
    testClock.advanceNanos(SECOND_NANOS);
    assertThat(longSumObserver.collectAll())
        .containsExactly(
            MetricData.create(
                RESOURCE,
                INSTRUMENTATION_LIBRARY_INFO,
                "testObserver",
                "",
                "1",
                MetricData.Type.LONG_SUM,
                Collections.singletonList(
                    LongPoint.create(
                        testClock.now() - 2 * SECOND_NANOS,
                        testClock.now(),
                        Labels.of("k", "v"),
                        12))));
  }
}
