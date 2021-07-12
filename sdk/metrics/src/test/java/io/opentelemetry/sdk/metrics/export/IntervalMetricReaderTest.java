/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.metrics.export;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.InstrumentationLibraryInfo;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.LongSumData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.resources.Resource;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class IntervalMetricReaderTest {
  private static final List<LongPointData> LONG_POINT_LIST =
      Collections.singletonList(LongPointData.create(1000, 3000, Attributes.empty(), 1234567));

  private static final MetricData METRIC_DATA =
      MetricData.createLongSum(
          Resource.empty(),
          InstrumentationLibraryInfo.create("IntervalMetricReaderTest", null),
          "my metric",
          "my metric description",
          "us",
          LongSumData.create(
              /* isMonotonic= */ true, AggregationTemporality.CUMULATIVE, LONG_POINT_LIST));

  @Mock private MetricProducer metricProducer;

  @BeforeEach
  void setup() {
    when(metricProducer.collectAllMetrics()).thenReturn(Collections.singletonList(METRIC_DATA));
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  void startOnlyOnce() {
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);

    ScheduledFuture mock = mock(ScheduledFuture.class);
    when(scheduler.scheduleWithFixedDelay(any(), anyLong(), anyLong(), any())).thenReturn(mock);

    IntervalMetricReader intervalMetricReader =
        new IntervalMetricReader(
            IntervalMetricReader.InternalState.builder()
                .setMetricProducers(Collections.emptyList())
                .setMetricExporter(mock(MetricExporter.class))
                .build(),
            scheduler);

    intervalMetricReader.start();
    intervalMetricReader.start();

    verify(scheduler, times(1)).scheduleWithFixedDelay(any(), anyLong(), anyLong(), any());
  }

  @Test
  void intervalExport() throws Exception {
    WaitingMetricExporter waitingMetricExporter = new WaitingMetricExporter();
    IntervalMetricReader intervalMetricReader =
        IntervalMetricReader.builder()
            .setExportIntervalMillis(100)
            .setMetricExporter(waitingMetricExporter)
            .setMetricProducers(Collections.singletonList(metricProducer))
            .buildAndStart();

    try {
      assertThat(waitingMetricExporter.waitForNumberOfExports(1))
          .containsExactly(Collections.singletonList(METRIC_DATA));

      assertThat(waitingMetricExporter.waitForNumberOfExports(2))
          .containsExactly(
              Collections.singletonList(METRIC_DATA), Collections.singletonList(METRIC_DATA));
    } finally {
      intervalMetricReader.shutdown();
    }
  }

  @Test
  void forceFlush() throws Exception {
    WaitingMetricExporter waitingMetricExporter = new WaitingMetricExporter();
    IntervalMetricReader intervalMetricReader =
        IntervalMetricReader.builder()
            // Will force flush.
            .setExportIntervalMillis(Long.MAX_VALUE)
            .setMetricExporter(waitingMetricExporter)
            .setMetricProducers(Collections.singletonList(metricProducer))
            .buildAndStart();

    assertThat(intervalMetricReader.forceFlush().join(10, TimeUnit.SECONDS).isSuccess()).isTrue();

    try {
      assertThat(waitingMetricExporter.waitForNumberOfExports(1))
          .containsExactly(Collections.singletonList(METRIC_DATA));
    } finally {
      intervalMetricReader.shutdown();
    }
  }

  @Test
  void forceFlushGlobal() throws Exception {
    WaitingMetricExporter waitingMetricExporter = new WaitingMetricExporter();
    IntervalMetricReader intervalMetricReader =
        IntervalMetricReader.builder()
            // Will force flush.
            .setExportIntervalMillis(Long.MAX_VALUE)
            .setMetricExporter(waitingMetricExporter)
            .setMetricProducers(Collections.singletonList(metricProducer))
            .build()
            .startAndRegisterGlobal();

    assertThat(IntervalMetricReader.forceFlushGlobal().join(10, TimeUnit.SECONDS).isSuccess())
        .isTrue();

    try {
      assertThat(waitingMetricExporter.waitForNumberOfExports(1))
          .containsExactly(Collections.singletonList(METRIC_DATA));
    } finally {
      intervalMetricReader.shutdown();
      IntervalMetricReader.resetGlobalForTest();
    }
  }

  @Test
  @Timeout(2)
  public void intervalExport_exporterThrowsException() throws Exception {
    WaitingMetricExporter waitingMetricExporter = new WaitingMetricExporter(/* shouldThrow=*/ true);
    IntervalMetricReader intervalMetricReader =
        IntervalMetricReader.builder()
            .setExportIntervalMillis(100)
            .setMetricExporter(waitingMetricExporter)
            .setMetricProducers(Collections.singletonList(metricProducer))
            .buildAndStart();

    try {
      assertThat(waitingMetricExporter.waitForNumberOfExports(2))
          .containsExactly(
              Collections.singletonList(METRIC_DATA), Collections.singletonList(METRIC_DATA));
    } finally {
      intervalMetricReader.shutdown();
    }
  }

  @Test
  void oneLastExportAfterShutdown() throws Exception {
    WaitingMetricExporter waitingMetricExporter = new WaitingMetricExporter();
    IntervalMetricReader intervalMetricReader =
        IntervalMetricReader.builder()
            .setExportIntervalMillis(100_000)
            .setMetricExporter(waitingMetricExporter)
            .setMetricProducers(Collections.singletonList(metricProducer))
            .buildAndStart();

    // Assume that this will be called in less than 100 seconds.
    intervalMetricReader.shutdown();

    // This export was called during shutdown.
    assertThat(waitingMetricExporter.waitForNumberOfExports(1))
        .containsExactly(Collections.singletonList(METRIC_DATA));

    assertThat(waitingMetricExporter.hasShutdown.get()).isTrue();
  }

  private static class WaitingMetricExporter implements MetricExporter {

    private final AtomicBoolean hasShutdown = new AtomicBoolean(false);
    private final boolean shouldThrow;
    private final BlockingQueue<List<MetricData>> queue = new LinkedBlockingQueue<>();
    private final List<Long> exportTimes = Collections.synchronizedList(new ArrayList<>());

    private WaitingMetricExporter() {
      this(false);
    }

    private WaitingMetricExporter(boolean shouldThrow) {
      this.shouldThrow = shouldThrow;
    }

    @Override
    public CompletableResultCode export(Collection<MetricData> metricList) {
      exportTimes.add(System.currentTimeMillis());
      queue.offer(new ArrayList<>(metricList));

      if (shouldThrow) {
        throw new RuntimeException("Export Failed!");
      }
      return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode flush() {
      return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode shutdown() {
      hasShutdown.set(true);
      return CompletableResultCode.ofSuccess();
    }

    /**
     * Waits until export is called for numberOfExports times. Returns the list of exported lists of
     * metrics.
     */
    @Nullable
    List<List<MetricData>> waitForNumberOfExports(int numberOfExports) throws Exception {
      List<List<MetricData>> result = new ArrayList<>();
      while (result.size() < numberOfExports) {
        List<MetricData> export = queue.poll(5, TimeUnit.SECONDS);
        assertThat(export).isNotNull();
        result.add(export);
      }
      return result;
    }
  }
}
