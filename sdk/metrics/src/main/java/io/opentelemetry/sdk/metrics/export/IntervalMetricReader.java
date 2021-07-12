/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.metrics.export;

import com.google.auto.value.AutoValue;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.internal.DaemonThreadFactory;
import io.opentelemetry.sdk.metrics.data.MetricData;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.concurrent.Immutable;

/**
 * Wraps a list of {@link MetricProducer}s and automatically reads and exports the metrics every
 * export interval. Metrics may also be dropped when it becomes time to export again, and there is
 * an export in progress.
 */
public final class IntervalMetricReader {
  private static final Logger logger = Logger.getLogger(IntervalMetricReader.class.getName());

  private static final AtomicReference<IntervalMetricReader> globalIntervalMetricReader =
      new AtomicReference<>();

  private final Exporter exporter;
  private final ScheduledExecutorService scheduler;

  private volatile ScheduledFuture<?> scheduledFuture;
  private final Object lock = new Object();

  /**
   * {@linkplain IntervalMetricReader#forceFlush() Force flushes} the globally registered {@link
   * IntervalMetricReader} if available, or does nothing otherwise.
   */
  public static CompletableResultCode forceFlushGlobal() {
    IntervalMetricReader intervalMetricReader = globalIntervalMetricReader.get();
    if (intervalMetricReader != null) {
      return intervalMetricReader.forceFlush();
    }
    return CompletableResultCode.ofSuccess();
  }

  static void resetGlobalForTest() {
    globalIntervalMetricReader.set(null);
  }

  /** Stops the scheduled task and calls export one more time. */
  public CompletableResultCode shutdown() {
    final CompletableResultCode result = new CompletableResultCode();
    if (scheduledFuture != null) {
      scheduledFuture.cancel(false);
    }
    scheduler.shutdown();
    try {
      scheduler.awaitTermination(5, TimeUnit.SECONDS);
      final CompletableResultCode flushResult = exporter.doRun();
      flushResult.join(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      // force a shutdown if the export hasn't finished.
      scheduler.shutdownNow();
      // reset the interrupted status
      Thread.currentThread().interrupt();
    } finally {
      final CompletableResultCode shutdownResult = exporter.shutdown();
      shutdownResult.whenComplete(
          () -> {
            if (!shutdownResult.isSuccess()) {
              result.fail();
            } else {
              result.succeed();
            }
          });
    }
    return result;
  }

  /**
   * Returns a new {@link IntervalMetricReaderBuilder} for {@link IntervalMetricReader}.
   *
   * @return a new {@link IntervalMetricReaderBuilder} for {@link IntervalMetricReader}.
   */
  public static IntervalMetricReaderBuilder builder() {
    return new IntervalMetricReaderBuilder(InternalState.builder());
  }

  /**
   * Requests the {@link IntervalMetricReader} to export current metrics and returns a {@link
   * CompletableResultCode} which is completed when the flush is finished.
   */
  public CompletableResultCode forceFlush() {
    return exporter.doRun();
  }

  IntervalMetricReader(InternalState internalState) {
    this(
        internalState,
        Executors.newScheduledThreadPool(1, new DaemonThreadFactory("IntervalMetricReader")));
  }

  // visible for testing
  IntervalMetricReader(InternalState internalState, ScheduledExecutorService intervalMetricReader) {
    this.exporter = new Exporter(internalState);
    this.scheduler = intervalMetricReader;
  }

  /** Starts this {@link IntervalMetricReader} to report to the configured exporter. */
  public IntervalMetricReader start() {
    synchronized (lock) {
      if (scheduledFuture != null) {
        return this;
      }
      scheduledFuture =
          scheduler.scheduleWithFixedDelay(
              exporter,
              exporter.internalState.getExportIntervalMillis(),
              exporter.internalState.getExportIntervalMillis(),
              TimeUnit.MILLISECONDS);
      return this;
    }
  }

  /**
   * Starts this {@link IntervalMetricReader} and registers it as the global {@link
   * IntervalMetricReader}.
   */
  public IntervalMetricReader startAndRegisterGlobal() {
    start();
    if (!globalIntervalMetricReader.compareAndSet(null, this)) {
      logger.log(Level.WARNING, "Global IntervalMetricReader already registered, ignoring.");
    }
    return this;
  }

  private static final class Exporter implements Runnable {

    private final InternalState internalState;
    private final AtomicBoolean exportAvailable = new AtomicBoolean(true);

    private Exporter(InternalState internalState) {
      this.internalState = internalState;
    }

    @Override
    public void run() {
      doRun().join(internalState.getExportIntervalMillis(), TimeUnit.MILLISECONDS);
    }

    // Make sure not to throw any exceptions.
    CompletableResultCode doRun() {
      final CompletableResultCode flushResult = new CompletableResultCode();
      if (exportAvailable.compareAndSet(true, false)) {
        try {
          List<MetricData> metricsList = new ArrayList<>();
          for (MetricProducer metricProducer : internalState.getMetricProducers()) {
            metricsList.addAll(metricProducer.collectAllMetrics());
          }
          final CompletableResultCode result =
              internalState.getMetricExporter().export(Collections.unmodifiableList(metricsList));
          result.whenComplete(
              () -> {
                if (!result.isSuccess()) {
                  logger.log(Level.FINE, "Exporter failed");
                }
                flushResult.succeed();
                exportAvailable.set(true);
              });
        } catch (Throwable t) {
          exportAvailable.set(true);
          logger.log(Level.WARNING, "Exporter threw an Exception", t);
          flushResult.fail();
        }
      } else {
        logger.log(Level.FINE, "Exporter busy. Dropping metrics.");
        flushResult.fail();
      }
      return flushResult;
    }

    CompletableResultCode shutdown() {
      return internalState.getMetricExporter().shutdown();
    }
  }

  @AutoValue
  @Immutable
  abstract static class InternalState {
    static final long DEFAULT_INTERVAL_MILLIS = 60_000;

    abstract MetricExporter getMetricExporter();

    abstract long getExportIntervalMillis();

    abstract Collection<MetricProducer> getMetricProducers();

    static Builder builder() {
      return new AutoValue_IntervalMetricReader_InternalState.Builder()
          .setExportIntervalMillis(DEFAULT_INTERVAL_MILLIS);
    }

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setExportIntervalMillis(long exportIntervalMillis);

      abstract Builder setMetricExporter(MetricExporter metricExporter);

      abstract Builder setMetricProducers(Collection<MetricProducer> metricProducers);

      abstract InternalState build();
    }
  }
}
