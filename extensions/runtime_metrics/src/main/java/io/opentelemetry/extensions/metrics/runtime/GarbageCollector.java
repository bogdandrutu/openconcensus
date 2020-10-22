/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.extensions.metrics.runtime;

import io.opentelemetry.OpenTelemetry;
import io.opentelemetry.common.Labels;
import io.opentelemetry.metrics.AsynchronousInstrument;
import io.opentelemetry.metrics.AsynchronousInstrument.LongResult;
import io.opentelemetry.metrics.LongSumObserver;
import io.opentelemetry.metrics.Meter;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;

/**
 * Exports metrics about JVM garbage collectors.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * new GarbageCollector().exportAll();
 * }</pre>
 *
 * <p>Example metrics being exported:
 *
 * <pre>
 *   runtime.jvm.gc.collection{gc="PS1"} 6.7
 * </pre>
 */
public final class GarbageCollector {
  private static final String GC_LABEL_KEY = "gc";

  private final List<GarbageCollectorMXBean> garbageCollectors;
  private final Meter meter;

  /** Constructs a new module that will export metrics in the "runtime.jvm.gc" namespace. */
  public GarbageCollector() {
    this.garbageCollectors = ManagementFactory.getGarbageCollectorMXBeans();
    this.meter = OpenTelemetry.getGlobalMeter("io.opentelemetry.extensions.metrics.runtime.gc");
  }

  /** Export all metrics generated by this module. */
  public void exportAll() {
    final LongSumObserver gcMetric =
        meter
            .longSumObserverBuilder("runtime.jvm.gc.collection")
            .setDescription("Time spent in a given JVM garbage collector in milliseconds.")
            .setUnit("ms")
            .build();
    final List<Labels> labelSets = new ArrayList<>(garbageCollectors.size());
    for (final GarbageCollectorMXBean gc : garbageCollectors) {
      labelSets.add(Labels.of(GC_LABEL_KEY, gc.getName()));
    }

    gcMetric.setCallback(
        new AsynchronousInstrument.Callback<LongResult>() {
          @Override
          public void update(LongResult resultLongObserver) {
            for (int i = 0; i < garbageCollectors.size(); i++) {
              resultLongObserver.observe(
                  garbageCollectors.get(i).getCollectionTime(), labelSets.get(i));
            }
          }
        });
  }
}
