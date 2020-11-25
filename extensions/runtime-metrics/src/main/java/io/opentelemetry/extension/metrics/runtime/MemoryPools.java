/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.extension.metrics.runtime;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Labels;
import io.opentelemetry.api.metrics.AsynchronousInstrument;
import io.opentelemetry.api.metrics.AsynchronousInstrument.LongResult;
import io.opentelemetry.api.metrics.LongUpDownSumObserver;
import io.opentelemetry.api.metrics.Meter;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.List;

/**
 * Exports metrics about JVM memory areas.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * new MemoryPools().exportAll();
 * }</pre>
 *
 * <p>Example metrics being exported: Component
 *
 * <pre>
 *   runtime.jvm.memory.area{type="used",area="heap"} 2000000
 *   runtime.jvm.memory.area{type="committed",area="nonheap"} 200000
 *   runtime.jvm.memory.area{type="used",pool="PS Eden Space"} 2000
 * </pre>
 *
 * @deprecated This module and classes have been moved to the OpenTelemetry Java Instrumentation
 *     project. It will be removed in release 0.13.0.
 */
@Deprecated
public final class MemoryPools {
  private static final String TYPE_LABEL_KEY = "type";
  private static final String AREA_LABEL_KEY = "area";
  private static final String POOL_LABEL_KEY = "pool";
  private static final String USED = "used";
  private static final String COMMITTED = "committed";
  private static final String MAX = "max";
  private static final String HEAP = "heap";
  private static final String NON_HEAP = "non_heap";

  private static final Labels COMMITTED_HEAP =
      Labels.of(TYPE_LABEL_KEY, COMMITTED, AREA_LABEL_KEY, HEAP);
  private static final Labels USED_HEAP = Labels.of(TYPE_LABEL_KEY, USED, AREA_LABEL_KEY, HEAP);
  private static final Labels MAX_HEAP = Labels.of(TYPE_LABEL_KEY, MAX, AREA_LABEL_KEY, HEAP);

  private static final Labels COMMITTED_NON_HEAP =
      Labels.of(TYPE_LABEL_KEY, COMMITTED, AREA_LABEL_KEY, NON_HEAP);
  private static final Labels USED_NON_HEAP =
      Labels.of(TYPE_LABEL_KEY, USED, AREA_LABEL_KEY, NON_HEAP);
  private static final Labels MAX_NON_HEAP =
      Labels.of(TYPE_LABEL_KEY, MAX, AREA_LABEL_KEY, NON_HEAP);

  private final MemoryMXBean memoryBean;
  private final List<MemoryPoolMXBean> poolBeans;
  private final Meter meter;

  /** Constructs a new module that will export metrics in the "runtime.jvm.memory" namespace. */
  public MemoryPools() {
    this.memoryBean = ManagementFactory.getMemoryMXBean();
    this.poolBeans = ManagementFactory.getMemoryPoolMXBeans();
    this.meter = OpenTelemetry.getGlobalMeter(MemoryPools.class.getName());
  }

  /** Export only the "area" metric. */
  public void exportMemoryAreaMetric() {
    final LongUpDownSumObserver areaMetric =
        this.meter
            .longUpDownSumObserverBuilder("runtime.jvm.memory.area")
            .setDescription("Bytes of a given JVM memory area.")
            .setUnit("By")
            .build();
    areaMetric.setCallback(
        new AsynchronousInstrument.Callback<LongResult>() {
          @Override
          public void update(LongResult resultLongObserver) {
            observeHeap(resultLongObserver, memoryBean.getHeapMemoryUsage());
            observeNonHeap(resultLongObserver, memoryBean.getNonHeapMemoryUsage());
          }
        });
  }

  /** Export only the "pool" metric. */
  public void exportMemoryPoolMetric() {
    final LongUpDownSumObserver poolMetric =
        this.meter
            .longUpDownSumObserverBuilder("runtime.jvm.memory.pool")
            .setDescription("Bytes of a given JVM memory pool.")
            .setUnit("By")
            .build();
    final List<Labels> usedLabelSets = new ArrayList<>(poolBeans.size());
    final List<Labels> committedLabelSets = new ArrayList<>(poolBeans.size());
    final List<Labels> maxLabelSets = new ArrayList<>(poolBeans.size());
    for (final MemoryPoolMXBean pool : poolBeans) {
      usedLabelSets.add(Labels.of(TYPE_LABEL_KEY, USED, POOL_LABEL_KEY, pool.getName()));
      committedLabelSets.add(Labels.of(TYPE_LABEL_KEY, COMMITTED, POOL_LABEL_KEY, pool.getName()));
      maxLabelSets.add(Labels.of(TYPE_LABEL_KEY, MAX, POOL_LABEL_KEY, pool.getName()));
    }
    poolMetric.setCallback(
        new AsynchronousInstrument.Callback<LongResult>() {
          @Override
          public void update(LongResult resultLongObserver) {
            for (int i = 0; i < poolBeans.size(); i++) {
              MemoryUsage poolUsage = poolBeans.get(i).getUsage();
              if (poolUsage != null) {
                observe(
                    resultLongObserver,
                    poolUsage,
                    usedLabelSets.get(i),
                    committedLabelSets.get(i),
                    maxLabelSets.get(i));
              }
            }
          }
        });
  }

  /** Export all metrics generated by this module. */
  public void exportAll() {
    exportMemoryAreaMetric();
    exportMemoryPoolMetric();
  }

  static void observeHeap(LongResult observer, MemoryUsage usage) {
    observe(observer, usage, USED_HEAP, COMMITTED_HEAP, MAX_HEAP);
  }

  static void observeNonHeap(LongResult observer, MemoryUsage usage) {
    observe(observer, usage, USED_NON_HEAP, COMMITTED_NON_HEAP, MAX_NON_HEAP);
  }

  private static void observe(
      LongResult observer,
      MemoryUsage usage,
      Labels usedLabels,
      Labels committedLabels,
      Labels maxLabels) {
    // TODO: Decide if init is needed or not. It is a constant that can be queried once on startup.
    // if (usage.getInit() != -1) {
    //  observer.observe(usage.getInit(), ...);
    // }
    observer.observe(usage.getUsed(), usedLabels);
    observer.observe(usage.getCommitted(), committedLabels);
    // TODO: Decide if max is needed or not. It is a constant that can be queried once on startup.
    if (usage.getMax() != -1) {
      observer.observe(usage.getMax(), maxLabels);
    }
  }
}
