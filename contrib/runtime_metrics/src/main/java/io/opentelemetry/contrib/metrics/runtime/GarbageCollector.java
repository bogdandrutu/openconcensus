/*
 * Copyright 2019, OpenTelemetry Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.opentelemetry.contrib.metrics.runtime;

import io.opentelemetry.OpenTelemetry;
import io.opentelemetry.metrics.LabelSet;
import io.opentelemetry.metrics.Meter;
import io.opentelemetry.metrics.Observer.Callback;
import io.opentelemetry.metrics.ObserverLong;
import io.opentelemetry.metrics.ObserverLong.Result;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
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
 *   jvm_gc_collection{gc="PS1"} 6.7
 * </pre>
 */
public final class GarbageCollector {
  private static final String GC_LABEL_KEY = "gc";

  private final List<GarbageCollectorMXBean> garbageCollectors;
  private final Meter meter;

  /** Constructs a new module that is capable to export metrics about "jvm_gc". */
  public GarbageCollector() {
    this.garbageCollectors = ManagementFactory.getGarbageCollectorMXBeans();
    this.meter = OpenTelemetry.getMeterFactory().get("jvm_gc");
  }

  /** Export all metrics generated by this module. */
  public void exportAll() {
    final ObserverLong gcMetric =
        meter
            .observerLongBuilder("collection")
            .setDescription("Time spent in a given JVM garbage collector in milliseconds.")
            .setUnit("ms")
            .setLabelKeys(Collections.singletonList(GC_LABEL_KEY))
            .setMonotonic(true)
            .build();
    final List<ObserverLong.Bound> handles = new ArrayList<>(garbageCollectors.size());
    for (final GarbageCollectorMXBean gc : garbageCollectors) {
      LabelSet labelSet = meter.createLabelSet(GC_LABEL_KEY, gc.getName());
      handles.add(gcMetric.getBound(labelSet));
    }

    gcMetric.setCallback(
        new Callback<Result>() {
          @Override
          public void update(Result result) {
            for (int i = 0; i < garbageCollectors.size(); i++) {
              result.put(handles.get(i), garbageCollectors.get(i).getCollectionTime());
            }
          }
        });
  }
}
