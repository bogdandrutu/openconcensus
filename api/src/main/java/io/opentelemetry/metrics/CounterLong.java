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

package io.opentelemetry.metrics;

import io.opentelemetry.metrics.CounterLong.TimeSeries;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Counter metric, to report instantaneous measurement of a long value. Cumulative values can go up
 * or stay the same, but can never go down. Cumulative values cannot be negative.
 *
 * <p>Example:
 *
 * <pre>{@code
 * class YourClass {
 *
 *   private static final Meter meter = Metrics.getMeter();
 *   private static final CounterLong counter =
 *       meter.
 *           .counterLongBuilder("processed_jobs")
 *           .setDescription("Processed jobs")
 *           .setUnit("1")
 *           .setLabelKeys(Collections.singletonList(LabelKey.create("Name", "desc")))
 *           .build();
 *   // It is recommended to keep a reference of a TimeSeries.
 *   private static final CounterLong.TimeSeries inboundTimeSeries =
 *       counter.getOrCreateTimeSeries(Collections.singletonList(LabelValue.create("SomeWork")));
 *   private static final CounterLong.TimeSeries defaultTimeSeries = counter.getDefaultTimeSeries();
 *
 *   void doDefaultWork() {
 *      // Your code here.
 *      defaultTimeSeries.add(10);
 *   }
 *
 *   void doSomeWork() {
 *      // Your code here.
 *      inboundTimeSeries.set(15);
 *   }
 *
 * }
 * }</pre>
 *
 * @since 0.1.0
 */
@ThreadSafe
public interface CounterLong extends Metric<TimeSeries> {

  @Override
  TimeSeries getOrCreateTimeSeries(List<LabelValue> labelValues);

  @Override
  TimeSeries getDefaultTimeSeries();

  /**
   * The value of a single point in the Cumulative.TimeSeries.
   *
   * @since 0.1.0
   */
  interface TimeSeries {

    /**
     * Adds the given value to the current value. The values cannot be negative.
     *
     * @param delta the value to add
     * @since 0.1.0
     */
    void add(long delta);

    /**
     * Sets the given value. The value must be larger than the current recorded value.
     *
     * <p>In general should be used in combination with {@link #setCallback(Runnable)} where the
     * recorded value is guaranteed to be monotonically increasing.
     *
     * @param val the new value.
     * @since 0.1.0
     */
    void set(long val);
  }

  /** Builder class for {@link CounterLong}. */
  interface Builder extends Metric.Builder<Builder, CounterLong> {}
}
