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

import io.opentelemetry.metrics.DoubleSumObserver.ResultDoubleSumObserver;
import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;

/**
 * {@code SumObserver} is the asynchronous instrument corresponding to Counter, used to capture a
 * monotonic sum with Observe(sum).
 *
 * <p>"Sum" appears in the name to remind that it is used to capture sums directly. Use a
 * SumObserver to capture any value that starts at zero and rises throughout the process lifetime
 * and never falls.
 *
 * <p>A {@code SumObserver} is a good choice in situations where a measurement is expensive to
 * compute, such that it would be wasteful to compute on every request.
 *
 * <p>Example:
 *
 * <pre>{@code
 * class YourClass {
 *
 *   private static final Meter meter = OpenTelemetry.getMeterRegistry().get("my_library_name");
 *   private static final DoubleSumObserver cpuObserver =
 *       meter.
 *           .doubleSumObserverBuilder("cpu_time")
 *           .setDescription("System CPU usage")
 *           .setUnit("ms")
 *           .build();
 *
 *   void init() {
 *     cpuObserver.setCallback(
 *         new DoubleSumObserver.Callback<DoubleObserver.ResultDoubleObserver>() {
 *          {@literal @}Override
 *           public void update(Result result) {
 *             // Get system cpu usage
 *             result.observe(cpuIdle, "state", "idle");
 *             result.observe(cpuUser, "state", "user");
 *           }
 *         });
 *   }
 * }
 * }</pre>
 *
 * @since 0.1.0
 */
@ThreadSafe
public interface DoubleSumObserver extends AsynchronousInstrument<ResultDoubleSumObserver> {
  @Override
  void setCallback(Callback<ResultDoubleSumObserver> metricUpdater);

  /** Builder class for {@link DoubleSumObserver}. */
  interface Builder extends AsynchronousInstrument.Builder {
    @Override
    Builder setDescription(String description);

    @Override
    Builder setUnit(String unit);

    @Override
    Builder setConstantLabels(Map<String, String> constantLabels);

    @Override
    DoubleSumObserver build();
  }

  /** The result for the {@link AsynchronousInstrument.Callback}. */
  interface ResultDoubleSumObserver {
    void observe(double sum, String... keyValueLabelPairs);
  }
}
