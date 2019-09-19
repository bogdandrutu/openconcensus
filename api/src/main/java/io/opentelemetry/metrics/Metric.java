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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Base interface for all metrics defined in this package.
 *
 * @param <H> the Handle.
 * @since 0.1.0
 */
@ThreadSafe
public interface Metric<H> {
  /**
   * Returns a {@code Handle} with associated with specified {@code labelValues}. Multiples requests
   * with the same {@code labelValues} may return the same {@code Handle}.
   *
   * <p>It is recommended to keep a reference to the Handle instead of always calling this method
   * for every operations.
   *
   * @param labelValues the list of label values. The number of label values must be the same to
   *     that of the label keys passed to {@link GaugeDouble.Builder#setLabelKeys(List)}.
   * @return a {@code Handle} the value of single gauge.
   * @throws NullPointerException if {@code labelValues} is null OR any element of {@code
   *     labelValues} is null.
   * @throws IllegalArgumentException if number of {@code labelValues}s are not equal to the label
   *     keys.
   * @since 0.1.0
   */
  H getHandle(List<String> labelValues);

  /**
   * Returns a {@code Handle} for a metric with all labels not set.
   *
   * @return a {@code Handle} for a metric with all labels not set.
   * @since 0.1.0
   */
  H getDefaultHandle();

  /**
   * Sets a callback that gets executed every time before exporting this metric.
   *
   * <p>Evaluation is deferred until needed, if this {@code Metric} is not exported then it will
   * never be called.
   *
   * @param metricUpdater the callback to be executed before export.
   * @since 0.1.0
   */
  void setCallback(Runnable metricUpdater);

  /**
   * Removes the {@code Handle} from the metric, if it is present. i.e. references to previous
   * {@code Handle} are invalid (not part of the metric).
   *
   * <p>If value is missing for one of the predefined keys {@code null} must be used for that value.
   *
   * @param labelValues the list of label values.
   * @since 0.1.0
   */
  void removeHandle(List<String> labelValues);

  /**
   * The {@code Builder} class for the {@code Metric}.
   *
   * @param <B> the specific builder object.
   * @param <V> the return value for {@code build()}.
   */
  interface Builder<B extends Builder<B, V>, V> {
    /**
     * Sets the description of the {@code Metric}.
     *
     * <p>Default value is {@code ""}.
     *
     * @param description the description of the Metric.
     * @return this.
     */
    B setDescription(String description);

    /**
     * Sets the unit of the {@code Metric}.
     *
     * <p>Default value is {@code "1"}.
     *
     * @param unit the unit of the Metric.
     * @return this.
     */
    B setUnit(String unit);

    /**
     * Sets the list of label keys for the Metric.
     *
     * <p>Default value is {@link Collections#emptyList()}
     *
     * @param labelKeys the list of label keys for the Metric.
     * @return this.
     */
    B setLabelKeys(List<String> labelKeys);

    /**
     * Sets the map of constant labels (they will be added to all the Handle) for the Metric.
     *
     * <p>Default value is {@link Collections#emptyMap()}.
     *
     * @param constantLabels the map of constant labels for the Metric.
     * @return this.
     */
    B setConstantLabels(Map<String, String> constantLabels);

    /**
     * Builds and returns a {@code Metric} with the desired options.
     *
     * @return a {@code Metric} with the desired options.
     */
    V build();
  }
}
