/*
 * Copyright 2020, OpenTelemetry Authors
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

package io.opentelemetry.sdk.metrics;

import javax.annotation.concurrent.ThreadSafe;

/** Aggregator represents the base class for all the available aggregations. */
@ThreadSafe
interface Aggregator<T extends Aggregator<?>> {

  /**
   * Merge aggregated values between the current instance and the given {@code aggregator}.
   *
   * @param aggregator value to merge with.
   */
  void merge(T aggregator);

  /**
   * LongAggregator represents the base class for all the available aggregations that work with long
   * values.
   */
  @ThreadSafe
  interface LongAggregator<T extends LongAggregator<?>> extends Aggregator<T> {

    /**
     * Updates the current aggregator with a newly recorded value.
     *
     * @param value the new {@code long} value to be added.
     */
    void update(long value);
  }

  /**
   * DoubleAggregator represents the base class for all the available aggregations that work with
   * double values.
   */
  @ThreadSafe
  interface DoubleAggregator<T extends DoubleAggregator<?>> extends Aggregator<T> {
    /**
     * Updates the current aggregator with a newly recorded value.
     *
     * @param value the new {@code double} value to be added.
     */
    void update(double value);
  }
}
