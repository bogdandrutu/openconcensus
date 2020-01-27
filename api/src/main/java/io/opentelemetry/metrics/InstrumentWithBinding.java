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

package io.opentelemetry.metrics;

/**
 * Base interface for all metrics with bounds defined in this package.
 *
 * @param <B> the specific type of Bound Instrument this instrument can provide.
 * @since 0.1.0
 */
public interface InstrumentWithBinding<B> extends Instrument {
  /**
   * Returns a {@code Bound Instrument} associated with the specified {@code labelSet}. Multiples
   * requests with the same {@code labelSet} may return the same {@code Bound Instrument} instance.
   *
   * <p>It is recommended that callers keep a reference to the Bound Instrument instead of always
   * calling this method for every operation.
   *
   * @param labelSet the set of labels.
   * @return a {@code Bound Instrument}
   * @throws NullPointerException if {@code labelValues} is null.
   * @since 0.1.0
   */
  B bind(LabelSet labelSet);

  /**
   * Removes the {@code Bound Instrument} from the Instrument. i.e. references to previous {@code
   * Bound Instrument} are invalid (not being managed by the instrument).
   *
   * @param boundInstrument the {@code Bound Instrument} to be removed.
   * @since 0.1.0
   */
  void unbind(B boundInstrument);
}
