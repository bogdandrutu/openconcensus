/*
 * Copyright 2019, OpenConsensus Authors
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

package openconsensus.opencensusshim.metrics.export;

import java.util.Collection;
import openconsensus.opencensusshim.common.ExperimentalApi;

/**
 * A {@link Metric} producer that can be registered for exporting using {@link
 * MetricProducerManager}.
 *
 * <p>All implementation MUST be thread-safe.
 *
 * @since 0.1.0
 */
@ExperimentalApi
public abstract class MetricProducer {

  /**
   * Returns a collection of produced {@link Metric}s to be exported.
   *
   * @return a collection of produced {@link Metric}s to be exported.
   * @since 0.1.0
   */
  public abstract Collection<Metric> getMetrics();
}
