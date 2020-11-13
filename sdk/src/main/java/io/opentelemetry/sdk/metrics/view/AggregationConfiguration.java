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

package io.opentelemetry.sdk.metrics.view;

import com.google.auto.value.AutoValue;
import io.opentelemetry.metrics.Instrument;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

@AutoValue
@Immutable
public abstract class AggregationConfiguration {

  public static AggregationConfiguration create(Aggregation aggregation, Temporality temporality) {
    return new AutoValue_AggregationConfiguration(aggregation, temporality);
  }

  /** Returns the {@link Aggregation} that should be used for this View. */
  @Nullable
  public abstract Aggregation aggregation();

  /** Returns the {@link Temporality} that should be used for this View (delta vs. cumulative). */
  @Nullable
  public abstract Temporality temporality();

  /** An enumeration which describes the time period over which metrics should be aggregated. */
  public enum Temporality {
    /** Metrics will be aggregated only over the most recent collection interval. */
    DELTA,
    /** Metrics will be aggregated over the lifetime of the associated {@link Instrument}. */
    CUMULATIVE
  }
}
