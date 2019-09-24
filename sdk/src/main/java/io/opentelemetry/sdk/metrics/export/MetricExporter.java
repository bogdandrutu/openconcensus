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

package io.opentelemetry.sdk.metrics.export;

import io.opentelemetry.proto.metrics.v1.Metric;
import java.util.Collection;

/**
 * Base interface that represents a metric exporter.
 *
 * @since 0.1.0
 */
public interface MetricExporter {

  /**
   * The possible results for the export method.
   *
   * @since 0.1.0
   */
  enum ResultCode {
    /** The export operation finished successfully. */
    SUCCESS,

    /** The export operation finished with an error, but retrying may succeed. */
    FAILED_RETRYABLE,

    /**
     * The export operation finished with an error, the caller should not try to export the same
     * data again.
     */
    FAILED_NOT_RETRYABLE
  }

  /**
   * Exports the list of given {@link Metric}.
   *
   * @param metrics the list of {@link Metric} to be exported.
   * @since 0.1.0
   */
  ResultCode export(Collection<Metric> metrics);

  // TODO: do we need a shutdown() method?
}
