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

package io.opentelemetry.sdk.metrics.aggregator;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.opentelemetry.sdk.metrics.data.MetricData.Point;
import io.opentelemetry.sdk.metrics.data.MetricData.SummaryPoint;
import io.opentelemetry.sdk.metrics.data.MetricData.ValueAtPercentile;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public final class DoubleMinMaxSumCount extends AbstractAggregator {

  private static final AggregatorFactory AGGREGATOR_FACTORY =
      new AggregatorFactory() {
        @Override
        public Aggregator getAggregator() {
          return new DoubleMinMaxSumCount();
        }
      };

  // The current value. This controls its own internal thread-safety via method access. Don't
  // try to use its fields directly.
  private final DoubleSummary current = new DoubleSummary();

  public static AggregatorFactory getFactory() {
    return AGGREGATOR_FACTORY;
  }

  @Override
  void doMergeAndReset(Aggregator target) {
    DoubleMinMaxSumCount other = (DoubleMinMaxSumCount) target;

    DoubleSummary copy = current.copyAndReset();
    other.current.update(copy);
  }

  @Nullable
  @Override
  public Point toPoint(long startEpochNanos, long epochNanos, Map<String, String> labels) {
    return current.toPoint(startEpochNanos, epochNanos, labels);
  }

  @Override
  public void recordDouble(double value) {
    current.record(value);
  }

  private static final class DoubleSummary {

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    @GuardedBy("lock")
    private double sum = 0;

    @GuardedBy("lock")
    private long count = 0;

    @GuardedBy("lock")
    private double min = Double.POSITIVE_INFINITY;

    @GuardedBy("lock")
    private double max = Double.NEGATIVE_INFINITY;

    private void update(DoubleSummary summary) {
      lock.writeLock().lock();
      try {
        this.count += summary.count;
        this.sum += summary.sum;
        this.min = Math.min(summary.min, this.min);
        this.max = Math.max(summary.max, this.max);
      } finally {
        lock.writeLock().unlock();
      }
    }

    private void record(double value) {
      lock.writeLock().lock();
      try {
        count++;
        sum += value;
        min = Math.min(value, min);
        max = Math.max(value, max);
      } finally {
        lock.writeLock().unlock();
      }
    }

    private DoubleSummary copyAndReset() {
      DoubleSummary copy = new DoubleSummary();
      lock.writeLock().lock();
      try {
        copy.count = count;
        copy.sum = sum;
        copy.min = min;
        copy.max = max;
        count = 0;
        sum = 0;
        min = Double.POSITIVE_INFINITY;
        max = Double.NEGATIVE_INFINITY;
      } finally {
        lock.writeLock().unlock();
      }
      return copy;
    }

    private SummaryPoint toPoint(
        long startEpochNanos, long epochNanos, Map<String, String> labels) {
      lock.readLock().lock();
      try {
        return SummaryPoint.create(
            startEpochNanos,
            epochNanos,
            labels,
            count,
            sum,
            count == 0
                ? Collections.<ValueAtPercentile>emptyList()
                : Arrays.asList(
                    ValueAtPercentile.create(0.0, min), ValueAtPercentile.create(100.0, max)));
      } finally {
        lock.readLock().unlock();
      }
    }
  }
}
