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

import static com.google.common.truth.Truth.assertThat;

import io.opentelemetry.sdk.metrics.data.MetricData.LongSummaryPoint;
import io.opentelemetry.sdk.metrics.data.MetricData.LongValueAtPercentile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.Test;

public class LongMinMaxSumCountTest {

  @Test
  public void testRecordings() {
    LongMinMaxSumCount aggregator = new LongMinMaxSumCount();

    assertThat(aggregator.toPoint(0, 100, Collections.<String, String>emptyMap()))
        .isEqualTo(
            LongSummaryPoint.create(
                0,
                100,
                Collections.<String, String>emptyMap(),
                0,
                0,
                Collections.<LongValueAtPercentile>emptyList()));

    aggregator.recordLong(100);
    assertThat(aggregator.toPoint(0, 100, Collections.<String, String>emptyMap()))
        .isEqualTo(
            LongSummaryPoint.create(
                0,
                100,
                Collections.<String, String>emptyMap(),
                1,
                100,
                createPercentileValues(100L, 100L)));

    aggregator.recordLong(50);
    assertThat(aggregator.toPoint(0, 100, Collections.<String, String>emptyMap()))
        .isEqualTo(
            LongSummaryPoint.create(
                0,
                100,
                Collections.<String, String>emptyMap(),
                2,
                150,
                createPercentileValues(50L, 100L)));

    aggregator.recordLong(-75);
    assertThat(aggregator.toPoint(0, 100, Collections.<String, String>emptyMap()))
        .isEqualTo(
            LongSummaryPoint.create(
                0,
                100,
                Collections.<String, String>emptyMap(),
                3,
                75,
                createPercentileValues(-75L, 100L)));
  }

  @Test
  public void testMergeAndReset() {
    LongMinMaxSumCount aggregator = new LongMinMaxSumCount();

    aggregator.recordLong(100);
    LongMinMaxSumCount mergedToAggregator = new LongMinMaxSumCount();
    aggregator.mergeToAndReset(mergedToAggregator);

    assertThat(mergedToAggregator.toPoint(0, 100, Collections.<String, String>emptyMap()))
        .isEqualTo(
            LongSummaryPoint.create(
                0,
                100,
                Collections.<String, String>emptyMap(),
                1,
                100,
                createPercentileValues(100L, 100L)));

    assertThat(aggregator.toPoint(0, 100, Collections.<String, String>emptyMap()))
        .isEqualTo(
            LongSummaryPoint.create(
                0,
                100,
                Collections.<String, String>emptyMap(),
                0,
                0,
                Collections.<LongValueAtPercentile>emptyList()));
  }

  @Test
  public void testMultithreadedUpdates() throws Exception {
    final LongMinMaxSumCount aggregator = new LongMinMaxSumCount();
    final LongMinMaxSumCount summarizer = new LongMinMaxSumCount();
    int numberOfThreads = 10;
    final long[] updates = new long[] {1, 2, 3, 5, 7, 11, 13, 17, 19, 23};
    final int numberOfUpdates = 1000;
    final CountDownLatch startingGun = new CountDownLatch(numberOfThreads);
    List<Thread> workers = new ArrayList<>();
    for (int i = 0; i < numberOfThreads; i++) {
      final int index = i;
      Thread t =
          new Thread(
              new Runnable() {
                @Override
                public void run() {
                  long update = updates[index];
                  try {
                    startingGun.await();
                  } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                  }
                  for (int j = 0; j < numberOfUpdates; j++) {
                    aggregator.recordLong(update);
                    if (ThreadLocalRandom.current().nextInt(10) == 0) {
                      aggregator.mergeToAndReset(summarizer);
                    }
                  }
                }
              });
      workers.add(t);
      t.start();
    }
    for (int i = 0; i <= numberOfThreads; i++) {
      startingGun.countDown();
    }

    for (Thread worker : workers) {
      worker.join();
    }
    // make sure everything gets merged when all the aggregation is done.
    aggregator.mergeToAndReset(summarizer);

    assertThat(summarizer.toPoint(0, 100, Collections.<String, String>emptyMap()))
        .isEqualTo(
            LongSummaryPoint.create(
                0,
                100,
                Collections.<String, String>emptyMap(),
                numberOfThreads * numberOfUpdates,
                101000,
                createPercentileValues(1L, 23L)));
  }

  private static List<LongValueAtPercentile> createPercentileValues(long min, long max) {
    return Arrays.asList(
        LongValueAtPercentile.create(0.0, min),
        LongValueAtPercentile.create(100.0, max));
  }
}
