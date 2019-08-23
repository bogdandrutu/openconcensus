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

package io.opentelemetry.sdk.contrib.trace.export;

import static com.google.common.truth.Truth.assertThat;

import io.opentelemetry.sdk.trace.MultiSpanProcessor;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.SpanProcessor;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Unit tests for {@link DisruptorAsyncSpanProcessor}. */
@RunWith(JUnit4.class)
public class DisruptorAsyncSpanProcessorTest {
  @Mock private ReadableSpan readableSpan;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  // EventQueueEntry for incrementing a Counter.
  private static class IncrementSpanProcessor implements SpanProcessor {
    private final AtomicInteger counterOnStart = new AtomicInteger(0);
    private final AtomicInteger counterOnEnd = new AtomicInteger(0);
    private final AtomicInteger counterOnShutdown = new AtomicInteger(0);

    @Override
    public void onStart(ReadableSpan span) {
      counterOnStart.incrementAndGet();
    }

    @Override
    public void onEnd(ReadableSpan span) {
      counterOnEnd.incrementAndGet();
    }

    @Override
    public void shutdown() {
      counterOnShutdown.incrementAndGet();
    }

    private int getCounterOnStart() {
      return counterOnStart.get();
    }

    private int getCounterOnEnd() {
      return counterOnEnd.get();
    }

    private int getCounterOnShutdown() {
      return counterOnShutdown.get();
    }
  }

  @Test
  public void incrementOnce() {
    IncrementSpanProcessor incrementSpanProcessor = new IncrementSpanProcessor();
    DisruptorAsyncSpanProcessor disruptorAsyncSpanProcessor =
        DisruptorAsyncSpanProcessor.newBuilder(incrementSpanProcessor).build();
    assertThat(incrementSpanProcessor.getCounterOnStart()).isEqualTo(0);
    assertThat(incrementSpanProcessor.getCounterOnEnd()).isEqualTo(0);
    disruptorAsyncSpanProcessor.onStart(readableSpan);
    disruptorAsyncSpanProcessor.onEnd(readableSpan);
    disruptorAsyncSpanProcessor.shutdown();
    assertThat(incrementSpanProcessor.getCounterOnStart()).isEqualTo(1);
    assertThat(incrementSpanProcessor.getCounterOnEnd()).isEqualTo(1);
    assertThat(incrementSpanProcessor.getCounterOnShutdown()).isEqualTo(1);
  }

  @Test
  public void shutdownIsCalledOnlyOnce() {
    IncrementSpanProcessor incrementSpanProcessor = new IncrementSpanProcessor();
    DisruptorAsyncSpanProcessor disruptorAsyncSpanProcessor =
        DisruptorAsyncSpanProcessor.newBuilder(incrementSpanProcessor).build();
    disruptorAsyncSpanProcessor.shutdown();
    disruptorAsyncSpanProcessor.shutdown();
    disruptorAsyncSpanProcessor.shutdown();
    disruptorAsyncSpanProcessor.shutdown();
    disruptorAsyncSpanProcessor.shutdown();
    assertThat(incrementSpanProcessor.getCounterOnShutdown()).isEqualTo(1);
  }

  @Test
  public void incrementAfterShutdown() {
    IncrementSpanProcessor incrementSpanProcessor = new IncrementSpanProcessor();
    DisruptorAsyncSpanProcessor disruptorAsyncSpanProcessor =
        DisruptorAsyncSpanProcessor.newBuilder(incrementSpanProcessor).build();
    disruptorAsyncSpanProcessor.shutdown();
    disruptorAsyncSpanProcessor.onStart(readableSpan);
    disruptorAsyncSpanProcessor.onEnd(readableSpan);
    assertThat(incrementSpanProcessor.getCounterOnStart()).isEqualTo(0);
    assertThat(incrementSpanProcessor.getCounterOnEnd()).isEqualTo(0);
    disruptorAsyncSpanProcessor.shutdown();
    assertThat(incrementSpanProcessor.getCounterOnShutdown()).isEqualTo(1);
  }

  @Test
  public void incrementTenK() {
    final int tenK = 10000;
    IncrementSpanProcessor incrementSpanProcessor = new IncrementSpanProcessor();
    DisruptorAsyncSpanProcessor disruptorAsyncSpanProcessor =
        DisruptorAsyncSpanProcessor.newBuilder(incrementSpanProcessor).build();
    for (int i = 0; i < tenK; i++) {
      disruptorAsyncSpanProcessor.onStart(readableSpan);
      disruptorAsyncSpanProcessor.onEnd(readableSpan);
    }
    disruptorAsyncSpanProcessor.shutdown();
    assertThat(incrementSpanProcessor.getCounterOnStart()).isEqualTo(tenK);
    assertThat(incrementSpanProcessor.getCounterOnEnd()).isEqualTo(tenK);
    assertThat(incrementSpanProcessor.getCounterOnShutdown()).isEqualTo(1);
  }

  @Test
  public void incrementMultiSpanProcessor() {
    IncrementSpanProcessor incrementSpanProcessor1 = new IncrementSpanProcessor();
    IncrementSpanProcessor incrementSpanProcessor2 = new IncrementSpanProcessor();
    DisruptorAsyncSpanProcessor disruptorAsyncSpanProcessor =
        DisruptorAsyncSpanProcessor.newBuilder(
                MultiSpanProcessor.create(
                    Arrays.<SpanProcessor>asList(incrementSpanProcessor1, incrementSpanProcessor2)))
            .build();
    disruptorAsyncSpanProcessor.onStart(readableSpan);
    disruptorAsyncSpanProcessor.onEnd(readableSpan);
    disruptorAsyncSpanProcessor.shutdown();
    assertThat(incrementSpanProcessor1.getCounterOnStart()).isEqualTo(1);
    assertThat(incrementSpanProcessor1.getCounterOnEnd()).isEqualTo(1);
    assertThat(incrementSpanProcessor1.getCounterOnShutdown()).isEqualTo(1);
    assertThat(incrementSpanProcessor2.getCounterOnStart()).isEqualTo(1);
    assertThat(incrementSpanProcessor2.getCounterOnEnd()).isEqualTo(1);
    assertThat(incrementSpanProcessor2.getCounterOnShutdown()).isEqualTo(1);
  }
}
