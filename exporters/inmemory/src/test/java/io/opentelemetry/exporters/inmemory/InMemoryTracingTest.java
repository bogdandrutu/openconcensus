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

package io.opentelemetry.exporters.inmemory;

import static com.google.common.truth.Truth.assertThat;

import io.opentelemetry.scope.DefaultScopeManager;
import io.opentelemetry.scope.ScopeManager;
import io.opentelemetry.sdk.trace.Samplers;
import io.opentelemetry.sdk.trace.TracerSdkProvider;
import io.opentelemetry.sdk.trace.config.TraceConfig;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.trace.Tracer;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link InMemoryTracing}. */
@RunWith(JUnit4.class)
public class InMemoryTracingTest {
  private final ScopeManager scopeManager = DefaultScopeManager.getInstance();
  private final TracerSdkProvider tracerSdkProvider =
      TracerSdkProvider.builder(scopeManager).build();
  private final InMemoryTracing tracing =
      InMemoryTracing.builder().setTracerProvider(tracerSdkProvider).build();
  private final Tracer tracer = tracerSdkProvider.get("InMemoryTracing");

  @Rule public final ExpectedException thrown = ExpectedException.none();

  @Test
  public void defaultInstance() {
    assertThat(tracing.getTracerProvider()).isSameInstanceAs(tracerSdkProvider);
    assertThat(tracing.getSpanExporter().getFinishedSpanItems()).hasSize(0);
  }

  @Test
  public void ctor_nullTracer() {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("tracerProvider");
    InMemoryTracing.builder().setTracerProvider(null).build();
  }

  @Test
  public void getFinishedSpanItems() {
    tracer.spanBuilder("A").startSpan().end();
    tracer.spanBuilder("B").startSpan().end();

    List<SpanData> finishedSpanItems = tracing.getSpanExporter().getFinishedSpanItems();
    assertThat(finishedSpanItems).hasSize(2);
    assertThat(finishedSpanItems.get(0).getName()).isEqualTo("A");
    assertThat(finishedSpanItems.get(1).getName()).isEqualTo("B");
  }

  @Test
  public void getFinishedSpanItems_sampled() {
    tracer.spanBuilder("A").startSpan().end();
    TraceConfig originalConfig = tracerSdkProvider.getActiveTraceConfig();
    tracerSdkProvider.updateActiveTraceConfig(
        originalConfig.toBuilder().setSampler(Samplers.alwaysOff()).build());
    try {
      tracer.spanBuilder("B").startSpan().end();
    } finally {
      tracerSdkProvider.updateActiveTraceConfig(originalConfig);
    }

    List<SpanData> finishedSpanItems = tracing.getSpanExporter().getFinishedSpanItems();
    assertThat(finishedSpanItems).hasSize(1);
    assertThat(finishedSpanItems.get(0).getName()).isEqualTo("A");
  }

  @Test
  public void reset() {
    tracer.spanBuilder("A").startSpan().end();
    tracer.spanBuilder("B").startSpan().end();
    assertThat(tracing.getSpanExporter().getFinishedSpanItems()).hasSize(2);

    tracing.getSpanExporter().reset();
    assertThat(tracing.getSpanExporter().getFinishedSpanItems()).hasSize(0);
  }
}
