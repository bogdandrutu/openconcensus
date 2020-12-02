/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.MeterProvider;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.TracerProvider;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk.ObfuscatedTracerProvider;
import io.opentelemetry.sdk.common.Clock;
import io.opentelemetry.sdk.internal.SystemClock;
import io.opentelemetry.sdk.metrics.MeterSdkProvider;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.IdGenerator;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.TracerSdkProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class OpenTelemetrySdkTest {

  @Mock private TracerSdkProvider tracerProvider;
  @Mock private MeterSdkProvider meterProvider;
  @Mock private ContextPropagators propagators;
  @Mock private Clock clock;

  @Test
  void testGetGlobal() {
    assertThat(OpenTelemetrySdk.get()).isSameAs(OpenTelemetry.get());
  }

  @Test
  void testGetTracerManagementWhenNotTracerSdk() {
    OpenTelemetry previous = OpenTelemetry.get();
    assertThatCode(OpenTelemetrySdk::getGlobalTracerManagement).doesNotThrowAnyException();
    try {
      OpenTelemetry.set(OpenTelemetry.builder().setTracerProvider(tracerProvider).build());
      assertThatThrownBy(OpenTelemetrySdk::getGlobalTracerManagement)
          .isInstanceOf(IllegalStateException.class);
    } finally {
      OpenTelemetry.set(previous);
    }
  }

  @Test
  void testGlobalDefault() {
    assertThat(((TracerSdkProvider) OpenTelemetrySdk.getGlobalTracerManagement()).get(""))
        .isSameAs(OpenTelemetry.getGlobalTracerProvider().get(""));
    assertThat(OpenTelemetrySdk.getGlobalMeterProvider())
        .isSameAs(OpenTelemetry.getGlobalMeterProvider());
    assertThat(OpenTelemetrySdk.getGlobalTracerManagement()).isNotNull();
  }

  @Test
  void testShortcutVersions() {
    assertThat(OpenTelemetry.getGlobalTracer("testTracer1"))
        .isEqualTo(OpenTelemetry.getGlobalTracerProvider().get("testTracer1"));
    assertThat(OpenTelemetry.getGlobalTracer("testTracer2", "testVersion"))
        .isEqualTo(OpenTelemetry.getGlobalTracerProvider().get("testTracer2", "testVersion"));
    assertThat(OpenTelemetry.getGlobalMeter("testMeter1"))
        .isEqualTo(OpenTelemetry.getGlobalMeterProvider().get("testMeter1"));
    assertThat(OpenTelemetry.getGlobalMeter("testMeter2", "testVersion"))
        .isEqualTo(OpenTelemetry.getGlobalMeterProvider().get("testMeter2", "testVersion"));
  }

  @Test
  void testBuilderDefaults() {
    OpenTelemetrySdk openTelemetry = OpenTelemetrySdk.builder().build();
    assertThat(openTelemetry.getTracerProvider())
        .isInstanceOfSatisfying(
            ObfuscatedTracerProvider.class,
            obfuscatedTracerProvider ->
                assertThat(obfuscatedTracerProvider.unobfuscate())
                    .isInstanceOf(TracerSdkProvider.class));
    assertThat(openTelemetry.getMeterProvider()).isInstanceOf(MeterSdkProvider.class);
    assertThat(openTelemetry.getResource()).isEqualTo(Resource.getDefault());
    assertThat(openTelemetry.getClock()).isEqualTo(SystemClock.getInstance());
  }

  @Test
  void building() {
    Resource resource = Resource.create(Attributes.builder().put("cat", "meow").build());
    OpenTelemetrySdk openTelemetry =
        OpenTelemetrySdk.builder()
            .setTracerProvider(tracerProvider)
            .setMeterProvider(meterProvider)
            .setPropagators(propagators)
            .setClock(clock)
            .setResource(resource)
            .build();
    assertThat(((ObfuscatedTracerProvider) openTelemetry.getTracerProvider()).unobfuscate())
        .isEqualTo(tracerProvider);
    assertThat(openTelemetry.getMeterProvider()).isEqualTo(meterProvider);
    assertThat(openTelemetry.getPropagators()).isEqualTo(propagators);
    assertThat(openTelemetry.getResource()).isEqualTo(resource);
    assertThat(openTelemetry.getClock()).isEqualTo(clock);
  }

  @Test
  void testConfiguration_tracerSettings() {
    Resource resource = Resource.create(Attributes.builder().put("cat", "meow").build());
    IdGenerator idGenerator = mock(IdGenerator.class);
    OpenTelemetrySdk openTelemetry =
        OpenTelemetrySdk.builder()
            .setClock(clock)
            .setResource(resource)
            .setIdGenerator(idGenerator)
            .build();
    TracerProvider unobfuscatedTracerProvider =
        ((ObfuscatedTracerProvider) openTelemetry.getTracerProvider()).unobfuscate();

    assertThat(unobfuscatedTracerProvider).isInstanceOf(TracerSdkProvider.class);
    // Since TracerProvider is in a different package, the only alternative to this reflective
    // approach would be to make the fields public for testing which is worse than this.
    assertThat(unobfuscatedTracerProvider)
        .extracting("sharedState")
        .hasFieldOrPropertyWithValue("clock", clock)
        .hasFieldOrPropertyWithValue("resource", resource)
        .hasFieldOrPropertyWithValue("idGenerator", idGenerator);

    assertThat(openTelemetry.getMeterProvider()).isInstanceOf(MeterSdkProvider.class);
    // Since MeterProvider is in a different package, the only alternative to this reflective
    // approach would be to make the fields public for testing which is worse than this.
    assertThat(openTelemetry.getMeterProvider())
        .extracting("registry")
        .extracting("meterProviderSharedState")
        .hasFieldOrPropertyWithValue("clock", clock)
        .hasFieldOrPropertyWithValue("resource", resource);

    assertThat(openTelemetry.getResource()).isSameAs(resource);
    assertThat(openTelemetry.getClock()).isSameAs(clock);
  }

  @Test
  void addSpanProcessors() {
    SpanProcessor spanProcessor1 = mock(SpanProcessor.class);
    SpanProcessor spanProcessor2 = mock(SpanProcessor.class);
    OpenTelemetrySdk openTelemetrySdk =
        OpenTelemetrySdk.builder()
            .addSpanProcessor(spanProcessor1)
            .addSpanProcessor(spanProcessor2)
            .build();

    TracerProvider tracerProvider = openTelemetrySdk.getTracerProvider();
    Span span = tracerProvider.get("test").spanBuilder("test").startSpan();
    span.end();

    verify(spanProcessor1).isStartRequired();
    verify(spanProcessor1).isEndRequired();
    verify(spanProcessor2).isStartRequired();
    verify(spanProcessor2).isEndRequired();
  }

  @Test
  void testTracerProviderAccess() {
    OpenTelemetrySdk openTelemetry =
        OpenTelemetrySdk.builder().setTracerProvider(tracerProvider).build();
    assertThat(openTelemetry.getTracerProvider())
        .asInstanceOf(type(ObfuscatedTracerProvider.class))
        .isNotNull()
        .matches(obfuscated -> obfuscated.unobfuscate() == tracerProvider);
    assertThat(openTelemetry.getTracerManagement()).isNotNull();
  }

  @Test
  void onlySdkInstancesAllowed() {
    assertThrows(
        IllegalArgumentException.class,
        () -> OpenTelemetrySdk.builder().setMeterProvider(mock(MeterProvider.class)));
    assertThrows(
        IllegalArgumentException.class,
        () -> OpenTelemetrySdk.builder().setTracerProvider(mock(TracerProvider.class)));
  }
}
