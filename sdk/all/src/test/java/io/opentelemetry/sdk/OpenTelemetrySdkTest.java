/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.mockito.Mockito.mock;

import io.opentelemetry.api.DefaultOpenTelemetry;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.TracerProvider;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.sdk.OpenTelemetrySdk.ObfuscatedTracerProvider;
import io.opentelemetry.sdk.common.Clock;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.IdGenerator;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.config.TraceConfig;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class OpenTelemetrySdkTest {

  @Mock private SdkTracerProvider tracerProvider;
  @Mock private SdkMeterProvider meterProvider;
  @Mock private ContextPropagators propagators;
  @Mock private Clock clock;
  @Mock private SpanProcessor spanProcessor;

  @Test
  void testGetGlobal() {
    assertThat(OpenTelemetrySdk.get()).isSameAs(GlobalOpenTelemetry.get());
  }

  @Test
  void testGetTracerManagementWhenNotTracerSdk() {
    OpenTelemetry previous = GlobalOpenTelemetry.get();
    assertThatCode(OpenTelemetrySdk::getGlobalTracerManagement).doesNotThrowAnyException();
    try {
      GlobalOpenTelemetry.set(
          DefaultOpenTelemetry.builder().setTracerProvider(tracerProvider).build());
      assertThatThrownBy(OpenTelemetrySdk::getGlobalTracerManagement)
          .isInstanceOf(IllegalStateException.class);
    } finally {
      GlobalOpenTelemetry.set(previous);
    }
  }

  @Test
  void testGlobalDefault() {
    assertThat(((SdkTracerProvider) OpenTelemetrySdk.getGlobalTracerManagement()).get(""))
        .isSameAs(GlobalOpenTelemetry.getTracerProvider().get(""));
    assertThat(OpenTelemetrySdk.getGlobalTracerManagement()).isNotNull();
  }

  @Test
  void testShortcutVersions() {
    assertThat(GlobalOpenTelemetry.getTracer("testTracer1"))
        .isEqualTo(GlobalOpenTelemetry.getTracerProvider().get("testTracer1"));
    assertThat(GlobalOpenTelemetry.getTracer("testTracer2", "testVersion"))
        .isEqualTo(GlobalOpenTelemetry.getTracerProvider().get("testTracer2", "testVersion"));
  }

  @Test
  void testBuilderDefaults() {
    OpenTelemetrySdk openTelemetry = OpenTelemetrySdk.builder().build();
    assertThat(openTelemetry.getTracerProvider())
        .isInstanceOfSatisfying(
            ObfuscatedTracerProvider.class,
            obfuscatedTracerProvider ->
                assertThat(obfuscatedTracerProvider.unobfuscate())
                    .isInstanceOf(SdkTracerProvider.class));
  }

  @Test
  void building() {
    OpenTelemetrySdk openTelemetry =
        OpenTelemetrySdk.builder()
            .setTracerProvider(tracerProvider)
            .setPropagators(propagators)
            .build();
    assertThat(((ObfuscatedTracerProvider) openTelemetry.getTracerProvider()).unobfuscate())
        .isEqualTo(tracerProvider);
    assertThat(openTelemetry.getPropagators()).isEqualTo(propagators);
  }

  @Test
  void testConfiguration_tracerSettings() {
    Resource resource = Resource.create(Attributes.builder().put("cat", "meow").build());
    IdGenerator idGenerator = mock(IdGenerator.class);
    TraceConfig traceConfig = mock(TraceConfig.class);
    OpenTelemetrySdk openTelemetry =
        OpenTelemetrySdk.builder()
            .setTracerProvider(
                SdkTracerProvider.builder()
                    .setClock(clock)
                    .setResource(resource)
                    .setIdGenerator(idGenerator)
                    .setTraceConfig(traceConfig)
                    .build())
            .build();
    TracerProvider unobfuscatedTracerProvider =
        ((ObfuscatedTracerProvider) openTelemetry.getTracerProvider()).unobfuscate();

    assertThat(unobfuscatedTracerProvider).isInstanceOf(SdkTracerProvider.class);
    // Since TracerProvider is in a different package, the only alternative to this reflective
    // approach would be to make the fields public for testing which is worse than this.
    assertThat(unobfuscatedTracerProvider)
        .extracting("sharedState")
        .hasFieldOrPropertyWithValue("clock", clock)
        .hasFieldOrPropertyWithValue("resource", resource)
        .hasFieldOrPropertyWithValue("idGenerator", idGenerator)
        .hasFieldOrPropertyWithValue("activeTraceConfig", traceConfig);
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

  // This is just a demonstration of maximum that one can do with OpenTelemetry configuration.
  // Demonstrates how clear or confusing is SDK configuration
  @Test
  void fullOpenTelemetrySdkConfigurationDemo() {
    TraceConfig currentConfig = TraceConfig.getDefault();
    TraceConfig newConfig =
        currentConfig.toBuilder()
            .setSampler(mock(Sampler.class))
            .setMaxLengthOfAttributeValues(128)
            .build();

    OpenTelemetrySdkBuilder sdkBuilder =
        OpenTelemetrySdk.builder()
            .setTracerProvider(
                SdkTracerProvider.builder()
                    .addSpanProcessor(SimpleSpanProcessor.builder(mock(SpanExporter.class)).build())
                    .addSpanProcessor(SimpleSpanProcessor.builder(mock(SpanExporter.class)).build())
                    .setClock(mock(Clock.class))
                    .setIdGenerator(mock(IdGenerator.class))
                    .setResource(mock(Resource.class))
                    .setTraceConfig(newConfig)
                    .build());

    sdkBuilder.build();
  }

  // This is just a demonstration of the bare minimal required configuration in order to get useful
  // SDK.
  // Demonstrates how clear or confusing is SDK configuration
  @Test
  void trivialOpenTelemetrySdkConfigurationDemo() {
    OpenTelemetrySdk.builder()
        .setTracerProvider(
            SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.builder(mock(SpanExporter.class)).build())
                .build())
        .setPropagators(ContextPropagators.create(mock(TextMapPropagator.class)))
        .build();
  }

  // This is just a demonstration of two small but not trivial configurations.
  // Demonstrates how clear or confusing is SDK configuration
  @Test
  void minimalOpenTelemetrySdkConfigurationDemo() {
    OpenTelemetrySdk.builder()
        .setTracerProvider(
            SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.builder(mock(SpanExporter.class)).build())
                .setTraceConfig(
                    TraceConfig.getDefault().toBuilder().setSampler(mock(Sampler.class)).build())
                .build())
        .setPropagators(ContextPropagators.create(mock(TextMapPropagator.class)))
        .build();

    OpenTelemetrySdk.builder()
        .setTracerProvider(
            SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.builder(mock(SpanExporter.class)).build())
                .setTraceConfig(
                    TraceConfig.getDefault().toBuilder().setSampler(mock(Sampler.class)).build())
                .setIdGenerator(mock(IdGenerator.class))
                .build())
        .setPropagators(ContextPropagators.create(mock(TextMapPropagator.class)))
        .build();
  }
}
