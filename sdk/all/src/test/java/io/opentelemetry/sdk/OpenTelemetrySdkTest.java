/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.OpenTelemetry;
import io.opentelemetry.baggage.BaggageManager;
import io.opentelemetry.common.Attributes;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.metrics.MeterProvider;
import io.opentelemetry.sdk.OpenTelemetrySdk.ObfuscatedTracerProvider;
import io.opentelemetry.sdk.common.Clock;
import io.opentelemetry.sdk.internal.MillisClock;
import io.opentelemetry.sdk.metrics.MeterSdkProvider;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.TracerSdkProvider;
import io.opentelemetry.trace.TracerProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class OpenTelemetrySdkTest {

  @Mock private TracerProvider tracerProvider;
  @Mock private MeterProvider meterProvider;
  @Mock private BaggageManager baggageManager;
  @Mock private ContextPropagators propagators;
  @Mock private Clock clock;

  @Test
  void testGlobalDefault() {
    assertThat(((TracerSdkProvider) OpenTelemetrySdk.getGlobalTracerManagement()).get(""))
        .isSameAs(OpenTelemetry.getGlobalTracerProvider().get(""));
    assertThat(OpenTelemetrySdk.getGlobalMeterProvider())
        .isSameAs(OpenTelemetry.getGlobalMeterProvider());
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
    assertThat(openTelemetry.getClock()).isEqualTo(MillisClock.getInstance());
  }

  @Test
  void testReconfigure() {
    Resource resource = Resource.create(Attributes.builder().put("cat", "meow").build());
    OpenTelemetrySdk openTelemetry =
        OpenTelemetrySdk.builder()
            .setTracerProvider(tracerProvider)
            .setMeterProvider(meterProvider)
            .setPropagators(propagators)
            .setClock(clock)
            .setResource(resource)
            .build();
    assertThat(openTelemetry.getTracerProvider()).isEqualTo(tracerProvider);
    assertThat(openTelemetry.getMeterProvider()).isEqualTo(meterProvider);
    assertThat(openTelemetry.getPropagators()).isEqualTo(propagators);
    assertThat(openTelemetry.getResource()).isEqualTo(resource);
    assertThat(openTelemetry.getClock()).isEqualTo(clock);
  }
}
