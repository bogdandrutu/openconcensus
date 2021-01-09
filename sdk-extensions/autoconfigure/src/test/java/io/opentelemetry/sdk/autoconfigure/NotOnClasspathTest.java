/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.autoconfigure;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import java.util.Collections;
import org.junit.jupiter.api.Test;

class NotOnClasspathTest {

  private static final ConfigProperties EMPTY =
      ConfigProperties.createForTest(Collections.emptyMap());

  @Test
  void otlpSpans() {
    assertThatThrownBy(() -> SpanExporterConfiguration.configureExporter("otlp", EMPTY))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining(
            "OTLP Trace Exporter enabled but opentelemetry-exporter-otlp-trace not found on "
                + "classpath");
  }

  @Test
  void jaeger() {
    assertThatThrownBy(() -> SpanExporterConfiguration.configureExporter("jaeger", EMPTY))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining(
            "Jaeger gRPC Exporter enabled but opentelemetry-exporter-jaeger not found on "
                + "classpath");
  }

  @Test
  void zipkin() {
    assertThatThrownBy(() -> SpanExporterConfiguration.configureExporter("zipkin", EMPTY))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining(
            "Zipkin Exporter enabled but opentelemetry-exporter-zipkin not found on classpath");
  }

  @Test
  void logging() {
    assertThatThrownBy(() -> SpanExporterConfiguration.configureExporter("logging", EMPTY))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining(
            "Logging Trace Exporter enabled but opentelemetry-exporter-logging not found on "
                + "classpath");
  }

  @Test
  void otlpMetrics() {
    assertThatThrownBy(
            () ->
                MetricExporterConfiguration.configureExporter(
                    "otlp", EMPTY, false, SdkMeterProvider.builder().build()))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining(
            "OTLP Metrics Exporter enabled but opentelemetry-exporter-otlp-metrics not found on "
                + "classpath");
  }

  @Test
  void prometheus() {
    assertThatThrownBy(
            () ->
                MetricExporterConfiguration.configureExporter(
                    "prometheus", EMPTY, false, SdkMeterProvider.builder().build()))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining(
            "Prometheus Metrics Server enabled but opentelemetry-exporter-prometheus not found on "
                + "classpath");
  }

  @Test
  void b3propagator() {
    assertThatThrownBy(
            () ->
                PropagatorConfiguration.configurePropagators(
                    ConfigProperties.createForTest(
                        Collections.singletonMap("otel.propagators", "b3"))))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining(
            "b3 propagator enabled but opentelemetry-extension-trace-propagators not found on "
                + "classpath");
  }
}
