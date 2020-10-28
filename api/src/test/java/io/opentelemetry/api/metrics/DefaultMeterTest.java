/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.api.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.api.OpenTelemetry;
import org.junit.jupiter.api.Test;

class DefaultMeterTest {
  @Test
  void expectDefaultMeter() {
    assertThat(OpenTelemetry.getGlobalMeterProvider()).isInstanceOf(DefaultMeterProvider.class);
    assertThat(OpenTelemetry.getGlobalMeter("test")).isInstanceOf(DefaultMeter.class);
    assertThat(OpenTelemetry.getGlobalMeter("test")).isSameAs(DefaultMeter.getInstance());
    assertThat(OpenTelemetry.getGlobalMeter("test", "0.1.0")).isSameAs(DefaultMeter.getInstance());
  }

  @Test
  void expectDefaultMeterProvider() {
    assertThat(OpenTelemetry.getGlobalMeterProvider()).isSameAs(DefaultMeterProvider.getInstance());
    assertThat(OpenTelemetry.getGlobalMeterProvider().get("test")).isInstanceOf(DefaultMeter.class);
    assertThat(OpenTelemetry.getGlobalMeterProvider().get("test", "0.1.0"))
        .isInstanceOf(DefaultMeter.class);
  }
}
