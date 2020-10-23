/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.metrics.spi;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.OpenTelemetry;
import io.opentelemetry.sdk.metrics.MeterSdkProvider;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link MeterProviderFactorySdk}. */
class MeterProviderFactorySdkTest {
  @Test
  void testDefault() {
    assertThat(OpenTelemetry.getGlobalMeterProvider()).isInstanceOf(MeterSdkProvider.class);
  }
}
