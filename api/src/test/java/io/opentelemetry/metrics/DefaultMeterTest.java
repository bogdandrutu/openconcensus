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

package io.opentelemetry.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.OpenTelemetry;
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
