/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import java.util.Collections;
import org.junit.jupiter.api.Test;

class OpenTelemetrySdkAutoConfigurationTest {

  @Test
  void resourcePrioritizesUser() {
    Resource resource =
        OpenTelemetrySdkAutoConfiguration.configureResource(
            ConfigProperties.createForTest(
                Collections.singletonMap("otel.resource.attributes", "telemetry.sdk.name=test")));
    assertThat(resource.getAttributes().get(ResourceAttributes.TELEMETRY_SDK_NAME))
        .isEqualTo("test");
  }
}
