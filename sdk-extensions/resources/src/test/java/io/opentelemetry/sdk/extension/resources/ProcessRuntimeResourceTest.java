/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extension.resources;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.resources.ResourceAttributes;
import org.junit.jupiter.api.Test;

class ProcessRuntimeResourceTest {
  @Test
  void shouldCreateRuntimeAttributes() {
    // when
    Attributes attributes = new ProcessRuntimeResource().getAttributes();

    // then
    assertThat(attributes.get(ResourceAttributes.PROCESS_RUNTIME_NAME)).isNotBlank();
    assertThat(attributes.get(ResourceAttributes.PROCESS_RUNTIME_VERSION)).isNotBlank();
    assertThat(attributes.get(ResourceAttributes.PROCESS_RUNTIME_DESCRIPTION)).isNotBlank();
  }

  @Test
  void inDefault() {
    // when
    Attributes attributes = Resource.getDefault().getAttributes();

    // then
    assertThat(attributes.get(ResourceAttributes.PROCESS_RUNTIME_NAME)).isNotBlank();
    assertThat(attributes.get(ResourceAttributes.PROCESS_RUNTIME_VERSION)).isNotBlank();
    assertThat(attributes.get(ResourceAttributes.PROCESS_RUNTIME_DESCRIPTION)).isNotBlank();
  }
}
