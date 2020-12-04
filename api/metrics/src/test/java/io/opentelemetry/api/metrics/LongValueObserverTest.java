/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.api.metrics;

import static io.opentelemetry.api.internal.StringUtils.METRIC_NAME_MAX_LENGTH;
import static io.opentelemetry.api.metrics.DefaultMeter.ERROR_MESSAGE_INVALID_NAME;
import static java.util.Arrays.fill;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/** Unit tests for {@link LongValueObserver}. */
class LongValueObserverTest {

  private static final String NAME = "name";
  private static final String DESCRIPTION = "description";
  private static final String UNIT = "1";
  private static final Meter meter = DefaultMeter.getInstance();

  @Test
  void preventNull_Name() {
    assertThrows(NullPointerException.class, () -> meter.longValueObserverBuilder(null), "name");
  }

  @Test
  void preventEmpty_Name() {
    assertThrows(
        IllegalArgumentException.class,
        () -> meter.longValueObserverBuilder("").build(),
        ERROR_MESSAGE_INVALID_NAME);
  }

  @Test
  void preventNonPrintableName() {
    assertThrows(
        IllegalArgumentException.class,
        () -> meter.longValueObserverBuilder("\2").build(),
        ERROR_MESSAGE_INVALID_NAME);
  }

  @Test
  void preventTooLongName() {
    char[] chars = new char[METRIC_NAME_MAX_LENGTH + 1];
    fill(chars, 'a');
    String longName = String.valueOf(chars);
    assertThrows(
        IllegalArgumentException.class,
        () -> meter.longValueObserverBuilder(longName).build(),
        ERROR_MESSAGE_INVALID_NAME);
  }

  @Test
  void preventNull_Description() {
    assertThrows(
        NullPointerException.class,
        () -> meter.longValueObserverBuilder("metric").setDescription(null).build(),
        "description");
  }

  @Test
  void preventNull_Unit() {
    assertThrows(
        NullPointerException.class,
        () -> meter.longValueObserverBuilder("metric").setUnit(null).build(),
        "unit");
  }

  @Test
  void preventNull_Callback() {
    assertThrows(
        NullPointerException.class,
        () -> meter.longValueObserverBuilder("metric").setCallback(null).build(),
        "callback");
  }

  @Test
  void doesNotThrow() {
    meter
        .longValueObserverBuilder(NAME)
        .setDescription(DESCRIPTION)
        .setUnit(UNIT)
        .setCallback(result -> {})
        .build();
  }
}
