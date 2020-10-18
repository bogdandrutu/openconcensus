/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.metrics;

import static org.junit.jupiter.api.Assertions.assertThrows;

import io.opentelemetry.OpenTelemetry;
import io.opentelemetry.internal.StringUtils;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class DoubleUpDownSumObserverTest {

  private static final String NAME = "name";
  private static final String DESCRIPTION = "description";
  private static final String UNIT = "1";
  private static final Meter meter = OpenTelemetry.getMeter("DoubleUpDownSumObserverTest");

  @Test
  void preventNull_Name() {
    assertThrows(
        NullPointerException.class, () -> meter.doubleUpDownSumObserverBuilder(null), "name");
  }

  @Test
  void preventEmpty_Name() {
    assertThrows(
        IllegalArgumentException.class,
        () -> meter.doubleUpDownSumObserverBuilder("").build(),
        DefaultMeter.ERROR_MESSAGE_INVALID_NAME);
  }

  @Test
  void preventNonPrintableName() {
    assertThrows(
        IllegalArgumentException.class, () -> meter.doubleUpDownSumObserverBuilder("\2").build());
  }

  @Test
  void preventTooLongName() {
    char[] chars = new char[StringUtils.METRIC_NAME_MAX_LENGTH + 1];
    Arrays.fill(chars, 'a');
    String longName = String.valueOf(chars);
    assertThrows(
        IllegalArgumentException.class,
        () -> meter.doubleUpDownSumObserverBuilder(longName).build(),
        DefaultMeter.ERROR_MESSAGE_INVALID_NAME);
  }

  @Test
  void preventNull_Description() {
    assertThrows(
        NullPointerException.class,
        () -> meter.doubleUpDownSumObserverBuilder("metric").setDescription(null).build(),
        "description");
  }

  @Test
  void preventNull_Unit() {
    assertThrows(
        NullPointerException.class,
        () -> meter.doubleUpDownSumObserverBuilder("metric").setUnit(null).build(),
        "unit");
  }

  @Test
  void preventNull_Callback() {
    DoubleUpDownSumObserver doubleUpDownSumObserver =
        meter.doubleUpDownSumObserverBuilder("metric").build();
    assertThrows(
        NullPointerException.class, () -> doubleUpDownSumObserver.setCallback(null), "callback");
  }

  @Test
  void doesNotThrow() {
    DoubleUpDownSumObserver doubleUpDownSumObserver =
        meter
            .doubleUpDownSumObserverBuilder(NAME)
            .setDescription(DESCRIPTION)
            .setUnit(UNIT)
            .build();
    doubleUpDownSumObserver.setCallback(result -> {});
  }
}
