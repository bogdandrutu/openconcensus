/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.metrics;

import static org.junit.jupiter.api.Assertions.assertThrows;

import io.opentelemetry.OpenTelemetry;
import io.opentelemetry.common.Labels;
import io.opentelemetry.internal.StringUtils;
import io.opentelemetry.metrics.LongUpDownCounter.BoundLongUpDownCounter;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class LongUpDownCounterTest {

  private static final String NAME = "name";
  private static final String DESCRIPTION = "description";
  private static final String UNIT = "1";
  private static final Labels CONSTANT_LABELS = Labels.of("key", "value");

  private final Meter meter = OpenTelemetry.getMeter("LongUpDownCounterTest");

  @Test
  void preventNull_Name() {
    assertThrows(NullPointerException.class, () -> meter.longUpDownCounterBuilder(null), "name");
  }

  @Test
  void preventEmpty_Name() {
    assertThrows(
        IllegalArgumentException.class,
        () -> meter.longUpDownCounterBuilder("").build(),
        DefaultMeter.ERROR_MESSAGE_INVALID_NAME);
  }

  @Test
  void preventNonPrintableName() {
    assertThrows(
        IllegalArgumentException.class,
        () -> meter.longUpDownCounterBuilder("\2").build(),
        DefaultMeter.ERROR_MESSAGE_INVALID_NAME);
  }

  @Test
  void preventTooLongName() {
    char[] chars = new char[StringUtils.NAME_MAX_LENGTH + 1];
    Arrays.fill(chars, 'a');
    String longName = String.valueOf(chars);
    assertThrows(
        IllegalArgumentException.class,
        () -> meter.longUpDownCounterBuilder(longName).build(),
        DefaultMeter.ERROR_MESSAGE_INVALID_NAME);
  }

  @Test
  void preventNull_Description() {
    assertThrows(
        NullPointerException.class,
        () -> meter.longUpDownCounterBuilder("metric").setDescription(null).build(),
        "description");
  }

  @Test
  void preventNull_Unit() {
    assertThrows(
        NullPointerException.class,
        () -> meter.longUpDownCounterBuilder("metric").setUnit(null).build(),
        "unit");
  }

  @Test
  void preventNull_ConstantLabels() {
    assertThrows(
        NullPointerException.class,
        () -> meter.longUpDownCounterBuilder("metric").setConstantLabels(null).build(),
        "constantLabels");
  }

  @Test
  void add_PreventNullLabels() {
    assertThrows(
        NullPointerException.class,
        () -> meter.longUpDownCounterBuilder("metric").build().add(1, null),
        "labels");
  }

  @Test
  void add_DoesNotThrow() {
    LongUpDownCounter longUpDownCounter =
        meter
            .longUpDownCounterBuilder(NAME)
            .setDescription(DESCRIPTION)
            .setUnit(UNIT)
            .setConstantLabels(CONSTANT_LABELS)
            .build();
    longUpDownCounter.add(1, Labels.empty());
    longUpDownCounter.add(-1, Labels.empty());
    longUpDownCounter.add(1);
    longUpDownCounter.add(-1);
  }

  @Test
  void bound_PreventNullLabels() {
    assertThrows(
        NullPointerException.class,
        () -> meter.longUpDownCounterBuilder("metric").build().bind(null),
        "labels");
  }

  @Test
  void bound_DoesNotThrow() {
    LongUpDownCounter longUpDownCounter =
        meter
            .longUpDownCounterBuilder(NAME)
            .setDescription(DESCRIPTION)
            .setUnit(UNIT)
            .setConstantLabels(CONSTANT_LABELS)
            .build();
    BoundLongUpDownCounter bound = longUpDownCounter.bind(Labels.empty());
    bound.add(1);
    bound.add(-1);
    bound.unbind();
  }
}
