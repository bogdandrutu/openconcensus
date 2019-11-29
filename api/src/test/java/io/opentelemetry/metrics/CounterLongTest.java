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

import io.opentelemetry.OpenTelemetry;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link CounterLong}. */
@RunWith(JUnit4.class)
public class CounterLongTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  private static final String NAME = "name";
  private static final String DESCRIPTION = "description";
  private static final String UNIT = "1";
  private static final List<String> LABEL_KEY = Collections.singletonList("key");

  private final Meter meter = OpenTelemetry.getMeterFactory().get("counter_long_test");

  @Test
  public void preventNonPrintableName() {
    thrown.expect(IllegalArgumentException.class);
    meter.counterLongBuilder("\2").build();
  }

  @Test
  public void preventTooLongName() {
    char[] chars = new char[DefaultMeter.NAME_MAX_LENGTH + 1];
    Arrays.fill(chars, 'a');
    String longName = String.valueOf(chars);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(DefaultMeter.ERROR_MESSAGE_INVALID_NAME);
    meter.counterLongBuilder(longName).build();
  }

  @Test
  public void preventNull_Description() {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("description");
    meter.counterLongBuilder("metric").setDescription(null).build();
  }

  @Test
  public void preventNull_Unit() {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("unit");
    meter.counterLongBuilder("metric").setUnit(null).build();
  }

  @Test
  public void preventNull_LabelKeys() {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("labelKeys");
    meter.counterLongBuilder("metric").setLabelKeys(null).build();
  }

  @Test
  public void preventNull_LabelKey() {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("labelKey");
    meter
        .counterLongBuilder("metric")
        .setLabelKeys(Collections.<String>singletonList(null))
        .build();
  }

  @Test
  public void preventNull_ConstantLabels() {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("constantLabels");
    meter.counterLongBuilder("metric").setConstantLabels(null).build();
  }

  @Test
  public void noopGetHandle_WithNullLabelSet() {
    CounterLong counterLong =
        meter
            .counterLongBuilder(NAME)
            .setDescription(DESCRIPTION)
            .setLabelKeys(LABEL_KEY)
            .setUnit(UNIT)
            .build();
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("labelSet");
    counterLong.getHandle(null);
  }

  @Test
  public void noopRemoveHandle_WithNullHandle() {
    CounterLong counterLong =
        meter
            .counterLongBuilder(NAME)
            .setDescription(DESCRIPTION)
            .setLabelKeys(LABEL_KEY)
            .setUnit(UNIT)
            .build();
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("handle");
    counterLong.removeHandle(null);
  }

  @Test
  public void doesNotThrow() {
    CounterLong counterLong =
        meter
            .counterLongBuilder(NAME)
            .setDescription(DESCRIPTION)
            .setLabelKeys(LABEL_KEY)
            .setUnit(UNIT)
            .build();
    counterLong.getDefaultHandle().add(1);
  }
}
