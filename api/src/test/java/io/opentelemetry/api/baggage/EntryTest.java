/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.api.baggage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.testing.EqualsTester;
import org.junit.jupiter.api.Test;

class EntryTest {

  private static final String KEY = "KEY";
  private static final String KEY_2 = "KEY2";
  private static final String VALUE = "VALUE";
  private static final String VALUE_2 = "VALUE2";
  private static final EntryMetadata SAMPLE_METADATA =
      EntryMetadata.create("propagation=unlimited");

  @Test
  void testGetKey() {
    assertThat(Entry.create(KEY, VALUE, SAMPLE_METADATA).getKey()).isEqualTo(KEY);
  }

  @Test
  void testGetEntryMetadata() {
    assertThat(Entry.create(KEY, VALUE, SAMPLE_METADATA).getEntryMetadata())
        .isEqualTo(SAMPLE_METADATA);
  }

  @Test
  void testEntryEquals() {
    new EqualsTester()
        .addEqualityGroup(
            Entry.create(KEY, VALUE, SAMPLE_METADATA), Entry.create(KEY, VALUE, SAMPLE_METADATA))
        .addEqualityGroup(Entry.create(KEY, VALUE_2, SAMPLE_METADATA))
        .addEqualityGroup(Entry.create(KEY_2, VALUE, SAMPLE_METADATA))
        .addEqualityGroup(Entry.create(KEY, VALUE, EntryMetadata.create("other")))
        .testEquals();
  }

  @Test
  void create_DisallowKeyUnprintableChars() {
    assertThrows(
        IllegalArgumentException.class, () -> Entry.create("\2ab\3cd", "value", SAMPLE_METADATA));
  }

  @Test
  void createString_DisallowKeyEmpty() {
    assertThrows(IllegalArgumentException.class, () -> Entry.create("", "value", SAMPLE_METADATA));
  }

  @Test
  void disallowEntryValueWithUnprintableChars() {
    String value = "\2ab\3cd";
    assertThrows(IllegalArgumentException.class, () -> Entry.create("key", value, SAMPLE_METADATA));
  }
}
