/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.baggage;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.context.Scope;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the methods in {@link BaggageUtils} and {@link Baggage} that interact with the
 * current {@link Baggage}.
 */
class ScopedBaggageTest {

  private static final String KEY_1 = "key 1";
  private static final String KEY_2 = "key 2";
  private static final String KEY_3 = "key 3";

  private static final String VALUE_1 = "value 1";
  private static final String VALUE_2 = "value 2";
  private static final String VALUE_3 = "value 3";
  private static final String VALUE_4 = "value 4";

  private static final EntryMetadata METADATA_UNLIMITED_PROPAGATION =
      EntryMetadata.create("unlimited");
  private static final EntryMetadata METADATA_NO_PROPAGATION = EntryMetadata.create("noprop");

  @Test
  void emptyBaggage() {
    Baggage defaultBaggage = BaggageUtils.getCurrentBaggage();
    assertThat(defaultBaggage.getEntries()).isEmpty();
  }

  @Test
  void withContext() {
    assertThat(BaggageUtils.getCurrentBaggage().getEntries()).isEmpty();
    Baggage scopedEntries =
        Baggage.builder().put(KEY_1, VALUE_1, METADATA_UNLIMITED_PROPAGATION).build();
    try (Scope scope = BaggageUtils.currentContextWith(scopedEntries)) {
      assertThat(BaggageUtils.getCurrentBaggage()).isSameAs(scopedEntries);
    }
    assertThat(BaggageUtils.getCurrentBaggage().getEntries()).isEmpty();
  }

  @Test
  void createBuilderFromCurrentEntries() {
    Baggage scopedBaggage =
        Baggage.builder().put(KEY_1, VALUE_1, METADATA_UNLIMITED_PROPAGATION).build();
    try (Scope scope = BaggageUtils.currentContextWith(scopedBaggage)) {
      Baggage newEntries =
          Baggage.builder().put(KEY_2, VALUE_2, METADATA_UNLIMITED_PROPAGATION).build();
      assertThat(newEntries.getEntries())
          .containsExactlyInAnyOrder(
              Entry.create(KEY_1, VALUE_1, METADATA_UNLIMITED_PROPAGATION),
              Entry.create(KEY_2, VALUE_2, METADATA_UNLIMITED_PROPAGATION));
      assertThat(BaggageUtils.getCurrentBaggage()).isSameAs(scopedBaggage);
    }
  }

  @Test
  void setCurrentEntriesWithBuilder() {
    assertThat(BaggageUtils.getCurrentBaggage().getEntries()).isEmpty();
    Baggage scopedBaggage =
        Baggage.builder().put(KEY_1, VALUE_1, METADATA_UNLIMITED_PROPAGATION).build();
    try (Scope scope = BaggageUtils.currentContextWith(scopedBaggage)) {
      assertThat(BaggageUtils.getCurrentBaggage().getEntries())
          .containsExactly(Entry.create(KEY_1, VALUE_1, METADATA_UNLIMITED_PROPAGATION));
      assertThat(BaggageUtils.getCurrentBaggage()).isSameAs(scopedBaggage);
    }
    assertThat(BaggageUtils.getCurrentBaggage().getEntries()).isEmpty();
  }

  @Test
  void addToCurrentEntriesWithBuilder() {
    Baggage scopedBaggage =
        Baggage.builder().put(KEY_1, VALUE_1, METADATA_UNLIMITED_PROPAGATION).build();
    try (Scope scope1 = BaggageUtils.currentContextWith(scopedBaggage)) {
      Baggage innerBaggage =
          Baggage.builder().put(KEY_2, VALUE_2, METADATA_UNLIMITED_PROPAGATION).build();
      try (Scope scope2 = BaggageUtils.currentContextWith(innerBaggage)) {
        assertThat(BaggageUtils.getCurrentBaggage().getEntries())
            .containsExactlyInAnyOrder(
                Entry.create(KEY_1, VALUE_1, METADATA_UNLIMITED_PROPAGATION),
                Entry.create(KEY_2, VALUE_2, METADATA_UNLIMITED_PROPAGATION));
        assertThat(BaggageUtils.getCurrentBaggage()).isSameAs(innerBaggage);
      }
      assertThat(BaggageUtils.getCurrentBaggage()).isSameAs(scopedBaggage);
    }
  }

  @Test
  void multiScopeBaggageWithMetadata() {
    Baggage scopedBaggage =
        Baggage.builder()
            .put(KEY_1, VALUE_1, METADATA_UNLIMITED_PROPAGATION)
            .put(KEY_2, VALUE_2, METADATA_UNLIMITED_PROPAGATION)
            .build();
    try (Scope scope1 = BaggageUtils.currentContextWith(scopedBaggage)) {
      Baggage innerBaggage =
          Baggage.builder()
              .put(KEY_3, VALUE_3, METADATA_NO_PROPAGATION)
              .put(KEY_2, VALUE_4, METADATA_NO_PROPAGATION)
              .build();
      try (Scope scope2 = BaggageUtils.currentContextWith(innerBaggage)) {
        assertThat(BaggageUtils.getCurrentBaggage().getEntries())
            .containsExactlyInAnyOrder(
                Entry.create(KEY_1, VALUE_1, METADATA_UNLIMITED_PROPAGATION),
                Entry.create(KEY_2, VALUE_4, METADATA_NO_PROPAGATION),
                Entry.create(KEY_3, VALUE_3, METADATA_NO_PROPAGATION));
        assertThat(BaggageUtils.getCurrentBaggage()).isSameAs(innerBaggage);
      }
      assertThat(BaggageUtils.getCurrentBaggage()).isSameAs(scopedBaggage);
    }
  }

  @Test
  void setNoParent_doesNotInheritContext() {
    assertThat(BaggageUtils.getCurrentBaggage().getEntries()).isEmpty();
    Baggage scopedBaggage =
        Baggage.builder().put(KEY_1, VALUE_1, METADATA_UNLIMITED_PROPAGATION).build();
    try (Scope scope = BaggageUtils.currentContextWith(scopedBaggage)) {
      Baggage innerBaggage =
          Baggage.builder()
              .setNoParent()
              .put(KEY_2, VALUE_2, METADATA_UNLIMITED_PROPAGATION)
              .build();
      assertThat(innerBaggage.getEntries())
          .containsExactly(Entry.create(KEY_2, VALUE_2, METADATA_UNLIMITED_PROPAGATION));
    }
    assertThat(BaggageUtils.getCurrentBaggage().getEntries()).isEmpty();
  }
}
