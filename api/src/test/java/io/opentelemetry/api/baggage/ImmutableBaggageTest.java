/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.api.baggage;

import static io.opentelemetry.api.baggage.BaggageTestUtil.listToBaggage;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.testing.EqualsTester;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link Baggage} and {@link Baggage.Builder}.
 *
 * <p>Tests for scope management with {@link Baggage} are in {@link ScopedBaggageTest}.
 */
class ImmutableBaggageTest {

  private static final EntryMetadata TMD = EntryMetadata.create("tmd");

  private static final String K1 = "k1";
  private static final String K2 = "k2";

  private static final String V1 = "v1";
  private static final String V2 = "v2";

  private static final Entry T1 = Entry.create(K1, V1, TMD);
  private static final Entry T2 = Entry.create(K2, V2, TMD);

  @Test
  void getEntries_empty() {
    Baggage baggage = Baggage.empty();
    assertThat(baggage.getEntries()).isEmpty();
  }

  @Test
  void getEntries_nonEmpty() {
    Baggage baggage = listToBaggage(T1, T2);
    assertThat(baggage.getEntries()).containsExactly(T1, T2);
  }

  @Test
  void getEntries_chain() {
    Entry t1alt = Entry.create(K1, V2, TMD);
    Baggage parent = listToBaggage(T1, T2);
    Context parentContext = Context.root().with(parent);
    Baggage baggage =
        Baggage.builder()
            .setParent(parentContext)
            .put(t1alt.getKey(), t1alt.getValue(), t1alt.getEntryMetadata())
            .build();
    assertThat(baggage.getEntries()).containsExactly(t1alt, T2);
  }

  @Test
  void put_newKey() {
    Baggage parent = listToBaggage(T1);
    Context parentContext = Context.root().with(parent);
    assertThat(Baggage.builder().setParent(parentContext).put(K2, V2, TMD).build().getEntries())
        .containsExactly(T1, T2);
  }

  @Test
  void put_existingKey() {
    Baggage parent = listToBaggage(T1);
    Context parentContext = Context.root().with(parent);
    assertThat(Baggage.builder().setParent(parentContext).put(K1, V2, TMD).build().getEntries())
        .containsExactly(Entry.create(K1, V2, TMD));
  }

  @Test
  void put_nullKey() {
    Baggage parent = listToBaggage(T1);
    Context parentContext = Context.root().with(parent);
    Baggage.Builder builder = Baggage.builder().setParent(parentContext);
    assertThrows(NullPointerException.class, () -> builder.put(null, V2, TMD), "key");
  }

  @Test
  void put_nullValue() {
    Baggage parent = listToBaggage(T1);
    Context parentContext = Context.root().with(parent);
    Baggage.Builder builder = Baggage.builder().setParent(parentContext);
    assertThrows(NullPointerException.class, () -> builder.put(K2, null, TMD), "value");
  }

  @Test
  void setParent_nullContext() {
    assertThrows(NullPointerException.class, () -> Baggage.builder().setParent((Context) null));
  }

  @Test
  void setParent_fromContext() {
    Context context = Context.root().with(listToBaggage(T2));
    Baggage baggage = Baggage.builder().setParent(context).build();
    assertThat(baggage.getEntries()).containsExactly(T2);
  }

  @Test
  void setParent_fromEmptyContext() {
    Context emptyContext = Context.root();
    Baggage parent = listToBaggage(T1);
    try (Scope scope = BaggageUtils.currentContextWith(parent)) {
      Baggage baggage = Baggage.builder().setParent(emptyContext).build();
      assertThat(baggage.getEntries()).isEmpty();
    }
  }

  @Test
  void setParent_setNoParent() {
    Baggage parent = listToBaggage(T1);
    Context parentContext = Context.root().with(parent);
    Baggage baggage = Baggage.builder().setParent(parentContext).setNoParent().build();
    assertThat(baggage.getEntries()).isEmpty();
  }

  @Test
  void remove_existingKey() {
    Baggage.Builder builder = Baggage.builder();
    builder.put(T1.getKey(), T1.getValue(), T1.getEntryMetadata());
    builder.put(T2.getKey(), T2.getValue(), T2.getEntryMetadata());

    assertThat(builder.remove(K1).build().getEntries()).containsExactly(T2);
  }

  @Test
  void remove_differentKey() {
    Baggage.Builder builder = Baggage.builder();
    builder.put(T1.getKey(), T1.getValue(), T1.getEntryMetadata());
    builder.put(T2.getKey(), T2.getValue(), T2.getEntryMetadata());

    assertThat(builder.remove(K2).build().getEntries()).containsExactly(T1);
  }

  @Test
  void remove_keyFromParent() {
    Baggage parent = listToBaggage(T1, T2);
    Context parentContext = Context.root().with(parent);
    assertThat(Baggage.builder().setParent(parentContext).remove(K1).build().getEntries())
        .containsExactly(T2);
  }

  @Test
  void remove_nullKey() {
    Baggage.Builder builder = Baggage.builder();
    assertThrows(NullPointerException.class, () -> builder.remove(null), "key");
  }

  @Test
  void toBuilder_keepsOriginalState() {
    assertThat(Baggage.empty().toBuilder().build()).isEqualTo(Baggage.empty());

    Baggage originalBaggage = Baggage.builder().put("key", "value").build();
    assertThat(originalBaggage.toBuilder().build()).isEqualTo(originalBaggage);

    Baggage parentedBaggage =
        Baggage.builder().setParent(Context.root().with(originalBaggage)).build();
    assertThat(parentedBaggage.toBuilder().build()).isEqualTo(parentedBaggage);
  }

  @Test
  void toBuilder_allowChanges() {
    Baggage singleItemNoParent = Baggage.builder().put("key1", "value1").setNoParent().build();
    Baggage singleItemWithParent =
        Baggage.builder()
            .setParent(Context.root().with(Baggage.empty()))
            .put("key1", "value1")
            .build();

    assertThat(Baggage.empty().toBuilder().put("key1", "value1").build())
        .isEqualTo(singleItemNoParent);
    assertThat(singleItemNoParent.toBuilder().put("key2", "value2").build())
        .isEqualTo(
            Baggage.builder().put("key1", "value1").put("key2", "value2").setNoParent().build());
    assertThat(singleItemNoParent.toBuilder().put("key1", "value2").build())
        .isEqualTo(Baggage.builder().put("key1", "value2").setNoParent().build());

    assertThat(singleItemWithParent.toBuilder().put("key1", "value2").build())
        .isEqualTo(
            Baggage.builder()
                .put("key1", "value2")
                .setParent(Context.root().with(Baggage.empty()))
                .build());
  }

  @Test
  void testEquals() {
    new EqualsTester()
        .addEqualityGroup(
            Baggage.builder().put(K1, V1, TMD).put(K2, V2, TMD).build(),
            Baggage.builder().put(K1, V1, TMD).put(K2, V2, TMD).build(),
            Baggage.builder().put(K2, V2, TMD).put(K1, V1, TMD).build())
        .addEqualityGroup(Baggage.builder().put(K1, V1, TMD).put(K2, V1, TMD).build())
        .addEqualityGroup(Baggage.builder().put(K1, V2, TMD).put(K2, V1, TMD).build())
        .testEquals();
  }
}
