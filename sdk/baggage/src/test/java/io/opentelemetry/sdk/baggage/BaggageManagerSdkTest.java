/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.baggage;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.Context;
import io.opentelemetry.baggage.Baggage;
import io.opentelemetry.baggage.BaggageUtils;
import io.opentelemetry.baggage.EmptyBaggage;
import io.opentelemetry.context.Scope;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Unit tests for {@link BaggageManagerSdk}. */
// Need to suppress warnings for MustBeClosed because Android 14 does not support
// try-with-resources.
@SuppressWarnings("MustBeClosedChecker")
class BaggageManagerSdkTest {
  @Mock private Baggage distContext;
  private final BaggageManagerSdk contextManager = new BaggageManagerSdk();

  @BeforeEach
  void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  void testGetCurrentContext_DefaultContext() {
    assertThat(contextManager.getCurrentBaggage()).isSameAs(EmptyBaggage.getInstance());
  }

  @Test
  void testGetCurrentContext_ContextSetToNull() {
    Context orig = BaggageUtils.withBaggage(null, Context.current()).attach();
    try {
      Baggage distContext = contextManager.getCurrentBaggage();
      assertThat(distContext).isNotNull();
      assertThat(distContext.getEntries()).isEmpty();
    } finally {
      Context.current().detach(orig);
    }
  }

  @Test
  void testWithBaggage() {
    assertThat(contextManager.getCurrentBaggage()).isSameAs(EmptyBaggage.getInstance());
    try (Scope wtm = contextManager.withContext(distContext)) {
      assertThat(contextManager.getCurrentBaggage()).isSameAs(distContext);
    }
    assertThat(contextManager.getCurrentBaggage()).isSameAs(EmptyBaggage.getInstance());
  }

  @Test
  void testWithBaggageUsingWrap() {
    Runnable runnable;
    try (Scope wtm = contextManager.withContext(distContext)) {
      assertThat(contextManager.getCurrentBaggage()).isSameAs(distContext);
      runnable =
          Context.current()
              .wrap(
                  () -> {
                    assertThat(contextManager.getCurrentBaggage()).isSameAs(distContext);
                  });
    }
    assertThat(contextManager.getCurrentBaggage()).isSameAs(EmptyBaggage.getInstance());
    // When we run the runnable we will have the Baggage in the current Context.
    runnable.run();
  }
}
