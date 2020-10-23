/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.trace;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.junit.jupiter.api.Test;

class TracingContextUtilsTest {
  @Test
  void testGetCurrentSpan_Default() {
    Span span = Span.current();
    assertThat(span).isSameAs(Span.getInvalid());
  }

  @Test
  void testGetCurrentSpan_SetSpan() {
    Span span = Span.wrap(SpanContext.getInvalid());
    try (Scope ignored = Context.current().with(span).makeCurrent()) {
      assertThat(Span.current()).isSameAs(span);
    }
  }

  @Test
  void testGetSpan_DefaultContext() {
    Span span = Span.fromContext(Context.current());
    assertThat(span).isSameAs(Span.getInvalid());
  }

  @Test
  void testGetSpan_ExplicitContext() {
    Span span = Span.wrap(SpanContext.getInvalid());
    Context context = Context.current().with(span);
    assertThat(Span.fromContext(context)).isSameAs(span);
  }

  @Test
  void testGetSpanWithoutDefault_DefaultContext() {
    Span span = Span.fromContextOrNull(Context.current());
    assertThat(span).isNull();
  }

  @Test
  void testGetSpanWithoutDefault_ExplicitContext() {
    Span span = Span.wrap(SpanContext.getInvalid());
    Context context = Context.current().with(span);
    assertThat(Span.fromContextOrNull(context)).isSameAs(span);
  }

  @Test
  void testInProcessContext() {
    Span span = Span.wrap(SpanContext.getInvalid());
    try (Scope scope = TracingContextUtils.currentContextWith(span)) {
      assertThat(Span.current()).isSameAs(span);
      Span secondSpan = Span.wrap(SpanContext.getInvalid());
      try (Scope secondScope = TracingContextUtils.currentContextWith(secondSpan)) {
        assertThat(Span.current()).isSameAs(secondSpan);
      } finally {
        assertThat(Span.current()).isSameAs(span);
      }
    }
    assertThat(Span.current().getSpanContext().isValid()).isFalse();
  }
}
