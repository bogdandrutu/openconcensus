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
    Span span = TracingContextUtils.getCurrentSpan();
    assertThat(span).isSameAs(Span.getInvalid());
  }

  @Test
  void testGetCurrentSpan_SetSpan() {
    Span span = Span.wrap(SpanContext.getInvalid());
    try (Scope ignored = Context.current().with(span).makeCurrent()) {
      assertThat(TracingContextUtils.getCurrentSpan()).isSameAs(span);
    }
  }

  @Test
  void testGetSpan_DefaultContext() {
    Span span = TracingContextUtils.getSpan(Context.current());
    assertThat(span).isSameAs(Span.getInvalid());
  }

  @Test
  void testGetSpan_ExplicitContext() {
    Span span = Span.wrap(SpanContext.getInvalid());
    Context context = Context.current().with(span);
    assertThat(TracingContextUtils.getSpan(context)).isSameAs(span);
  }

  @Test
  void testGetSpanWithoutDefault_DefaultContext() {
    Span span = TracingContextUtils.getSpanWithoutDefault(Context.current());
    assertThat(span).isNull();
  }

  @Test
  void testGetSpanWithoutDefault_ExplicitContext() {
    Span span = Span.wrap(SpanContext.getInvalid());
    Context context = Context.current().with(span);
    assertThat(TracingContextUtils.getSpanWithoutDefault(context)).isSameAs(span);
  }

  @Test
  void testInProcessContext() {
    Span span = Span.wrap(SpanContext.getInvalid());
    try (Scope scope = TracingContextUtils.currentContextWith(span)) {
      assertThat(TracingContextUtils.getCurrentSpan()).isSameAs(span);
      Span secondSpan = Span.wrap(SpanContext.getInvalid());
      try (Scope secondScope = TracingContextUtils.currentContextWith(secondSpan)) {
        assertThat(TracingContextUtils.getCurrentSpan()).isSameAs(secondSpan);
      } finally {
        assertThat(TracingContextUtils.getCurrentSpan()).isSameAs(span);
      }
    }
    assertThat(TracingContextUtils.getCurrentSpan().getContext().isValid()).isFalse();
  }
}
