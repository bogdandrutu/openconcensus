/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.trace;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.Context;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link TracingContextUtils}. */
public final class TracingContextUtilsTest {

  @Test
  void testGetCurrentSpan_Default() {
    Span span = TracingContextUtils.getCurrentSpan();
    assertThat(span).isSameAs(DefaultSpan.getInvalid());
  }

  @Test
  void testGetCurrentSpan_SetSpan() {
    Span span = DefaultSpan.create(SpanContext.getInvalid());
    Context orig = TracingContextUtils.withSpan(span, Context.current()).attach();
    try {
      assertThat(TracingContextUtils.getCurrentSpan()).isSameAs(span);
    } finally {
      Context.current().detach(orig);
    }
  }

  @Test
  void testGetSpan_DefaultContext() {
    Span span = TracingContextUtils.getSpan(Context.current());
    assertThat(span).isSameAs(DefaultSpan.getInvalid());
  }

  @Test
  void testGetSpan_ExplicitContext() {
    Span span = DefaultSpan.create(SpanContext.getInvalid());
    Context context = TracingContextUtils.withSpan(span, Context.current());
    assertThat(TracingContextUtils.getSpan(context)).isSameAs(span);
  }

  @Test
  void testGetSpanWithoutDefault_DefaultContext() {
    Span span = TracingContextUtils.getSpanWithoutDefault(Context.current());
    assertThat(span).isNull();
  }

  @Test
  void testGetSpanWithoutDefault_ExplicitContext() {
    Span span = DefaultSpan.create(SpanContext.getInvalid());
    Context context = TracingContextUtils.withSpan(span, Context.current());
    assertThat(TracingContextUtils.getSpanWithoutDefault(context)).isSameAs(span);
  }
}
