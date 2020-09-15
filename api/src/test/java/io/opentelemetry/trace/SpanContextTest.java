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

package io.opentelemetry.trace;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** Unit tests for {@link SpanContext}. */
class SpanContextTest {
  private static final byte[] firstTraceIdBytes =
      new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 'a'};
  private static final String FIRST_TRACE_ID = TraceId.bytesToHex(firstTraceIdBytes);
  private static final byte[] secondTraceIdBytes =
      new byte[] {0, 0, 0, 0, 0, 0, 0, '0', 0, 0, 0, 0, 0, 0, 0, 0};
  private static final String SECOND_TRACE_ID = TraceId.bytesToHex(secondTraceIdBytes);

  private static final byte[] firstSpanIdBytes = new byte[] {0, 0, 0, 0, 0, 0, 0, 'a'};
  private static final String FIRST_SPAN_ID = SpanId.bytesToHex(firstSpanIdBytes);
  private static final byte[] secondSpanIdBytes = new byte[] {'0', 0, 0, 0, 0, 0, 0, 0};
  private static final String SECOND_SPAN_ID = SpanId.bytesToHex(secondSpanIdBytes);
  private static final TraceState FIRST_TRACE_STATE =
      TraceState.builder().set("foo", "bar").build();
  private static final TraceState SECOND_TRACE_STATE =
      TraceState.builder().set("foo", "baz").build();
  private static final TraceState EMPTY_TRACE_STATE = TraceState.builder().build();
  private static final SpanContext first =
      SpanContext.create(FIRST_TRACE_ID, FIRST_SPAN_ID, TraceFlags.getDefault(), FIRST_TRACE_STATE);
  private static final SpanContext second =
      SpanContext.create(
          SECOND_TRACE_ID, SECOND_SPAN_ID, TraceFlags.getSampled(), SECOND_TRACE_STATE);
  private static final SpanContext remote =
      SpanContext.createFromRemoteParent(
          SECOND_TRACE_ID, SECOND_SPAN_ID, TraceFlags.getSampled(), EMPTY_TRACE_STATE);

  @Test
  void invalidSpanContext() {
    assertThat(SpanContext.getInvalid().getTraceIdAsHexString()).isEqualTo(TraceId.getInvalid());
    assertThat(SpanContext.getInvalid().getSpanIdAsHexString()).isEqualTo(SpanId.getInvalid());
    assertThat(SpanContext.getInvalid().getTraceFlags()).isEqualTo(TraceFlags.getDefault());
  }

  @Test
  void isValid() {
    assertThat(SpanContext.getInvalid().isValid()).isFalse();
    assertThat(
            SpanContext.create(
                    FIRST_TRACE_ID, SpanId.getInvalid(), TraceFlags.getDefault(), EMPTY_TRACE_STATE)
                .isValid())
        .isFalse();
    assertThat(
            SpanContext.create(
                    TraceId.getInvalid(), FIRST_SPAN_ID, TraceFlags.getDefault(), EMPTY_TRACE_STATE)
                .isValid())
        .isFalse();
    assertThat(first.isValid()).isTrue();
    assertThat(second.isValid()).isTrue();
  }

  @Test
  void getTraceId() {
    assertThat(first.getTraceIdAsHexString()).isEqualTo(FIRST_TRACE_ID);
    assertThat(second.getTraceIdAsHexString()).isEqualTo(SECOND_TRACE_ID);
  }

  @Test
  void getSpanId() {
    assertThat(first.getSpanIdAsHexString()).isEqualTo(FIRST_SPAN_ID);
    assertThat(second.getSpanIdAsHexString()).isEqualTo(SECOND_SPAN_ID);
  }

  @Test
  void getTraceFlags() {
    assertThat(first.getTraceFlags()).isEqualTo(TraceFlags.getDefault());
    assertThat(second.getTraceFlags()).isEqualTo(TraceFlags.getSampled());
  }

  @Test
  void getTraceState() {
    assertThat(first.getTraceState()).isEqualTo(FIRST_TRACE_STATE);
    assertThat(second.getTraceState()).isEqualTo(SECOND_TRACE_STATE);
  }

  @Test
  void isRemote() {
    assertThat(first.isRemote()).isFalse();
    assertThat(second.isRemote()).isFalse();
    assertThat(remote.isRemote()).isTrue();
  }
}
