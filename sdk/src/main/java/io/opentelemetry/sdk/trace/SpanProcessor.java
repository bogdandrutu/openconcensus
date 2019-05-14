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

package io.opentelemetry.sdk.trace;

import io.opentelemetry.trace.Span;

/**
 * SpanProcessor is the interface {@code TracerSdk} uses to allow synchronous hooks for when a
 * {@code Span} is started or when a {@code Span} is ended.
 */
public interface SpanProcessor {
  /**
   * Called when a {@link io.opentelemetry.trace.Span} is started, if the {@link
   * Span#isRecordingEvents()} returns true.
   *
   * <p>This method is called synchronously on the execution thread, should not throw or block the
   * execution thread.
   *
   * @param span the {@code SpanSdk} that just started.
   */
  void onStartSync(SpanSdk span);

  /**
   * Called when a {@link io.opentelemetry.trace.Span} is ended, if the {@link
   * Span#isRecordingEvents()} returns true.
   *
   * <p>This method is called synchronously on the execution thread, should not throw or block the
   * execution thread.
   *
   * @param span the {@code SpanSdk} that just ended.
   */
  void onEndSync(SpanSdk span);

  /** Called when {@link TracerSdk#shutdown()} is called. */
  void shutdown();
}
