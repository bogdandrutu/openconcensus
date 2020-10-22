/*
 * Copyright 2020, OpenTelemetry Authors
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

package io.opentelemetry.sdk.extension.jfr;

import io.opentelemetry.trace.SpanContext;
import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;

@Name("io.opentelemetry.context.Scope")
@Label("Scope")
@Category("Open Telemetry Tracing")
@Description(
    "Open Telemetry trace event corresponding to the span currently "
        + "in scope/active on this thread.")
class ScopeEvent extends Event {

  @Label("Trace Id")
  private final String traceId;

  @Label("Span Id")
  private final String spanId;

  ScopeEvent(SpanContext spanContext) {
    this.traceId = spanContext.getTraceIdAsHexString();
    this.spanId = spanContext.getSpanIdAsHexString();
  }

  public String getTraceId() {
    return traceId;
  }

  public String getSpanId() {
    return spanId;
  }
}