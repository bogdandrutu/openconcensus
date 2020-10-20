/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extensions.trace.testbed.concurrentcommonrequesthandler;

import io.opentelemetry.context.Context;
import io.opentelemetry.trace.Span;
import io.opentelemetry.trace.Span.Kind;
import io.opentelemetry.trace.Tracer;

/**
 * One instance per Client. Executed concurrently for all requests of one client. 'beforeRequest'
 * and 'afterResponse' are executed in different threads for one 'send'
 */
final class RequestHandler {
  static final String OPERATION_NAME = "send";

  private final Tracer tracer;

  private final Context parentContext;

  public RequestHandler(Tracer tracer) {
    this(tracer, null);
  }

  public RequestHandler(Tracer tracer, Context parentContext) {
    this.tracer = tracer;
    this.parentContext = parentContext;
  }

  public void beforeRequest(Object request, RequestHandlerContext requestHandlerContext) {
    // we cannot use active span because we don't know in which thread it is executed
    // and we cannot therefore activate span. thread can come from common thread pool.
    Span.Builder spanBuilder =
        tracer.spanBuilder(OPERATION_NAME).setNoParent().setSpanKind(Kind.CLIENT);

    if (parentContext != null) {
      spanBuilder.setParent(parentContext);
    }

    requestHandlerContext.put("span", spanBuilder.startSpan());
  }

  public void afterResponse(Object response, RequestHandlerContext requestHandlerContext) {
    Object spanObject = requestHandlerContext.get("span");
    if (spanObject instanceof Span) {
      Span span = (Span) spanObject;
      span.end();
    }
  }
}
