/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extensions.trace.testbed.concurrentcommonrequesthandler;

import io.opentelemetry.sdk.extensions.trace.testbed.TestUtils;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

final class Client {
  private final ExecutorService executor = Executors.newCachedThreadPool();

  private final RequestHandler requestHandler;

  public Client(RequestHandler requestHandler) {
    this.requestHandler = requestHandler;
  }

  public Future<String> send(final Object message) {
    final Context context = new Context();
    return executor.submit(
        () -> {
          TestUtils.sleep();
          executor
              .submit(
                  () -> {
                    TestUtils.sleep();
                    requestHandler.beforeRequest(message, context);
                  })
              .get();

          executor
              .submit(
                  () -> {
                    TestUtils.sleep();
                    requestHandler.afterResponse(message, context);
                  })
              .get();

          return message + ":response";
        });
  }
}
