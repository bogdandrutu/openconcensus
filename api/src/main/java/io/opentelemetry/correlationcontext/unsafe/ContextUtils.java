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

package io.opentelemetry.correlationcontext.unsafe;

import io.grpc.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.correlationcontext.CorrelationContext;
import io.opentelemetry.correlationcontext.EmptyCorrelationContext;
import javax.annotation.concurrent.Immutable;

/**
 * Utility methods for accessing the {@link CorrelationContext} contained in the {@link
 * io.grpc.Context}.
 *
 * <p>Most code should interact with the current context via the public APIs in {@link
 * CorrelationContext} and avoid accessing this class directly.
 *
 * @since 0.1.0
 */
@Immutable
public final class ContextUtils {
  private static final Context.Key<CorrelationContext> DIST_CONTEXT_KEY =
      Context.keyWithDefault(
          "opentelemetry-dist-context-key", EmptyCorrelationContext.getInstance());

  /**
   * Creates a new {@code Context} with the given value set.
   *
   * @param distContext the value to be set.
   * @return a new context with the given value set.
   * @since 0.1.0
   */
  public static Context withValue(CorrelationContext distContext) {
    return Context.current().withValue(DIST_CONTEXT_KEY, distContext);
  }

  /**
   * Creates a new {@code Context} with the given value set.
   *
   * @param distContext the value to be set.
   * @param context the parent {@code Context}.
   * @return a new context with the given value set.
   * @since 0.1.0
   */
  public static Context withValue(CorrelationContext distContext, Context context) {
    return context.withValue(DIST_CONTEXT_KEY, distContext);
  }

  /**
   * Returns the value from the current {@code Context}.
   *
   * @return the value from the specified {@code Context}.
   * @since 0.1.0
   */
  public static CorrelationContext getValue() {
    return DIST_CONTEXT_KEY.get();
  }

  /**
   * Returns the value from the specified {@code Context}.
   *
   * @param context the specified {@code Context}.
   * @return the value from the specified {@code Context}.
   * @since 0.1.0
   */
  public static CorrelationContext getValue(Context context) {
    return DIST_CONTEXT_KEY.get(context);
  }

  /**
   * Returns a new {@link Scope} encapsulating the provided {@code CorrelationContext} added to the
   * current {@code Context}.
   *
   * @param distContext the {@code CorrelationContext} to be added to the current {@code Context}.
   * @return the {@link Scope} for the updated {@code Context}.
   * @since 0.1.0
   */
  public static Scope withCorrelationContext(CorrelationContext distContext) {
    return CorrelationContextInScope.create(distContext);
  }

  private ContextUtils() {}
}
