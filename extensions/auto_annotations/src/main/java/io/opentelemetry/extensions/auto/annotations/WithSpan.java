/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.extensions.auto.annotations;

import io.opentelemetry.trace.Span.Kind;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation marks that an execution of this method or constructor should result in a new
 * {@link io.opentelemetry.trace.Span}.
 *
 * <p>Application developers can use this annotation to signal OpenTelemetry auto-instrumentation
 * that a new span should be created whenever marked method is executed.
 *
 * <p>If you are a library developer, then probably you should NOT use this annotation, because it
 * is non-functional without the OpenTelemetry auto-instrumentation agent, or some other annotation
 * processor.
 *
 * @see <a href="https://github.com/open-telemetry/opentelemetry-auto-instr-java">OpenTelemetry
 *     Auto-Instrumentation</a>
 */
@Target({ElementType.METHOD, ElementType.CONSTRUCTOR})
@Retention(RetentionPolicy.RUNTIME)
public @interface WithSpan {
  /**
   * Optional name of the created span.
   *
   * <p>If not specified, an appropriate default name should be created by auto-instrumentation.
   * E.g. {@code "className"."method"}
   */
  String value() default "";

  /** Specify the {@link Kind} of span to be created. Defaults to {@link Kind#INTERNAL}. */
  Kind kind() default Kind.INTERNAL;
}
