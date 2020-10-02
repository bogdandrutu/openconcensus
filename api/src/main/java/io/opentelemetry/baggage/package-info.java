/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * API for associating entries with scoped operations.
 *
 * <p>This package manages a set of entries in the {@code io.grpc.Context}. The entries can be used
 * to label anything that is associated with a specific operation. For example, the {@code
 * opentelemetry.stats} package labels all stats with the current entries.
 *
 * <p>{@link io.opentelemetry.baggage.Entry Entrys} are key-value pairs of {@link
 * java.lang.String}s. They are stored as a map in a {@link io.opentelemetry.baggage.Baggage}.
 *
 * <p>Note that entries are independent of the tracing data that is propagated in the {@code
 * io.grpc.Context}, such as trace ID.
 */
// TODO: Add code examples.
package io.opentelemetry.baggage;
