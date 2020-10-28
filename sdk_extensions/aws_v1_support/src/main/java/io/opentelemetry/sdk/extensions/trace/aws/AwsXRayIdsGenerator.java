/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.extensions.trace.aws;

import io.opentelemetry.api.trace.TraceId;
import io.opentelemetry.sdk.trace.IdsGenerator;
import io.opentelemetry.sdk.trace.RandomIdsGenerator;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Generates tracing ids compatible with the AWS X-Ray tracing service. In the X-Ray system the
 * first 32 bits of the trace id are the Unix epoch time in secords. Spans (AWS calls them segments)
 * submit with trace id timestamps outside of the last 30 days are rejected.
 *
 * @see <a
 *     href="https://docs.aws.amazon.com/xray/latest/devguide/xray-api-sendingdata.html#xray-api-traceids">Generating
 *     Trace IDs</a>
 */
public class AwsXRayIdsGenerator implements IdsGenerator {

  private static final RandomIdsGenerator RANDOM_IDS_GENERATOR = new RandomIdsGenerator();

  @Override
  public String generateSpanId() {
    return RANDOM_IDS_GENERATOR.generateSpanId();
  }

  @Override
  public String generateTraceId() {
    // hi - 4 bytes timestamp, 4 bytes random
    // low - 8 bytes random.
    // Since we include timestamp, impossible to be invalid.

    Random random = ThreadLocalRandom.current();
    long timestampSecs = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    long hiRandom = random.nextInt() & 0xFFFFFFFFL;

    long lowRandom = random.nextLong();

    return TraceId.fromLongs(timestampSecs << 32 | hiRandom, lowRandom);
  }
}
