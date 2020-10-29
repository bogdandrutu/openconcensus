/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.trace.samplers;

import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import io.opentelemetry.api.common.ReadableAttributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.TraceId;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.List;
import javax.annotation.concurrent.Immutable;

/**
 * We assume the lower 64 bits of the traceId's are randomly distributed around the whole (long)
 * range. We convert an incoming probability into an upper bound on that value, such that we can
 * just compare the absolute value of the id and the bound to see if we are within the desired
 * probability range. Using the low bits of the traceId also ensures that systems that only use 64
 * bit ID's will also work with this sampler.
 */
@AutoValue
@Immutable
abstract class TraceIdRatioBasedSampler implements Sampler {

  TraceIdRatioBasedSampler() {}

  static TraceIdRatioBasedSampler create(double ratio) {
    Preconditions.checkArgument(ratio >= 0.0 && ratio <= 1.0, "ratio must be in range [0.0, 1.0]");
    long idUpperBound;
    // Special case the limits, to avoid any possible issues with lack of precision across
    // double/long boundaries. For probability == 0.0, we use Long.MIN_VALUE as this guarantees
    // that we will never sample a trace, even in the case where the id == Long.MIN_VALUE, since
    // Math.Abs(Long.MIN_VALUE) == Long.MIN_VALUE.
    if (ratio == 0.0) {
      idUpperBound = Long.MIN_VALUE;
    } else if (ratio == 1.0) {
      idUpperBound = Long.MAX_VALUE;
    } else {
      idUpperBound = (long) (ratio * Long.MAX_VALUE);
    }
    return new AutoValue_TraceIdRatioBasedSampler(
        ratio,
        idUpperBound,
        ImmutableSamplingResult.createWithProbability(
            SamplingResult.Decision.RECORD_AND_SAMPLE, ratio),
        ImmutableSamplingResult.createWithProbability(SamplingResult.Decision.DROP, ratio));
  }

  abstract double getRatio();

  abstract long getIdUpperBound();

  abstract SamplingResult getPositiveSamplingResult();

  abstract SamplingResult getNegativeSamplingResult();

  @Override
  public final SamplingResult shouldSample(
      Context parentContext,
      String traceId,
      String name,
      Span.Kind spanKind,
      ReadableAttributes attributes,
      List<SpanData.Link> parentLinks) {
    // Always sample if we are within probability range. This is true even for child spans (that
    // may have had a different sampling samplingResult made) to allow for different sampling
    // policies,
    // and dynamic increases to sampling probabilities for debugging purposes.
    // Note use of '<' for comparison. This ensures that we never sample for probability == 0.0,
    // while allowing for a (very) small chance of *not* sampling if the id == Long.MAX_VALUE.
    // This is considered a reasonable tradeoff for the simplicity/performance requirements (this
    // code is executed in-line for every Span creation).
    return Math.abs(TraceId.getTraceIdRandomPart(traceId)) < getIdUpperBound()
        ? getPositiveSamplingResult()
        : getNegativeSamplingResult();
  }

  @Override
  public final String getDescription() {
    return String.format("TraceIdRatioBased{%.6f}", getRatio());
  }

  @Override
  public final String toString() {
    return getDescription();
  }
}
