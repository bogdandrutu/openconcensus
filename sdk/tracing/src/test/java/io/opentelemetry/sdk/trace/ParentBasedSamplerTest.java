/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.trace;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.sdk.trace.samplers.SamplingResult;
import java.util.Collections;
import org.junit.jupiter.api.Test;

class ParentBasedSamplerTest {
  private static final String SPAN_NAME = "MySpanName";
  private static final Span.Kind SPAN_KIND = Span.Kind.INTERNAL;
  private final IdsGenerator idsGenerator = IdsGenerator.random();
  private final String traceId = idsGenerator.generateTraceId();
  private final String parentSpanId = idsGenerator.generateSpanId();
  private final TraceState traceState = TraceState.builder().build();
  private final SpanContext sampledSpanContext =
      SpanContext.create(traceId, parentSpanId, TraceFlags.getSampled(), traceState);
  private final Context sampledParentContext = Context.root().with(Span.wrap(sampledSpanContext));
  private final Context notSampledParentContext =
      Context.root()
          .with(
              Span.wrap(
                  SpanContext.create(traceId, parentSpanId, TraceFlags.getDefault(), traceState)));
  private final Context invalidParentContext = Context.root().with(Span.getInvalid());
  private final Context sampledRemoteParentContext =
      Context.root()
          .with(
              Span.wrap(
                  SpanContext.createFromRemoteParent(
                      traceId, parentSpanId, TraceFlags.getSampled(), traceState)));
  private final Context notSampledRemoteParentContext =
      Context.root()
          .with(
              Span.wrap(
                  SpanContext.createFromRemoteParent(
                      traceId, parentSpanId, TraceFlags.getDefault(), traceState)));

  @Test
  void alwaysOn() {
    // Sampled parent.
    assertThat(
            Sampler.parentBased(Sampler.alwaysOn())
                .shouldSample(
                    sampledParentContext,
                    traceId,
                    SPAN_NAME,
                    SPAN_KIND,
                    Attributes.empty(),
                    Collections.emptyList())
                .getDecision())
        .isEqualTo(SamplingResult.Decision.RECORD_AND_SAMPLE);

    // Not sampled parent.
    assertThat(
            Sampler.parentBased(Sampler.alwaysOn())
                .shouldSample(
                    notSampledParentContext,
                    traceId,
                    SPAN_NAME,
                    SPAN_KIND,
                    Attributes.empty(),
                    Collections.emptyList())
                .getDecision())
        .isEqualTo(SamplingResult.Decision.DROP);
  }

  @Test
  void alwaysOff() {
    // Sampled parent.
    assertThat(
            Sampler.parentBased(Sampler.alwaysOff())
                .shouldSample(
                    sampledParentContext,
                    traceId,
                    SPAN_NAME,
                    SPAN_KIND,
                    Attributes.empty(),
                    Collections.emptyList())
                .getDecision())
        .isEqualTo(SamplingResult.Decision.RECORD_AND_SAMPLE);

    // Not sampled parent.
    assertThat(
            Sampler.parentBased(Sampler.alwaysOff())
                .shouldSample(
                    notSampledParentContext,
                    traceId,
                    SPAN_NAME,
                    SPAN_KIND,
                    Attributes.empty(),
                    Collections.emptyList())
                .getDecision())
        .isEqualTo(SamplingResult.Decision.DROP);
  }

  @Test
  void notSampled_remoteParent() {
    assertThat(
            Sampler.parentBasedBuilder(Sampler.alwaysOff())
                .setRemoteParentNotSampled(Sampler.alwaysOn())
                .build()
                .shouldSample(
                    notSampledRemoteParentContext,
                    traceId,
                    SPAN_NAME,
                    SPAN_KIND,
                    Attributes.empty(),
                    Collections.emptyList())
                .getDecision())
        .isEqualTo(SamplingResult.Decision.RECORD_AND_SAMPLE);

    assertThat(
            Sampler.parentBasedBuilder(Sampler.alwaysOff())
                .setRemoteParentNotSampled(Sampler.alwaysOff())
                .build()
                .shouldSample(
                    notSampledRemoteParentContext,
                    traceId,
                    SPAN_NAME,
                    SPAN_KIND,
                    Attributes.empty(),
                    Collections.emptyList())
                .getDecision())
        .isEqualTo(SamplingResult.Decision.DROP);

    assertThat(
            Sampler.parentBasedBuilder(Sampler.alwaysOn())
                .setRemoteParentNotSampled(Sampler.alwaysOff())
                .build()
                .shouldSample(
                    notSampledRemoteParentContext,
                    traceId,
                    SPAN_NAME,
                    SPAN_KIND,
                    Attributes.empty(),
                    Collections.emptyList())
                .getDecision())
        .isEqualTo(SamplingResult.Decision.DROP);

    assertThat(
            Sampler.parentBasedBuilder(Sampler.alwaysOn())
                .setRemoteParentNotSampled(Sampler.alwaysOn())
                .build()
                .shouldSample(
                    notSampledRemoteParentContext,
                    traceId,
                    SPAN_NAME,
                    SPAN_KIND,
                    Attributes.empty(),
                    Collections.emptyList())
                .getDecision())
        .isEqualTo(SamplingResult.Decision.RECORD_AND_SAMPLE);
  }

  @Test
  void parentBasedSampler_NotSampled_NotRemote_Parent() {

    assertThat(
            Sampler.parentBasedBuilder(Sampler.alwaysOff())
                .setLocalParentNotSampled(Sampler.alwaysOn())
                .build()
                .shouldSample(
                    notSampledParentContext,
                    traceId,
                    SPAN_NAME,
                    SPAN_KIND,
                    Attributes.empty(),
                    Collections.emptyList())
                .getDecision())
        .isEqualTo(SamplingResult.Decision.RECORD_AND_SAMPLE);

    assertThat(
            Sampler.parentBasedBuilder(Sampler.alwaysOff())
                .setLocalParentNotSampled(Sampler.alwaysOff())
                .build()
                .shouldSample(
                    notSampledParentContext,
                    traceId,
                    SPAN_NAME,
                    SPAN_KIND,
                    Attributes.empty(),
                    Collections.emptyList())
                .getDecision())
        .isEqualTo(SamplingResult.Decision.DROP);

    assertThat(
            Sampler.parentBasedBuilder(Sampler.alwaysOn())
                .setLocalParentNotSampled(Sampler.alwaysOff())
                .build()
                .shouldSample(
                    notSampledParentContext,
                    traceId,
                    SPAN_NAME,
                    SPAN_KIND,
                    Attributes.empty(),
                    Collections.emptyList())
                .getDecision())
        .isEqualTo(SamplingResult.Decision.DROP);

    assertThat(
            Sampler.parentBasedBuilder(Sampler.alwaysOn())
                .setLocalParentNotSampled(Sampler.alwaysOn())
                .build()
                .shouldSample(
                    notSampledParentContext,
                    traceId,
                    SPAN_NAME,
                    SPAN_KIND,
                    Attributes.empty(),
                    Collections.emptyList())
                .getDecision())
        .isEqualTo(SamplingResult.Decision.RECORD_AND_SAMPLE);
  }

  @Test
  void sampled_remoteParent() {
    assertThat(
            Sampler.parentBasedBuilder(Sampler.alwaysOff())
                .setRemoteParentSampled(Sampler.alwaysOff())
                .build()
                .shouldSample(
                    sampledRemoteParentContext,
                    traceId,
                    SPAN_NAME,
                    SPAN_KIND,
                    Attributes.empty(),
                    Collections.emptyList())
                .getDecision())
        .isEqualTo(SamplingResult.Decision.DROP);

    assertThat(
            Sampler.parentBasedBuilder(Sampler.alwaysOff())
                .setRemoteParentSampled(Sampler.alwaysOn())
                .build()
                .shouldSample(
                    sampledRemoteParentContext,
                    traceId,
                    SPAN_NAME,
                    SPAN_KIND,
                    Attributes.empty(),
                    Collections.emptyList())
                .getDecision())
        .isEqualTo(SamplingResult.Decision.RECORD_AND_SAMPLE);

    assertThat(
            Sampler.parentBasedBuilder(Sampler.alwaysOn())
                .setRemoteParentSampled(Sampler.alwaysOn())
                .build()
                .shouldSample(
                    sampledRemoteParentContext,
                    traceId,
                    SPAN_NAME,
                    SPAN_KIND,
                    Attributes.empty(),
                    Collections.emptyList())
                .getDecision())
        .isEqualTo(SamplingResult.Decision.RECORD_AND_SAMPLE);

    assertThat(
            Sampler.parentBasedBuilder(Sampler.alwaysOn())
                .setRemoteParentSampled(Sampler.alwaysOff())
                .build()
                .shouldSample(
                    sampledRemoteParentContext,
                    traceId,
                    SPAN_NAME,
                    SPAN_KIND,
                    Attributes.empty(),
                    Collections.emptyList())
                .getDecision())
        .isEqualTo(SamplingResult.Decision.DROP);
  }

  @Test
  void parentBasedSampler_Sampled_NotRemote_Parent() {
    assertThat(
            Sampler.parentBasedBuilder(Sampler.alwaysOff())
                .setLocalParentSampled(Sampler.alwaysOn())
                .build()
                .shouldSample(
                    sampledParentContext,
                    traceId,
                    SPAN_NAME,
                    SPAN_KIND,
                    Attributes.empty(),
                    Collections.emptyList())
                .getDecision())
        .isEqualTo(SamplingResult.Decision.RECORD_AND_SAMPLE);

    assertThat(
            Sampler.parentBasedBuilder(Sampler.alwaysOff())
                .setLocalParentSampled(Sampler.alwaysOff())
                .build()
                .shouldSample(
                    sampledParentContext,
                    traceId,
                    SPAN_NAME,
                    SPAN_KIND,
                    Attributes.empty(),
                    Collections.emptyList())
                .getDecision())
        .isEqualTo(SamplingResult.Decision.DROP);

    assertThat(
            Sampler.parentBasedBuilder(Sampler.alwaysOn())
                .setLocalParentSampled(Sampler.alwaysOff())
                .build()
                .shouldSample(
                    sampledParentContext,
                    traceId,
                    SPAN_NAME,
                    SPAN_KIND,
                    Attributes.empty(),
                    Collections.emptyList())
                .getDecision())
        .isEqualTo(SamplingResult.Decision.DROP);

    assertThat(
            Sampler.parentBasedBuilder(Sampler.alwaysOn())
                .setLocalParentSampled(Sampler.alwaysOn())
                .build()
                .shouldSample(
                    sampledParentContext,
                    traceId,
                    SPAN_NAME,
                    SPAN_KIND,
                    Attributes.empty(),
                    Collections.emptyList())
                .getDecision())
        .isEqualTo(SamplingResult.Decision.RECORD_AND_SAMPLE);
  }

  @Test
  void invalidParent() {
    assertThat(
            Sampler.parentBased(Sampler.alwaysOff())
                .shouldSample(
                    invalidParentContext,
                    traceId,
                    SPAN_NAME,
                    SPAN_KIND,
                    Attributes.empty(),
                    Collections.emptyList())
                .getDecision())
        .isEqualTo(SamplingResult.Decision.DROP);

    assertThat(
            Sampler.parentBased(Sampler.alwaysOff())
                .shouldSample(
                    invalidParentContext,
                    traceId,
                    SPAN_NAME,
                    SPAN_KIND,
                    Attributes.empty(),
                    Collections.emptyList())
                .getDecision())
        .isEqualTo(SamplingResult.Decision.DROP);

    assertThat(
            Sampler.parentBasedBuilder(Sampler.alwaysOff())
                .setRemoteParentSampled(Sampler.alwaysOn())
                .setRemoteParentNotSampled(Sampler.alwaysOn())
                .setLocalParentSampled(Sampler.alwaysOn())
                .setLocalParentNotSampled(Sampler.alwaysOn())
                .build()
                .shouldSample(
                    invalidParentContext,
                    traceId,
                    SPAN_NAME,
                    SPAN_KIND,
                    Attributes.empty(),
                    Collections.emptyList())
                .getDecision())
        .isEqualTo(SamplingResult.Decision.DROP);

    assertThat(
            Sampler.parentBased(Sampler.alwaysOn())
                .shouldSample(
                    invalidParentContext,
                    traceId,
                    SPAN_NAME,
                    SPAN_KIND,
                    Attributes.empty(),
                    Collections.emptyList())
                .getDecision())
        .isEqualTo(SamplingResult.Decision.RECORD_AND_SAMPLE);
  }

  @Test
  void getDescription() {
    assertThat(Sampler.parentBased(Sampler.alwaysOn()).getDescription())
        .isEqualTo(
            "ParentBased{root:AlwaysOnSampler,remoteParentSampled:AlwaysOnSampler,"
                + "remoteParentNotSampled:AlwaysOffSampler,localParentSampled:AlwaysOnSampler,"
                + "localParentNotSampled:AlwaysOffSampler}");
  }
}
