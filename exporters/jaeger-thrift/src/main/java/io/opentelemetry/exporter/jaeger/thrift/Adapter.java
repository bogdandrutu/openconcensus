/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.exporter.jaeger.thrift;

import static io.opentelemetry.api.common.AttributeKey.booleanKey;

import com.google.gson.Gson;
import io.jaegertracing.thriftjava.Log;
import io.jaegertracing.thriftjava.Span;
import io.jaegertracing.thriftjava.SpanRef;
import io.jaegertracing.thriftjava.SpanRefType;
import io.jaegertracing.thriftjava.Tag;
import io.jaegertracing.thriftjava.TagType;
import io.opentelemetry.api.common.AttributeConsumer;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.ReadableAttributes;
import io.opentelemetry.api.trace.SpanId;
import io.opentelemetry.api.trace.TraceId;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.data.SpanData.Event;
import io.opentelemetry.sdk.trace.data.SpanData.Link;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

/** Adapts OpenTelemetry objects to Jaeger objects. */
@ThreadSafe
final class Adapter {

  static final AttributeKey<Boolean> KEY_ERROR = booleanKey("error");
  static final String KEY_LOG_EVENT = "event";
  static final String KEY_EVENT_DROPPED_ATTRIBUTES_COUNT = "otel.event.dropped_attributes_count";
  static final String KEY_SPAN_KIND = "span.kind";
  static final String KEY_SPAN_STATUS_MESSAGE = "span.status.message";
  static final String KEY_SPAN_STATUS_CODE = "span.status.code";
  static final String KEY_INSTRUMENTATION_LIBRARY_NAME = "otel.library.name";
  static final String KEY_INSTRUMENTATION_LIBRARY_VERSION = "otel.library.version";
  public static final Gson GSON = new Gson();

  private Adapter() {}

  /**
   * Converts a list of {@link SpanData} into a collection of Jaeger's {@link Span}.
   *
   * @param spans the list of spans to be converted
   * @return the collection of Jaeger spans
   * @see #toJaeger(SpanData)
   */
  static List<Span> toJaeger(Collection<SpanData> spans) {
    return spans.stream().map(Adapter::toJaeger).collect(Collectors.toList());
  }

  /**
   * Converts a single {@link SpanData} into a Jaeger's {@link Span}.
   *
   * @param span the span to be converted
   * @return the Jaeger span
   */
  static Span toJaeger(SpanData span) {
    Span target = new Span();

    long traceIdHigh = TraceId.traceIdHighBytesAsLong(span.getTraceId());
    long traceIdLow = TraceId.traceIdLowBytesAsLong(span.getTraceId());
    long spanIdAsLong = SpanId.asLong(span.getSpanId());

    target.setTraceIdHigh(traceIdHigh);
    target.setTraceIdLow(traceIdLow);
    target.setSpanId(spanIdAsLong);
    target.setOperationName(span.getName());
    target.setStartTime(TimeUnit.NANOSECONDS.toMicros(span.getStartEpochNanos()));
    target.setDuration(
        TimeUnit.NANOSECONDS.toMicros(span.getEndEpochNanos() - span.getStartEpochNanos()));

    List<Tag> tags = toTags(span.getAttributes());

    target.setLogs(toJaegerLogs(span.getEvents()));
    List<SpanRef> references = toSpanRefs(span.getLinks());

    // add the parent span
    if (SpanId.isValid(span.getParentSpanId())) {
      references.add(
          new SpanRef(
              SpanRefType.CHILD_OF,
              traceIdLow,
              traceIdHigh,
              SpanId.asLong(span.getParentSpanId())));
    }
    target.setReferences(references);

    tags.add(
        new Tag(KEY_SPAN_KIND, TagType.STRING)
            .setVStr(span.getKind().name().toLowerCase(Locale.ROOT)));

    if (span.getStatus().getDescription() != null) {
      tags.add(
          new Tag(KEY_SPAN_STATUS_MESSAGE, TagType.STRING)
              .setVStr(span.getStatus().getDescription()));
    }

    tags.add(
        new Tag(KEY_SPAN_STATUS_CODE, TagType.LONG)
            .setVLong(span.getStatus().getCanonicalCode().value()));

    tags.add(
        new Tag(KEY_INSTRUMENTATION_LIBRARY_NAME, TagType.STRING)
            .setVStr(span.getInstrumentationLibraryInfo().getName()));

    if (span.getInstrumentationLibraryInfo().getVersion() != null) {
      tags.add(
          new Tag(KEY_INSTRUMENTATION_LIBRARY_VERSION, TagType.STRING)
              .setVStr(span.getInstrumentationLibraryInfo().getVersion()));
    }

    if (!span.getStatus().isOk()) {
      tags.add(toTag(KEY_ERROR, true));
    }
    target.setTags(tags);

    return target;
  }

  /**
   * Converts {@link Event}s into a collection of Jaeger's {@link Log}.
   *
   * @param timedEvents the timed events to be converted
   * @return a collection of Jaeger logs
   * @see #toJaegerLog(Event)
   */
  // VisibleForTesting
  static List<Log> toJaegerLogs(List<Event> timedEvents) {
    return timedEvents.stream().map(Adapter::toJaegerLog).collect(Collectors.toList());
  }

  /**
   * Converts a {@link Event} into Jaeger's {@link Log}.
   *
   * @param event the timed event to be converted
   * @return a Jaeger log
   */
  // VisibleForTesting
  static Log toJaegerLog(Event event) {
    Log result = new Log();
    result.setTimestamp(TimeUnit.NANOSECONDS.toMicros(event.getEpochNanos()));
    result.addToFields(new Tag(KEY_LOG_EVENT, TagType.STRING).setVStr(event.getName()));

    int droppedAttributesCount = event.getDroppedAttributesCount();
    if (droppedAttributesCount > 0) {
      result.addToFields(
          new Tag(KEY_EVENT_DROPPED_ATTRIBUTES_COUNT, TagType.LONG)
              .setVLong(droppedAttributesCount));
    }
    List<Tag> attributeTags = toTags(event.getAttributes());
    for (Tag attributeTag : attributeTags) {
      result.addToFields(attributeTag);
    }
    return result;
  }

  /**
   * Converts a map of attributes into a collection of Jaeger's {@link Tag}.
   *
   * @param attributes the span attributes
   * @return a collection of Jaeger key values
   * @see #toTag
   */
  static List<Tag> toTags(ReadableAttributes attributes) {
    List<Tag> results = new ArrayList<>();
    attributes.forEach(
        new AttributeConsumer() {
          @Override
          public <T> void accept(AttributeKey<T> key, T value) {
            results.add(toTag(key, value));
          }
        });
    return results;
  }

  /**
   * Converts the given {@link AttributeKey} and value into Jaeger's {@link Tag}.
   *
   * @param key the entry key as string
   * @param value the entry value
   * @return a Jaeger key value
   */
  // VisibleForTesting
  static <T> Tag toTag(AttributeKey<T> key, T value) {
    switch (key.getType()) {
      case STRING:
        return new Tag(key.getKey(), TagType.STRING).setVStr((String) value);
      case LONG:
        return new Tag(key.getKey(), TagType.LONG).setVLong((long) value);
      case BOOLEAN:
        return new Tag(key.getKey(), TagType.BOOL).setVBool((boolean) value);
      case DOUBLE:
        return new Tag(key.getKey(), TagType.DOUBLE).setVDouble((double) value);
      default:
        return new Tag(key.getKey(), TagType.STRING).setVStr(GSON.toJson(value));
    }
  }

  /**
   * Converts {@link Link}s into a collection of Jaeger's {@link SpanRef}.
   *
   * @param links the span's links property to be converted
   * @return a collection of Jaeger span references
   */
  // VisibleForTesting
  static List<SpanRef> toSpanRefs(List<Link> links) {
    List<SpanRef> spanRefs = new ArrayList<>(links.size());
    for (Link link : links) {
      spanRefs.add(toSpanRef(link));
    }
    return spanRefs;
  }

  /**
   * Converts a single {@link Link} into a Jaeger's {@link SpanRef}.
   *
   * @param link the OpenTelemetry link to be converted
   * @return the Jaeger span reference
   */
  // VisibleForTesting
  static SpanRef toSpanRef(Link link) {
    // we can assume that all links are *follows from*
    // https://github.com/open-telemetry/opentelemetry-java/issues/475
    // https://github.com/open-telemetry/opentelemetry-java/pull/481/files#r312577862
    return new SpanRef(
        SpanRefType.FOLLOWS_FROM,
        TraceId.traceIdLowBytesAsLong(link.getContext().getTraceIdAsHexString()),
        TraceId.traceIdHighBytesAsLong(link.getContext().getTraceIdAsHexString()),
        SpanId.asLong(link.getContext().getSpanIdAsHexString()));
  }
}
