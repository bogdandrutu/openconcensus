/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.trace;

import static io.opentelemetry.api.common.AttributeKey.booleanKey;
import static io.opentelemetry.api.common.AttributeKey.doubleKey;
import static io.opentelemetry.api.common.AttributeKey.longKey;
import static io.opentelemetry.api.common.AttributeKey.stringKey;

import com.google.common.collect.EvictingQueue;
import io.opentelemetry.api.common.AttributeConsumer;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.common.ReadableAttributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.attributes.SemanticAttributes;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.common.Clock;
import io.opentelemetry.sdk.common.InstrumentationLibraryInfo;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.config.TraceConfig;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.data.SpanData.Event;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/** Implementation for the {@link Span} class that records trace events. */
@ThreadSafe
final class RecordEventsReadableSpan implements ReadWriteSpan {

  private static final Logger logger = Logger.getLogger(RecordEventsReadableSpan.class.getName());

  // The config used when constructing this Span.
  private final TraceConfig traceConfig;
  // Contains the identifiers associated with this Span.
  private final SpanContext context;
  // The parent SpanId of this span. Invalid if this is a root span.
  private final String parentSpanId;
  // True if the parent is on a different process.
  private final boolean hasRemoteParent;
  // Handler called when the span starts and ends.
  private final SpanProcessor spanProcessor;
  // The displayed name of the span.
  // List of recorded links to parent and child spans.
  private final List<SpanData.Link> links;
  // Number of links recorded.
  private final int totalRecordedLinks;
  // The kind of the span.
  private final Kind kind;
  // The clock used to get the time.
  private final Clock clock;
  // The resource associated with this span.
  private final Resource resource;
  // instrumentation library of the named tracer which created this span
  private final InstrumentationLibraryInfo instrumentationLibraryInfo;
  // The start time of the span.
  private final long startEpochNanos;
  // Lock used to internally guard the mutable state of this instance
  private final Object lock = new Object();

  @GuardedBy("lock")
  private String name;
  // Set of recorded attributes. DO NOT CALL any other method that changes the ordering of events.
  @GuardedBy("lock")
  @Nullable
  private AttributesMap attributes;
  // List of recorded events.
  @GuardedBy("lock")
  private final EvictingQueue<Event> events;
  // Number of events recorded.
  @GuardedBy("lock")
  private int totalRecordedEvents = 0;
  // The status of the span.
  @GuardedBy("lock")
  @Nullable
  private SpanData.Status status = SpanData.Status.unset();
  // The end time of the span.
  @GuardedBy("lock")
  private long endEpochNanos;
  // True if the span is ended.
  @GuardedBy("lock")
  private boolean hasEnded;

  private RecordEventsReadableSpan(
      SpanContext context,
      String name,
      InstrumentationLibraryInfo instrumentationLibraryInfo,
      Kind kind,
      String parentSpanId,
      boolean hasRemoteParent,
      TraceConfig traceConfig,
      SpanProcessor spanProcessor,
      Clock clock,
      Resource resource,
      @Nullable AttributesMap attributes,
      List<SpanData.Link> links,
      int totalRecordedLinks,
      long startEpochNanos) {
    this.context = context;
    this.instrumentationLibraryInfo = instrumentationLibraryInfo;
    this.parentSpanId = parentSpanId;
    this.hasRemoteParent = hasRemoteParent;
    this.links = links;
    this.totalRecordedLinks = totalRecordedLinks;
    this.name = name;
    this.kind = kind;
    this.spanProcessor = spanProcessor;
    this.resource = resource;
    this.hasEnded = false;
    this.clock = clock;
    this.startEpochNanos = startEpochNanos;
    this.attributes = attributes;
    this.events = EvictingQueue.create(traceConfig.getMaxNumberOfEvents());
    this.traceConfig = traceConfig;
  }

  /**
   * Creates and starts a span with the given configuration.
   *
   * @param context supplies the trace_id and span_id for the newly started span.
   * @param name the displayed name for the new span.
   * @param kind the span kind.
   * @param parentSpanId the span_id of the parent span, or {@code Span.INVALID} if the new span is
   *     a root span.
   * @param hasRemoteParent {@code true} if the parentContext is remote. {@code false} if this is a
   *     root span.
   * @param traceConfig trace parameters like sampler and probability.
   * @param spanProcessor handler called when the span starts and ends.
   * @param clock the clock used to get the time.
   * @param resource the resource associated with this span.
   * @param attributes the attributes set during span creation.
   * @param links the links set during span creation, may be truncated. The list MUST be immutable.
   * @return a new and started span.
   */
  static RecordEventsReadableSpan startSpan(
      SpanContext context,
      String name,
      InstrumentationLibraryInfo instrumentationLibraryInfo,
      Kind kind,
      @Nullable String parentSpanId,
      boolean hasRemoteParent,
      @Nonnull Context parentContext,
      TraceConfig traceConfig,
      SpanProcessor spanProcessor,
      Clock clock,
      Resource resource,
      AttributesMap attributes,
      List<SpanData.Link> links,
      int totalRecordedLinks,
      long startEpochNanos) {
    RecordEventsReadableSpan span =
        new RecordEventsReadableSpan(
            context,
            name,
            instrumentationLibraryInfo,
            kind,
            parentSpanId,
            hasRemoteParent,
            traceConfig,
            spanProcessor,
            clock,
            resource,
            attributes,
            links,
            totalRecordedLinks,
            startEpochNanos == 0 ? clock.now() : startEpochNanos);
    // Call onStart here instead of calling in the constructor to make sure the span is completely
    // initialized.
    spanProcessor.onStart(parentContext, span);
    return span;
  }

  @Override
  public SpanData toSpanData() {
    // Copy within synchronized context
    synchronized (lock) {
      return SpanWrapper.create(
          this,
          links,
          getImmutableTimedEvents(),
          getImmutableAttributes(),
          (attributes == null) ? 0 : attributes.getTotalAddedValues(),
          totalRecordedEvents,
          getSpanDataStatus(),
          name,
          endEpochNanos,
          hasEnded);
    }
  }

  @Override
  public boolean hasEnded() {
    synchronized (lock) {
      return hasEnded;
    }
  }

  @Override
  public SpanContext getSpanContext() {
    return context;
  }

  /**
   * Returns the name of the {@code Span}.
   *
   * @return the name of the {@code Span}.
   */
  @Override
  public String getName() {
    synchronized (lock) {
      return name;
    }
  }

  /**
   * Returns the instrumentation library specified when creating the tracer which produced this
   * span.
   *
   * @return an instance of {@link InstrumentationLibraryInfo} describing the instrumentation
   *     library
   */
  @Override
  public InstrumentationLibraryInfo getInstrumentationLibraryInfo() {
    return instrumentationLibraryInfo;
  }

  /**
   * Returns the latency of the {@code Span} in nanos. If still active then returns now() - start
   * time.
   *
   * @return the latency of the {@code Span} in nanos.
   */
  @Override
  public long getLatencyNanos() {
    synchronized (lock) {
      return (hasEnded ? endEpochNanos : clock.now()) - startEpochNanos;
    }
  }

  /**
   * Returns the {@code Clock} used by this {@code Span}.
   *
   * @return the {@code Clock} used by this {@code Span}.
   */
  Clock getClock() {
    return clock;
  }

  @Override
  public ReadWriteSpan setAttribute(String key, String value) {
    setAttribute(stringKey(key), value);
    return this;
  }

  @Override
  public ReadWriteSpan setAttribute(String key, long value) {
    setAttribute(longKey(key), value);
    return this;
  }

  @Override
  public ReadWriteSpan setAttribute(String key, double value) {
    setAttribute(doubleKey(key), value);
    return this;
  }

  @Override
  public ReadWriteSpan setAttribute(String key, boolean value) {
    setAttribute(booleanKey(key), value);
    return this;
  }

  @Override
  public <T> ReadWriteSpan setAttribute(AttributeKey<T> key, T value) {
    if (key == null || key.getKey() == null || key.getKey().length() == 0 || value == null) {
      return this;
    }
    synchronized (lock) {
      if (hasEnded) {
        logger.log(Level.FINE, "Calling setAttribute() on an ended Span.");
        return this;
      }
      if (attributes == null) {
        attributes = new AttributesMap(traceConfig.getMaxNumberOfAttributes());
      }

      if (traceConfig.shouldTruncateStringAttributeValues()) {
        value = StringUtils.truncateToSize(key, value, traceConfig.getMaxLengthOfAttributeValues());
      }

      attributes.put(key, value);
    }
    return this;
  }

  @Override
  public ReadWriteSpan addEvent(String name) {
    if (name == null) {
      return this;
    }
    addTimedEvent(Event.create(clock.now(), name, Attributes.empty(), 0));
    return this;
  }

  @Override
  public ReadWriteSpan addEvent(String name, long timestamp, TimeUnit unit) {
    if (name == null) {
      return this;
    }
    addTimedEvent(Event.create(unit.toNanos(timestamp), name, Attributes.empty(), 0));
    return this;
  }

  @Override
  public ReadWriteSpan addEvent(String name, Attributes attributes) {
    if (name == null) {
      return this;
    }
    int totalAttributeCount = attributes.size();
    addTimedEvent(
        Event.create(
            clock.now(),
            name,
            copyAndLimitAttributes(attributes, traceConfig.getMaxNumberOfAttributesPerEvent()),
            totalAttributeCount));
    return this;
  }

  @Override
  public ReadWriteSpan addEvent(String name, Attributes attributes, long timestamp, TimeUnit unit) {
    if (name == null) {
      return this;
    }
    int totalAttributeCount = attributes.size();
    addTimedEvent(
        Event.create(
            unit.toNanos(timestamp),
            name,
            copyAndLimitAttributes(attributes, traceConfig.getMaxNumberOfAttributesPerEvent()),
            totalAttributeCount));
    return this;
  }

  static Attributes copyAndLimitAttributes(final Attributes attributes, final int limit) {
    if (attributes.isEmpty() || attributes.size() <= limit) {
      return attributes;
    }

    AttributesBuilder result = Attributes.builder();
    attributes.forEach(new LimitingAttributeConsumer(limit, result));
    return result.build();
  }

  private void addTimedEvent(Event timedEvent) {
    synchronized (lock) {
      if (hasEnded) {
        logger.log(Level.FINE, "Calling addEvent() on an ended Span.");
        return;
      }
      events.add(timedEvent);
      totalRecordedEvents++;
    }
  }

  @Override
  public ReadWriteSpan setStatus(StatusCode statusCode) {
    setStatus(statusCode, null);
    return this;
  }

  @Override
  public ReadWriteSpan setStatus(StatusCode statusCode, @Nullable String description) {
    if (statusCode == null) {
      return this;
    }
    synchronized (lock) {
      if (hasEnded) {
        logger.log(Level.FINE, "Calling setStatus() on an ended Span.");
        return this;
      }
      this.status = SpanData.Status.create(statusCode, description);
    }
    return this;
  }

  @Override
  public ReadWriteSpan recordException(Throwable exception) {
    recordException(exception, null);
    return this;
  }

  @Override
  public ReadWriteSpan recordException(Throwable exception, Attributes additionalAttributes) {
    if (exception == null) {
      return this;
    }
    long timestampNanos = clock.now();

    AttributesBuilder attributes = Attributes.builder();
    attributes.put(SemanticAttributes.EXCEPTION_TYPE, exception.getClass().getCanonicalName());
    if (exception.getMessage() != null) {
      attributes.put(SemanticAttributes.EXCEPTION_MESSAGE, exception.getMessage());
    }
    StringWriter writer = new StringWriter();
    exception.printStackTrace(new PrintWriter(writer));
    attributes.put(SemanticAttributes.EXCEPTION_STACKTRACE, writer.toString());

    if (additionalAttributes != null) {
      attributes.putAll(additionalAttributes);
    }

    addEvent(
        SemanticAttributes.EXCEPTION_EVENT_NAME,
        attributes.build(),
        timestampNanos,
        TimeUnit.NANOSECONDS);
    return this;
  }

  @Override
  public ReadWriteSpan updateName(String name) {
    if (name == null) {
      return this;
    }
    synchronized (lock) {
      if (hasEnded) {
        logger.log(Level.FINE, "Calling updateName() on an ended Span.");
        return this;
      }
      this.name = name;
    }
    return this;
  }

  @Override
  public void end() {
    endInternal(clock.now());
  }

  @Override
  public void end(long timestamp, TimeUnit unit) {
    if (unit == null) {
      unit = TimeUnit.NANOSECONDS;
    }
    endInternal(timestamp == 0 ? clock.now() : unit.toNanos(timestamp));
  }

  private void endInternal(long endEpochNanos) {
    synchronized (lock) {
      if (hasEnded) {
        logger.log(Level.FINE, "Calling end() on an ended Span.");
        return;
      }
      this.endEpochNanos = endEpochNanos;
      hasEnded = true;
    }
    spanProcessor.onEnd(this);
  }

  @Override
  public boolean isRecording() {
    synchronized (lock) {
      return !hasEnded;
    }
  }

  @GuardedBy("lock")
  private SpanData.Status getSpanDataStatus() {
    synchronized (lock) {
      return status;
    }
  }

  String getParentSpanId() {
    return parentSpanId;
  }

  Resource getResource() {
    return resource;
  }

  Kind getKind() {
    return kind;
  }

  long getStartEpochNanos() {
    return startEpochNanos;
  }

  boolean hasRemoteParent() {
    return hasRemoteParent;
  }

  int getTotalRecordedLinks() {
    return totalRecordedLinks;
  }

  @GuardedBy("lock")
  private List<Event> getImmutableTimedEvents() {
    if (events.isEmpty()) {
      return Collections.emptyList();
    }

    return Collections.unmodifiableList(new ArrayList<>(events));
  }

  @GuardedBy("lock")
  private ReadableAttributes getImmutableAttributes() {
    if (attributes == null || attributes.isEmpty()) {
      return Attributes.empty();
    }
    // if the span has ended, then the attributes are unmodifiable,
    // so we can return them directly and save copying all the data.
    if (hasEnded) {
      return attributes;
    }
    // otherwise, make a copy of the data into an immutable container.
    return attributes.immutableCopy();
  }

  @Override
  public String toString() {
    String name;
    String attributes;
    String status;
    long totalRecordedEvents;
    long endEpochNanos;
    synchronized (lock) {
      name = this.name;
      attributes = String.valueOf(this.attributes);
      status = String.valueOf(this.status);
      totalRecordedEvents = this.totalRecordedEvents;
      endEpochNanos = this.endEpochNanos;
    }
    StringBuilder sb = new StringBuilder();
    sb.append("RecordEventsReadableSpan{traceId=");
    sb.append(context.getTraceIdAsHexString());
    sb.append(", spanId=");
    sb.append(context.getSpanIdAsHexString());
    sb.append(", parentSpanId=");
    sb.append(parentSpanId);
    sb.append(", name=");
    sb.append(name);
    sb.append(", kind=");
    sb.append(kind);
    sb.append(", attributes=");
    sb.append(attributes);
    sb.append(", status=");
    sb.append(status);
    sb.append(", totalRecordedEvents=");
    sb.append(totalRecordedEvents);
    sb.append(", totalRecordedLinks=");
    sb.append(totalRecordedLinks);
    sb.append(", startEpochNanos=");
    sb.append(startEpochNanos);
    sb.append(", endEpochNanos=");
    sb.append(endEpochNanos);
    sb.append("}");
    return sb.toString();
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static class LimitingAttributeConsumer implements AttributeConsumer {
    private final int limit;
    private final AttributesBuilder builder;
    private int added;

    public LimitingAttributeConsumer(int limit, AttributesBuilder builder) {
      this.limit = limit;
      this.builder = builder;
    }

    @Override
    public void accept(AttributeKey key, Object value) {
      if (added < limit) {
        builder.put(key, value);
        added++;
      }
    }
  }
}
