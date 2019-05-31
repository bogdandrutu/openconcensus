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

package io.opentelemetry.sdk.trace;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.EvictingQueue;
import com.google.protobuf.Timestamp;
import io.opentelemetry.resource.Resource;
import io.opentelemetry.sdk.internal.Clock;
import io.opentelemetry.sdk.internal.TimestampConverter;
import io.opentelemetry.sdk.trace.config.TraceConfig;
import io.opentelemetry.trace.AttributeValue;
import io.opentelemetry.trace.Event;
import io.opentelemetry.trace.Link;
import io.opentelemetry.trace.Span;
import io.opentelemetry.trace.SpanContext;
import io.opentelemetry.trace.SpanData;
import io.opentelemetry.trace.SpanId;
import io.opentelemetry.trace.Status;
import io.opentelemetry.trace.Tracer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/** Implementation for the {@link Span} class that records trace events. */
@ThreadSafe
public final class RecordEventsSpanImpl implements SpanSdk {
  private static final Logger logger = Logger.getLogger(Tracer.class.getName());

  // Contains the identifiers associated with this Span.
  private final SpanContext context;
  // The parent SpanId of this span. Null if this is a root span.
  @Nullable private final SpanId parentSpanId;
  // True if the parent is on a different process.
  @Nullable private final Boolean hasRemoteParent;
  // Active trace configs when the Span was created.
  private final TraceConfig traceConfig;
  // Handler called when the span starts and ends.
  private final SpanProcessor spanProcessor;
  // The displayed name of the span.
  @GuardedBy("this")
  private String name;
  // The kind of the span.
  private final Kind kind;
  // The clock used to get the time.
  private final Clock clock;
  // The time converter used to convert nano time to Timestamp. This is needed because Java has
  // millisecond granularity for Timestamp and tracing events are recorded more often.
  private final TimestampConverter timestampConverter;
  // The resource associated with this span.
  private final Resource resource;
  // The start time of the span.
  private final long startNanoTime;
  // Set of recorded attributes. DO NOT CALL any other method that changes the ordering of events.
  @GuardedBy("this")
  @Nullable
  private AttributesWithCapacity attributes;
  // List of recorded events.
  @GuardedBy("this")
  @Nullable
  private EvictingQueue<TimedEvent> events;
  // Number of events recorded.
  @GuardedBy("this")
  private int totalRecordedEvents = 0;
  // List of recorded links to parent and child spans.
  @GuardedBy("this")
  @Nullable
  private EvictingQueue<Link> links;
  // Number of link recorded.
  @GuardedBy("this")
  private int totalRecordedLinks = 0;
  // The number of children.
  @GuardedBy("this")
  private int numberOfChildren;
  // The status of the span.
  @GuardedBy("this")
  @Nullable
  private Status status;
  // The end time of the span.
  @GuardedBy("this")
  private long endNanoTime;
  // True if the span is ended.
  @GuardedBy("this")
  private boolean hasBeenEnded;

  /**
   * Creates and starts a span with the given configuration.
   *
   * @param context supplies the trace_id and span_id for the newly started span.
   * @param name the displayed name for the new span.
   * @param parentSpanId the span_id of the parent span, or null if the new span is a root span.
   * @param hasRemoteParent {@code true} if the parentContext is remote. {@code null} if this is a
   *     root span.
   * @param traceConfig trace parameters like sampler and probability.
   * @param spanProcessor handler called when the span starts and ends.
   * @param timestampConverter null if the span is a root span or the parent is not sampled. If the
   *     parent is sampled, we should use the same converter to ensure ordering between tracing
   *     events.
   * @param clock the clock used to get the time.
   * @return a new and started span.
   */
  @VisibleForTesting
  public static RecordEventsSpanImpl startSpan(
      SpanContext context,
      String name,
      Kind kind,
      @Nullable SpanId parentSpanId,
      @Nullable Boolean hasRemoteParent,
      TraceConfig traceConfig,
      SpanProcessor spanProcessor,
      @Nullable TimestampConverter timestampConverter,
      Clock clock,
      Resource resource) {
    RecordEventsSpanImpl span =
        new RecordEventsSpanImpl(
            context,
            name,
            kind,
            parentSpanId,
            hasRemoteParent,
            traceConfig,
            spanProcessor,
            timestampConverter,
            clock,
            resource);
    // Call onStart here instead of calling in the constructor to make sure the span is completely
    // initialized.
    spanProcessor.onStartSync(span);
    return span;
  }

  /**
   * Returns the name of the {@code Span}.
   *
   * @return the name of the {@code Span}.
   */
  @Override
  public String getName() {
    synchronized (this) {
      return name;
    }
  }

  @Override
  public io.opentelemetry.proto.trace.v1.Span toSpanProto() {
    throw new UnsupportedOperationException("to be implemented");
  }

  /**
   * Returns the status of the {@code Span}. If not set defaults to {@link Status#OK}.
   *
   * @return the status of the {@code Span}.
   */
  public Status getStatus() {
    synchronized (this) {
      return getStatusWithDefault();
    }
  }

  /**
   * Returns the end nano time (see {@link System#nanoTime()}). If the current {@code Span} is not
   * ended then returns {@link Clock#nowNanos()}.
   *
   * @return the end nano time.
   */
  public long getEndNanoTime() {
    synchronized (this) {
      return hasBeenEnded ? endNanoTime : clock.nowNanos();
    }
  }

  /**
   * Returns the latency of the {@code Span} in nanos. If still active then returns now() - start
   * time.
   *
   * @return the latency of the {@code Span} in nanos.
   */
  public long getLatencyNs() {
    synchronized (this) {
      return hasBeenEnded ? endNanoTime - startNanoTime : clock.nowNanos() - startNanoTime;
    }
  }

  /**
   * Returns the kind of this {@code Span}.
   *
   * @return the kind of this {@code Span}.
   */
  public Kind getKind() {
    return kind;
  }

  /**
   * Returns the {@code TimestampConverter} used by this {@code Span}.
   *
   * @return the {@code TimestampConverter} used by this {@code Span}.
   */
  @Nullable
  TimestampConverter getTimestampConverter() {
    return timestampConverter;
  }

  /**
   * Returns an immutable representation of all the data from this {@code Span}.
   *
   * <p>This should only be called with an ended {@code Span}.
   *
   * @return an immutable representation of all the data from this {@code Span}.
   * @throws IllegalStateException if the Span doesn't have RECORD_EVENTS option.
   */
  public SpanData toSpanData() {
    synchronized (this) {
      Preconditions.checkState(
          hasBeenEnded, "Calling toSpanData() on a running span is not supported.");
      return SpanData.create(
          getContext(),
          parentSpanId,
          resource,
          name,
          kind,
          toSpanDataTimestamp(timestampConverter, startNanoTime),
          attributes == null
              ? Collections.<String, AttributeValue>emptyMap()
              : Collections.unmodifiableMap(attributes),
          events == null
              ? Collections.<SpanData.TimedEvent>emptyList()
              : toSpanDataTimedEvents(events, timestampConverter),
          links == null
              ? Collections.<Link>emptyList()
              : Collections.unmodifiableList(new ArrayList<Link>(links)),
          getStatusWithDefault(),
          toSpanDataTimestamp(timestampConverter, endNanoTime));
    }
  }

  @Override
  public void setAttribute(String key, String value) {
    setAttribute(key, AttributeValue.stringAttributeValue(value));
  }

  @Override
  public void setAttribute(String key, long value) {
    setAttribute(key, AttributeValue.longAttributeValue(value));
  }

  @Override
  public void setAttribute(String key, double value) {
    setAttribute(key, AttributeValue.doubleAttributeValue(value));
  }

  @Override
  public void setAttribute(String key, boolean value) {
    setAttribute(key, AttributeValue.booleanAttributeValue(value));
  }

  @Override
  public void setAttribute(String key, AttributeValue value) {
    Preconditions.checkNotNull(key, "key");
    Preconditions.checkNotNull(value, "value");
    synchronized (this) {
      if (hasBeenEnded) {
        logger.log(Level.FINE, "Calling setAttribute() on an ended Span.");
        return;
      }
      getInitializedAttributes().putAttribute(key, value);
    }
  }

  @Override
  public void addEvent(String name) {
    addTimedEvent(TimedEvent.create(clock.nowNanos(), name));
  }

  @Override
  public void addEvent(String name, Map<String, AttributeValue> attributes) {
    addTimedEvent(TimedEvent.create(clock.nowNanos(), name, attributes));
  }

  @Override
  public void addEvent(Event event) {
    addTimedEvent(TimedEvent.create(clock.nowNanos(), event));
  }

  private void addTimedEvent(TimedEvent timedEvent) {
    synchronized (this) {
      if (hasBeenEnded) {
        logger.log(Level.FINE, "Calling addEvent() on an ended Span.");
        return;
      }
      getInitializedEvents().add(timedEvent);
      totalRecordedEvents++;
    }
  }

  private static List<SpanData.TimedEvent> toSpanDataTimedEvents(
      Collection<TimedEvent> timedEvents, TimestampConverter converter) {
    List<SpanData.TimedEvent> spanDataTimedEvents = new ArrayList<>(timedEvents.size());
    for (TimedEvent timedEvent : timedEvents) {
      spanDataTimedEvents.add(
          SpanData.TimedEvent.create(
              toSpanDataTimestamp(converter, timedEvent.getNanotime()),
              SpanData.Event.create(timedEvent.getName(), timedEvent.getAttributes())));
    }
    return Collections.unmodifiableList(spanDataTimedEvents);
  }

  @Override
  public void addLink(Link link) {
    Preconditions.checkNotNull(link, "link");
    synchronized (this) {
      if (hasBeenEnded) {
        logger.log(Level.FINE, "Calling addLink() on an ended Span.");
        return;
      }
      getInitializedLinks().add(link);
      totalRecordedLinks++;
    }
  }

  @Override
  public void setStatus(Status status) {
    Preconditions.checkNotNull(status, "status");
    synchronized (this) {
      if (hasBeenEnded) {
        logger.log(Level.FINE, "Calling setStatus() on an ended Span.");
        return;
      }
      this.status = status;
    }
  }

  @Override
  public void updateName(String name) {
    Preconditions.checkNotNull(name, "name");
    synchronized (this) {
      this.name = name;
    }
  }

  @Override
  public void end() {
    synchronized (this) {
      if (hasBeenEnded) {
        logger.log(Level.FINE, "Calling end() on an ended Span.");
        return;
      }
      endNanoTime = clock.nowNanos();
      hasBeenEnded = true;
    }
    spanProcessor.onEndSync(this);
  }

  @Override
  public SpanContext getContext() {
    return context;
  }

  @Override
  public boolean isRecordingEvents() {
    return true;
  }

  void addChild() {
    synchronized (this) {
      if (hasBeenEnded) {
        logger.log(Level.FINE, "Calling end() on an ended Span.");
        return;
      }
      numberOfChildren++;
    }
  }

  @GuardedBy("this")
  private AttributesWithCapacity getInitializedAttributes() {
    if (attributes == null) {
      attributes = new AttributesWithCapacity(traceConfig.getMaxNumberOfAttributes());
    }
    return attributes;
  }

  @GuardedBy("this")
  private EvictingQueue<TimedEvent> getInitializedEvents() {
    if (events == null) {
      events = EvictingQueue.<TimedEvent>create((int) traceConfig.getMaxNumberOfEvents());
    }
    return events;
  }

  @GuardedBy("this")
  private EvictingQueue<Link> getInitializedLinks() {
    if (links == null) {
      links = EvictingQueue.<Link>create((int) traceConfig.getMaxNumberOfLinks());
    }
    return links;
  }

  @GuardedBy("this")
  private Status getStatusWithDefault() {
    return status == null ? Status.OK : status;
  }

  // A map implementation with a fixed capacity that drops events when the map gets full. Eviction
  // is based on the access order.
  static final class AttributesWithCapacity extends LinkedHashMap<String, AttributeValue> {
    private final long capacity;
    private int totalRecordedAttributes = 0;
    // Here because -Werror complains about this: [serial] serializable class AttributesWithCapacity
    // has no definition of serialVersionUID. This class shouldn't be serialized.
    private static final long serialVersionUID = 42L;

    private AttributesWithCapacity(long capacity) {
      // Capacity of the map is capacity + 1 to avoid resizing because removeEldestEntry is invoked
      // by put and putAll after inserting a new entry into the map. The loadFactor is set to 1
      // to avoid resizing because. The accessOrder is set to true.
      super((int) capacity + 1, 1, /*accessOrder=*/ true);
      this.capacity = capacity;
    }

    // Users must call this method instead of put to keep count of the total number of entries
    // inserted.
    private void putAttribute(String key, AttributeValue value) {
      totalRecordedAttributes += 1;
      put(key, value);
    }

    int getNumberOfDroppedAttributes() {
      return totalRecordedAttributes - size();
    }

    // It is called after each put or putAll call in order to determine if the eldest inserted
    // entry should be removed or not.
    @Override
    protected boolean removeEldestEntry(Map.Entry<String, AttributeValue> eldest) {
      return size() > this.capacity;
    }
  }

  private RecordEventsSpanImpl(
      SpanContext context,
      String name,
      Kind kind,
      @Nullable SpanId parentSpanId,
      @Nullable Boolean hasRemoteParent,
      TraceConfig traceConfig,
      SpanProcessor spanProcessor,
      @Nullable TimestampConverter timestampConverter,
      Clock clock,
      Resource resource) {
    this.context = context;
    this.parentSpanId = parentSpanId;
    this.hasRemoteParent = hasRemoteParent;
    this.name = name;
    this.kind = kind;
    this.traceConfig = traceConfig;
    this.spanProcessor = spanProcessor;
    this.clock = clock;
    this.resource = resource;
    this.hasBeenEnded = false;
    this.numberOfChildren = 0;
    this.timestampConverter =
        timestampConverter != null ? timestampConverter : TimestampConverter.now(clock);
    startNanoTime = clock.nowNanos();
  }

  @SuppressWarnings("NoFinalizer")
  @Override
  protected void finalize() throws Throwable {
    synchronized (this) {
      if (!hasBeenEnded) {
        logger.log(Level.SEVERE, "Span " + name + " is GC'ed without being ended.");
      }
    }
    super.finalize();
  }

  private static SpanData.Timestamp toSpanDataTimestamp(
      TimestampConverter timestampConverter, long nanoTime) {
    Timestamp protoTimestamp = timestampConverter.convertNanoTime(nanoTime);
    return SpanData.Timestamp.create(protoTimestamp.getSeconds(), protoTimestamp.getNanos());
  }
}
