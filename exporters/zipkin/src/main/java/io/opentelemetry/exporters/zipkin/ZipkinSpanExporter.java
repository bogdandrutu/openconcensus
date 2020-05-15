/*
 * Copyright 2020, OpenTelemetry Authors
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

package io.opentelemetry.exporters.zipkin;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import io.opentelemetry.common.AttributeValue;
import io.opentelemetry.common.AttributeValue.Type;
import io.opentelemetry.sdk.resources.ResourceConstants;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.trace.Span.Kind;
import io.opentelemetry.trace.Status;
import io.opentelemetry.trace.attributes.SemanticAttributes;
import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.codec.BytesEncoder;
import zipkin2.reporter.Sender;

/**
 * This class was based on the OpenCensus zipkin exporter code at
 * https://github.com/census-instrumentation/opencensus-java/tree/c960b19889de5e4a7b25f90919d28b066590d4f0/exporters/trace/zipkin
 */
public final class ZipkinSpanExporter implements SpanExporter {

  private static final Logger logger = Logger.getLogger(ZipkinSpanExporter.class.getName());

  // The naming follows Zipkin convention. For http see here:
  // https://github.com/openzipkin/brave/blob/eee993f998ae57b08644cc357a6d478827428710/instrumentation/http/src/main/java/brave/http/HttpTags.java
  // For discussion about GRPC errors/tags, see here:  https://github.com/openzipkin/brave/pull/999
  // Note: these 3 fields are non-private for testing
  static final String GRPC_STATUS_CODE = "grpc.status_code";
  static final String GRPC_STATUS_DESCRIPTION = "grpc.status_description";
  static final String STATUS_ERROR = "error";

  private final BytesEncoder<Span> encoder;
  private final Sender sender;
  private final Endpoint localEndpoint;

  ZipkinSpanExporter(BytesEncoder<Span> encoder, Sender sender, String serviceName) {
    this.encoder = encoder;
    this.sender = sender;
    this.localEndpoint = produceLocalEndpoint(serviceName);
  }

  /** Logic borrowed from brave.internal.Platform.produceLocalEndpoint */
  static Endpoint produceLocalEndpoint(String serviceName) {
    Endpoint.Builder builder = Endpoint.newBuilder().serviceName(serviceName);
    try {
      Enumeration<NetworkInterface> nics = NetworkInterface.getNetworkInterfaces();
      if (nics == null) {
        return builder.build();
      }
      while (nics.hasMoreElements()) {
        NetworkInterface nic = nics.nextElement();
        Enumeration<InetAddress> addresses = nic.getInetAddresses();
        while (addresses.hasMoreElements()) {
          InetAddress address = addresses.nextElement();
          if (address.isSiteLocalAddress()) {
            builder.ip(address);
            break;
          }
        }
      }
    } catch (Exception e) {
      // don't crash the caller if there was a problem reading nics.
      if (logger.isLoggable(Level.FINE)) {
        logger.log(Level.FINE, "error reading nics", e);
      }
    }
    return builder.build();
  }

  static Span generateSpan(SpanData spanData, Endpoint localEndpoint) {
    Endpoint endpoint = chooseEndpoint(spanData, localEndpoint);

    long startTimestamp = toEpochMicros(spanData.getStartEpochNanos());

    long endTimestamp = toEpochMicros(spanData.getEndEpochNanos());

    Span.Builder spanBuilder =
        Span.newBuilder()
            .traceId(spanData.getTraceId().toLowerBase16())
            .id(spanData.getSpanId().toLowerBase16())
            .kind(toSpanKind(spanData))
            .name(spanData.getName())
            .timestamp(toEpochMicros(spanData.getStartEpochNanos()))
            .duration(endTimestamp - startTimestamp)
            .localEndpoint(endpoint);

    if (spanData.getParentSpanId().isValid()) {
      spanBuilder.parentId(spanData.getParentSpanId().toLowerBase16());
    }

    Map<String, AttributeValue> spanAttributes = spanData.getAttributes();
    for (Map.Entry<String, AttributeValue> label : spanAttributes.entrySet()) {
      spanBuilder.putTag(label.getKey(), attributeValueToString(label.getValue()));
    }
    Status status = spanData.getStatus();
    // for GRPC spans, include status code & description.
    if (status != null && spanAttributes.containsKey(SemanticAttributes.RPC_SERVICE.key())) {
      spanBuilder.putTag(GRPC_STATUS_CODE, status.getCanonicalCode().toString());
      if (status.getDescription() != null) {
        spanBuilder.putTag(GRPC_STATUS_DESCRIPTION, status.getDescription());
      }
    }
    // add the error tag, if it isn't already in the source span.
    if (status != null && !status.isOk() && !spanAttributes.containsKey(STATUS_ERROR)) {
      spanBuilder.putTag(STATUS_ERROR, status.getCanonicalCode().toString());
    }

    for (SpanData.TimedEvent annotation : spanData.getTimedEvents()) {
      spanBuilder.addAnnotation(toEpochMicros(annotation.getEpochNanos()), annotation.getName());
    }

    return spanBuilder.build();
  }

  private static Endpoint chooseEndpoint(SpanData spanData, Endpoint localEndpoint) {
    Map<String, AttributeValue> resourceAttributes = spanData.getResource().getAttributes();

    // use the service.name from the Resource, if it's been set.
    AttributeValue serviceNameValue = resourceAttributes.get(ResourceConstants.SERVICE_NAME);
    if (serviceNameValue == null) {
      return localEndpoint;
    }
    return Endpoint.newBuilder().serviceName(serviceNameValue.getStringValue()).build();
  }

  @Nullable
  private static Span.Kind toSpanKind(SpanData spanData) {
    // This is a hack because the Span API did not have SpanKind.
    if (spanData.getKind() == Kind.SERVER
        || (spanData.getKind() == null && Boolean.TRUE.equals(spanData.getHasRemoteParent()))) {
      return Span.Kind.SERVER;
    }

    // This is a hack because the Span API did not have SpanKind.
    if (spanData.getKind() == Kind.CLIENT || spanData.getName().startsWith("Sent.")) {
      return Span.Kind.CLIENT;
    }

    if (spanData.getKind() == Kind.PRODUCER) {
      return Span.Kind.PRODUCER;
    }
    if (spanData.getKind() == Kind.CONSUMER) {
      return Span.Kind.CONSUMER;
    }

    return null;
  }

  private static long toEpochMicros(long epochNanos) {
    return MICROSECONDS.convert(epochNanos, NANOSECONDS);
  }

  private static String attributeValueToString(AttributeValue attributeValue) {
    Type type = attributeValue.getType();
    switch (type) {
      case STRING:
        return attributeValue.getStringValue();
      case BOOLEAN:
        return String.valueOf(attributeValue.getBooleanValue());
      case LONG:
        return String.valueOf(attributeValue.getLongValue());
      case DOUBLE:
        return String.valueOf(attributeValue.getDoubleValue());
      case STRING_ARRAY:
        return commaSeparated(attributeValue.getStringArrayValue());
      case BOOLEAN_ARRAY:
        return commaSeparated(attributeValue.getBooleanArrayValue());
      case LONG_ARRAY:
        return commaSeparated(attributeValue.getLongArrayValue());
      case DOUBLE_ARRAY:
        return commaSeparated(attributeValue.getDoubleArrayValue());
    }
    throw new IllegalStateException("Unknown attribute type: " + type);
  }

  private static String commaSeparated(List<?> values) {
    StringBuilder builder = new StringBuilder();
    for (Object value : values) {
      if (builder.length() != 0) {
        builder.append(',');
      }
      builder.append(value);
    }
    return builder.toString();
  }

  @Override
  public ResultCode export(final Collection<SpanData> spanDataList) {
    List<byte[]> encodedSpans = new ArrayList<>(spanDataList.size());
    for (SpanData spanData : spanDataList) {
      encodedSpans.add(encoder.encode(generateSpan(spanData, localEndpoint)));
    }
    try {
      sender.sendSpans(encodedSpans).execute();
    } catch (IOException e) {
      return ResultCode.FAILURE;
    }
    return ResultCode.SUCCESS;
  }

  @Override
  public ResultCode flush() {
    // nothing required here
    return ResultCode.SUCCESS;
  }

  @Override
  public void shutdown() {
    try {
      sender.close();
    } catch (IOException e) {
      logger.log(Level.WARNING, "Exception while closing the Zipkin Sender instance", e);
    }
  }

  /**
   * Create a new {@link ZipkinSpanExporter} from the given configuration.
   *
   * @param configuration a {@link ZipkinExporterConfiguration} instance.
   * @return A ready-to-use {@link ZipkinSpanExporter}
   */
  public static ZipkinSpanExporter create(ZipkinExporterConfiguration configuration) {
    return new ZipkinSpanExporter(
        configuration.getEncoder(), configuration.getSender(), configuration.getServiceName());
  }
}
