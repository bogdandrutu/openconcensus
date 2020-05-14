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

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableList;
import io.opentelemetry.common.AttributeValue;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.data.SpanDataImpl;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.trace.Span.Kind;
import io.opentelemetry.trace.SpanId;
import io.opentelemetry.trace.Status;
import io.opentelemetry.trace.TraceFlags;
import io.opentelemetry.trace.TraceId;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.codec.Encoding;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.junit.ZipkinRule;
import zipkin2.reporter.urlconnection.URLConnectionSender;

/**
 * Tests which use Zipkin's {@link ZipkinRule} to verify that the {@link ZipkinSpanExporter} can
 * send spans via HTTP to Zipkin's API using supported encodings.
 */
@RunWith(JUnit4.class)
public class ZipkinSpanExporterEndToEndHttpTest {

  private static final String TRACE_ID = "d239036e7d5cec116b562147388b35bf";
  private static final String SPAN_ID = "9cc1e3049173be09";
  private static final String PARENT_SPAN_ID = "8b03ab423da481c5";
  private static final String SPAN_NAME = "Recv.helloworld.Greeter.SayHello";
  private static final long START_EPOCH_NANOS = 1505855794_194009601L;
  private static final long END_EPOCH_NANOS = 1505855799_465726528L;
  private static final long RECEIVED_TIMESTAMP_NANOS = 1505855799_433901068L;
  private static final long SENT_TIMESTAMP_NANOS = 1505855799_459486280L;
  private static final Map<String, AttributeValue> attributes = Collections.emptyMap();
  private static final List<SpanDataImpl.TimedEvent> annotations =
      ImmutableList.of(
          SpanDataImpl.TimedEvent.create(
              RECEIVED_TIMESTAMP_NANOS, "RECEIVED", Collections.<String, AttributeValue>emptyMap()),
          SpanDataImpl.TimedEvent.create(
              SENT_TIMESTAMP_NANOS, "SENT", Collections.<String, AttributeValue>emptyMap()));

  private static final String ENDPOINT_V1_SPANS = "/api/v1/spans";
  private static final String ENDPOINT_V2_SPANS = "/api/v2/spans";
  private static final String SERVICE_NAME = "myService";
  private static final Endpoint localEndpoint =
      ZipkinSpanExporter.produceLocalEndpoint(SERVICE_NAME);

  @Rule public ZipkinRule zipkin = new ZipkinRule();

  @Test
  public void testExportWithDefaultEncoding() {

    ZipkinExporterConfiguration configuration =
        ZipkinExporterConfiguration.builder()
            .setEndpoint(zipkin.httpUrl() + ENDPOINT_V2_SPANS)
            .setServiceName(SERVICE_NAME)
            .build();

    exportAndVerify(configuration);
  }

  @Test
  public void testExportAsProtobuf() {

    ZipkinExporterConfiguration configuration =
        buildZipkinExporterConfiguration(
            zipkin.httpUrl() + ENDPOINT_V2_SPANS, Encoding.PROTO3, SpanBytesEncoder.PROTO3);
    exportAndVerify(configuration);
  }

  @Test
  public void testExportAsThrift() {

    @SuppressWarnings("deprecation")
    ZipkinExporterConfiguration configuration =
        buildZipkinExporterConfiguration(
            zipkin.httpUrl() + ENDPOINT_V1_SPANS, Encoding.THRIFT, SpanBytesEncoder.THRIFT);
    exportAndVerify(configuration);
  }

  @Test
  public void testExportAsJsonV1() {
    ZipkinExporterConfiguration configuration =
        buildZipkinExporterConfiguration(
            zipkin.httpUrl() + ENDPOINT_V1_SPANS, Encoding.JSON, SpanBytesEncoder.JSON_V1);
    exportAndVerify(configuration);
  }

  @Test
  public void testExportFailedAsWrongEncoderUsed() {

    ZipkinExporterConfiguration configuration =
        buildZipkinExporterConfiguration(
            zipkin.httpUrl() + ENDPOINT_V2_SPANS, Encoding.JSON, SpanBytesEncoder.PROTO3);

    ZipkinSpanExporter zipkinSpanExporter = ZipkinSpanExporter.create(configuration);

    SpanData spanData = buildStandardSpan().build();
    SpanExporter.ResultCode resultCode = zipkinSpanExporter.export(Collections.singleton(spanData));

    assertThat(resultCode).isEqualTo(SpanExporter.ResultCode.FAILURE);
    List<Span> zipkinSpans = zipkin.getTrace(TRACE_ID);
    assertThat(zipkinSpans).isNotNull();
    assertThat(zipkinSpans).isEmpty();
  }

  private static ZipkinExporterConfiguration buildZipkinExporterConfiguration(
      String endpoint, Encoding encoding, SpanBytesEncoder encoder) {
    return ZipkinExporterConfiguration.builder()
        .setSender(URLConnectionSender.newBuilder().endpoint(endpoint).encoding(encoding).build())
        .setServiceName(SERVICE_NAME)
        .setEncoder(encoder)
        .build();
  }

  /**
   * Exports a span, verify that it was received by Zipkin, and check that the span stored by Zipkin
   * matches what was sent.
   */
  private void exportAndVerify(ZipkinExporterConfiguration configuration) {
    ZipkinSpanExporter zipkinSpanExporter = ZipkinSpanExporter.create(configuration);

    SpanData spanData = buildStandardSpan().build();
    SpanExporter.ResultCode resultCode = zipkinSpanExporter.export(Collections.singleton(spanData));

    assertThat(resultCode).isEqualTo(SpanExporter.ResultCode.SUCCESS);
    List<Span> zipkinSpans = zipkin.getTrace(TRACE_ID);

    assertThat(zipkinSpans).isNotNull();
    assertThat(zipkinSpans.size()).isEqualTo(1);
    assertThat(zipkinSpans.get(0)).isEqualTo(buildZipkinSpan());
  }

  private static SpanDataImpl.Builder buildStandardSpan() {
    return SpanDataImpl.newBuilder()
        .setTraceId(TraceId.fromLowerBase16(TRACE_ID, 0))
        .setSpanId(SpanId.fromLowerBase16(SPAN_ID, 0))
        .setParentSpanId(SpanId.fromLowerBase16(PARENT_SPAN_ID, 0))
        .setTraceFlags(TraceFlags.builder().setIsSampled(true).build())
        .setStatus(Status.OK)
        .setKind(Kind.SERVER)
        .setHasRemoteParent(true)
        .setName(SPAN_NAME)
        .setStartEpochNanos(START_EPOCH_NANOS)
        .setAttributes(attributes)
        .setTotalAttributeCount(attributes.size())
        .setTimedEvents(annotations)
        .setLinks(Collections.<SpanDataImpl.Link>emptyList())
        .setEndEpochNanos(END_EPOCH_NANOS)
        .setHasEnded(true);
  }

  private static Span buildZipkinSpan() {
    return Span.newBuilder()
        .traceId(TRACE_ID)
        .parentId(PARENT_SPAN_ID)
        .id(SPAN_ID)
        .kind(Span.Kind.SERVER)
        .name(SPAN_NAME)
        .timestamp(START_EPOCH_NANOS / 1000)
        .duration((END_EPOCH_NANOS / 1000) - (START_EPOCH_NANOS / 1000))
        .localEndpoint(localEndpoint)
        .addAnnotation(RECEIVED_TIMESTAMP_NANOS / 1000, "RECEIVED")
        .addAnnotation(SENT_TIMESTAMP_NANOS / 1000, "SENT")
        .build();
  }
}
