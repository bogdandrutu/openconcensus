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

package io.opentelemetry.example;

import io.opentelemetry.OpenTelemetry;
import io.opentelemetry.common.Labels;
import io.opentelemetry.context.Scope;
import io.opentelemetry.exporters.otlp.OtlpGrpcMetricExporter;
import io.opentelemetry.exporters.otlp.OtlpGrpcSpanExporter;
import io.opentelemetry.metrics.LongCounter;
import io.opentelemetry.metrics.Meter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.export.IntervalMetricReader;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.trace.Span;
import io.opentelemetry.trace.Tracer;
import java.util.Collections;

/**
 * Example code for setting up the OTLP exporters.
 *
 * <p>If you wish to use this code, you'll need to run a copy of the collector locally, on the
 * default port. There is a docker-compose configuration for doing this in the docker subdirectory
 * of this module.
 */
public class OtlpExporterExample {
  public static void main(String[] args) throws InterruptedException {
    // this will make sure that a proper service.name attribute is set on all the spans/metrics.
    // note: this is not something you should generally do in code, but should be provided on the
    // command-line. This is here to make the example more self-contained.
    System.setProperty("otel.resource.attributes", "service.name=OtlpExporterExample");

    // set up the span exporter and wire it into the SDK
    OtlpGrpcSpanExporter spanExporter = OtlpGrpcSpanExporter.getDefault();
    BatchSpanProcessor spanProcessor =
        BatchSpanProcessor.newBuilder(spanExporter).setScheduleDelayMillis(100).build();
    OpenTelemetrySdk.getTracerProvider().addSpanProcessor(spanProcessor);

    // set up the metric exporter and wire it into the SDK and a timed reader.
    OtlpGrpcMetricExporter metricExporter = OtlpGrpcMetricExporter.getDefault();
    IntervalMetricReader intervalMetricReader =
        IntervalMetricReader.builder()
            .setMetricExporter(metricExporter)
            .setMetricProducers(
                Collections.singleton(OpenTelemetrySdk.getMeterProvider().getMetricProducer()))
            .setExportIntervalMillis(500)
            .build();

    Tracer tracer = OpenTelemetry.getTracer("io.opentelemetry.example");
    Meter meter = OpenTelemetry.getMeter("io.opentelemetry.example");
    LongCounter counter = meter.longCounterBuilder("example_counter").build();

    for (int i = 0; i < 10; i++) {
      Span exampleSpan = tracer.spanBuilder("exampleSpan").startSpan();
      try (Scope scope = tracer.withSpan(exampleSpan)) {
        counter.add(1);
        exampleSpan.setAttribute("good", "true");
        exampleSpan.setAttribute("exampleNumber", i);
        Thread.sleep(100);
      } finally {
        exampleSpan.end();
      }
    }

    // sleep for a bit to let everything settle
    Thread.sleep(2000);

    OpenTelemetrySdk.getTracerProvider().shutdown();
    intervalMetricReader.shutdown();
  }
}
