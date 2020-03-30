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

package io.opentelemetry.exporters.jaeger;

import static io.restassured.RestAssured.given;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.opentelemetry.OpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.export.SimpleSpansProcessor;
import io.opentelemetry.trace.Span;
import io.opentelemetry.trace.Tracer;
import io.restassured.http.ContentType;
import io.restassured.response.Response;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;

@RunWith(JUnit4.class)
public class JaegerIntegrationTest {

  private static final int QUERY_PORT = 16686;
  private static final int COLLECTOR_PORT = 14250;
  private static final String JAEGER_VERSION = "1.17";
  private static final String SERVICE_NAME = "E2E-test";
  private static final String JAEGER_URL = "http://localhost";
  private final Tracer tracer =
      OpenTelemetry.getTracerProvider().get(getClass().getCanonicalName());
  private JaegerGrpcSpanExporter jaegerExporter;

  @SuppressWarnings("rawtypes")
  @ClassRule
  public static GenericContainer jaeger =
      new GenericContainer("jaegertracing/all-in-one:" + JAEGER_VERSION)
          .withExposedPorts(COLLECTOR_PORT, QUERY_PORT)
          .waitingFor(new HttpWaitStrategy().forPath("/"));

  @Before
  public void setupJaegerExporter() {
    ManagedChannel jaegerChannel =
        ManagedChannelBuilder.forAddress("127.0.0.1", jaeger.getMappedPort(COLLECTOR_PORT))
            .usePlaintext()
            .build();
    this.jaegerExporter =
        JaegerGrpcSpanExporter.newBuilder()
            .setServiceName(SERVICE_NAME)
            .setChannel(jaegerChannel)
            .setDeadlineMs(30000)
            .build();
    OpenTelemetrySdk.getTracerProvider()
        .addSpanProcessor(SimpleSpansProcessor.newBuilder(this.jaegerExporter).build());
  }

  @Test
  public void testJaegerIntegration() {
    imitateWork();
    Awaitility.await().atMost(30, TimeUnit.SECONDS).until(assertJaegerHaveTrace());
  }

  private void imitateWork() {
    Span span = this.tracer.spanBuilder("Test span").startSpan();
    span.addEvent("some event");
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    span.end();
  }

  private static Callable<Boolean> assertJaegerHaveTrace() {
    return new Callable<Boolean>() {
      @Override
      public Boolean call() {
        try {
          String url =
              String.format(
                  "%s/api/traces?service=%s",
                  String.format(JAEGER_URL + ":%d", jaeger.getMappedPort(QUERY_PORT)),
                  SERVICE_NAME);
          Response response =
              given()
                  .headers("Content-Type", ContentType.JSON, "Accept", ContentType.JSON)
                  .when()
                  .get(url)
                  .then()
                  .contentType(ContentType.JSON)
                  .extract()
                  .response();
          Map<String, String> path = response.jsonPath().getMap("data[0]");
          return path.get("traceID") != null;
        } catch (Exception e) {
          return false;
        }
      }
    };
  }
}
