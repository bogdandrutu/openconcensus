package io.opentelemetry.example;

import static io.restassured.RestAssured.given;

import io.restassured.http.ContentType;
import io.restassured.response.Response;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

@RunWith(JUnit4.class)
public class JaegerExampleIntegrationTest {

  private static final String ARCHIVE_NAME = System.getProperty("archive.name");
  private static final String APP_NAME = "jaeger-example-app.jar";

  private static final int QUERY_PORT = 16686;
  private static final int COLLECTOR_PORT = 14250;
  private static final String JAEGER_VERSION = "1.17";
  private static final String SERVICE_NAME = "example";
  private static final String JAEGER_HOSTNAME = "jaeger";
  private static final String JAEGER_URL = "http://localhost";

  private static final Network network = Network.newNetwork();

  @ClassRule
  public static GenericContainer jaegerContainer =
      new GenericContainer<>("jaegertracing/all-in-one:" + JAEGER_VERSION)
          .withNetwork(network)
          .withNetworkAliases(JAEGER_HOSTNAME)
          .withExposedPorts(COLLECTOR_PORT, QUERY_PORT)
          .waitingFor(new HttpWaitStrategy().forPath("/"));

  private static GenericContainer jaegerExampleAppContainer =
      new GenericContainer(buildJaegerExampleAppImage())
          .withNetwork(network)
          .waitingFor(Wait.forLogMessage(".*Bye.*", 1));

  private static ImageFromDockerfile buildJaegerExampleAppImage() {
    return new ImageFromDockerfile()
        .withFileFromFile(ARCHIVE_NAME, Paths.get(ARCHIVE_NAME).toFile())
        .withDockerfileFromBuilder(
            builder ->
                builder
                    .from("openjdk:7u111-jre-alpine")
                    .copy(ARCHIVE_NAME, "/app/" + APP_NAME)
                    .entryPoint(
                        "java",
                        "-cp",
                        "/app/" + APP_NAME,
                        "io.opentelemetry.example.JaegerExample",
                        JAEGER_HOSTNAME,
                        Integer.toString(COLLECTOR_PORT))
                    .build());
  }

  @Test
  public void testJaegerExampleAppIntegration() {
    jaegerExampleAppContainer.start();
    Awaitility.await().atMost(30, TimeUnit.SECONDS).until(assertJaegerHaveTrace());
  }

  private static Callable<Boolean> assertJaegerHaveTrace() {
    return () -> {
      try {
        String url =
            String.format(
                "%s/api/traces?service=%s",
                String.format(JAEGER_URL + ":%d", jaegerContainer.getMappedPort(QUERY_PORT)),
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
    };
  }
}
