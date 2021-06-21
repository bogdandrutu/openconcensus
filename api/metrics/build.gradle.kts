plugins {
    id("otel.java-conventions")
    id("maven-publish")

    id("otel.jmh-conventions")
    id("ru.vyarus.animalsniffer")
}

description = "OpenTelemetry API"
otelJava.moduleName.set("io.opentelemetry.api.metrics")

dependencies {
    api(project(":api:all"))

    annotationProcessor("com.google.auto.value:auto-value")

    testImplementation("edu.berkeley.cs.jqf:jqf-fuzz")
    testImplementation("com.google.guava:guava-testlib")
}
