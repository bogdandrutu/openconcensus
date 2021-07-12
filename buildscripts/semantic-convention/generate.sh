#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR="${SCRIPT_DIR}/../../"

# freeze the spec & generator tools versions to make SemanticAttributes generation reproducible
SEMCONV_VERSION=1.5.0
SPEC_VERSION=v$SEMCONV_VERSION
SCHEMA_URL=https://opentelemetry.io/schemas/$SEMCONV_VERSION
GENERATOR_VERSION=0.4.1

cd ${SCRIPT_DIR}

rm -rf opentelemetry-specification || true
mkdir opentelemetry-specification
cd opentelemetry-specification

git init
git remote add origin https://github.com/open-telemetry/opentelemetry-specification.git
git fetch origin "$SPEC_VERSION"
git reset --hard FETCH_HEAD
cd ${SCRIPT_DIR}

docker run --rm \
  -v ${SCRIPT_DIR}/opentelemetry-specification/semantic_conventions/trace:/source \
  -v ${SCRIPT_DIR}/templates:/templates \
  -v ${ROOT_DIR}/semconv/src/main/java/io/opentelemetry/semconv/trace/attributes/:/output \
  otel/semconvgen:$GENERATOR_VERSION \
  -f /source code \
  --template /templates/SemanticAttributes.java.j2 \
  --output /output/SemanticAttributes.java \
  -Dclass=SemanticAttributes \
  -DschemaUrl=$SCHEMA_URL \
  -Dpkg=io.opentelemetry.semconv.trace.attributes

docker run --rm \
  -v ${SCRIPT_DIR}/opentelemetry-specification/semantic_conventions/resource:/source \
  -v ${SCRIPT_DIR}/templates:/templates \
  -v ${ROOT_DIR}/semconv/src/main/java/io/opentelemetry/semconv/resource/attributes/:/output \
  otel/semconvgen:$GENERATOR_VERSION \
  -f /source code \
  --template /templates/SemanticAttributes.java.j2 \
  --output /output/ResourceAttributes.java \
  -Dclass=ResourceAttributes \
  -DschemaUrl=$SCHEMA_URL \
  -Dpkg=io.opentelemetry.semconv.resource.attributes

cd "$ROOT_DIR"
./gradlew spotlessApply
