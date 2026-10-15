#!/usr/bin/env bash
set -euo pipefail
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
build=$(mktemp -d)
trap 'rm -rf -- "$build"' EXIT
"${MVN:-mvn}" -B -ntp -f "$repo/pixels-common/pom.xml" \
    org.apache.maven.plugins:maven-dependency-plugin:2.10:build-classpath \
    -DincludeArtifactIds=protobuf-java -Dmdep.outputFile="$build/dependencies.cp" \
    > "$build/dependencies.log" 2>&1 || { cat "$build/dependencies.log" >&2; exit 1; }
dependencies=$(tr -d '\n' < "$build/dependencies.cp")
common="$repo/pixels-common/src/main/java/io/pixelsdb/pixels/common/ingest"
retina="$repo/pixels-retina/src/main/java/io/pixelsdb/pixels/retina/ingest"
tests="$repo/pixels-retina/src/test/java/io/pixelsdb/pixels/retina/ingest"
javac_args=(-source 8 -target 8)
if [[ $(javac -version 2>&1) != 'javac 1.8'* ]]; then
    javac_args=(--release 8)
fi
javac "${javac_args[@]}" -cp "$dependencies" -Xlint:all -Xlint:-options -Werror -d "$build" \
    "$common"/*.java "$retina/LocalMutationJournal.java" \
    "$tests/LocalMutationJournalContract.java" "$tests/LocalMutationJournalGcContract.java"
java -cp "$build:$dependencies" io.pixelsdb.pixels.retina.ingest.LocalMutationJournalContract
java -cp "$build:$dependencies" io.pixelsdb.pixels.retina.ingest.LocalMutationJournalGcContract
