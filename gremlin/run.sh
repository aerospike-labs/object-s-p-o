#!/bin/sh
# Builds if needed and starts Gremlin Server on port 8182 over the vertex model.
# Needs Java 17 (set JAVA_HOME); Gremlin Server 3.7's Groovy doesn't run on newer JDKs.
set -e
cd "$(dirname "$0")"
[ -d target/lib ] || mvn -q -DskipTests package
JAVA="${JAVA_HOME:+$JAVA_HOME/bin/}java"
exec "$JAVA" -Dlogback.configurationFile=conf/logback.xml \
  -cp "target/lpg-gremlin-0.1.0.jar:target/lib/*" \
  org.apache.tinkerpop.gremlin.server.GremlinServer conf/gremlin-server.yaml
