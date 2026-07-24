FROM clojure:tools-deps-alpine AS builder
WORKDIR /build

COPY deps.edn ./
COPY src/ ./src/

RUN clojure -Spath > classpath.txt

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app

COPY --from=builder /root/.m2 /root/.m2
COPY --from=builder /build ./


CMD ["sh", "-c", "java -cp $(cat classpath.txt) clojure.main -m pipeline.main data"]
