# --- STAGE 1: Compilation Environment ---
FROM clojure:tools-deps-alpine AS builder
WORKDIR /build
COPY deps.edn ./
RUN clojure -P
COPY src/ ./src/
COPY scratchpad/ ./scratchpad/

# --- STAGE 2: Pristine Production Runtime ---
FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=builder /root/.m2 /root/.m2
COPY --from=builder /build ./
CMD ["java", "-cp", "src;.", "clojure.main", "-m", "pipeline.main", "data"]