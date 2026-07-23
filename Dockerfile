# --- Production Runtime Sandbox Environment ---
FROM eclipse-temurin:17-jre-alpine
WORKDIR /app

# Copy your local cache libraries and project sources straight into the sandbox
COPY .cpcache/ ./.cpcache/
COPY src/ ./src/
COPY target/classes/ ./target/classes/

# Run the compiled bytecode using a Linux colon path separator
CMD ["java", "-cp", "target/classes:src:.cpcache", "pipeline.main", "data"]