# High-Performance JVM Clojure Data Pipeline 🚀

An enterprise-grade, fault-tolerant telemetry data ingestion pipeline built natively on the Java Virtual Machine using Clojure.

## 🛠️ Architecture & Tech Stack
- **Runtime Environment:** Java Virtual Machine (JVM Clojure 1.11+)
- **Database Engine:** Persistent SQLite via Modern `next.jdbc`
- **Data Verification:** Global Contract Enforcement via `clojure.spec.alpha`
- **Data Ingestion:** High-speed streaming via `clojure.data.csv`

## 🛡️ Core Systems Plumbing Features
1. **Defensive Transaction Circuit Breakers:** Wraps database inserts inside strict multi-row transaction blocks (`with-transaction`). Any corrupted row trips an immediate rollback (`throw ex-info`), leaving storage 100% pristine.
2. **Object Normalization Layer:** Intercepts live JVM `#uuid` memory object tagged literals and flattens them into relational-safe string primitives to avoid driver serialization crashes.
3. **Regex Ingestion Sanitization:** Implements robust regular expression pattern filtering (`#"\s+"`) to strip trailing whitespace and hidden characters, completely eliminating `NumberFormatException` runtime failures.
4. **Generative Stress-Testing:** Leverages `clojure.spec.gen.alpha` to dynamically manufacture thousands of randomized, structurally perfect testing maps to load-test pipeline boundary states.