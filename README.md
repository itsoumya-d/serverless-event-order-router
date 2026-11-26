# Serverless Event Order Router

A high-performance serverless event stream dispatcher and idempotent order sequencer designed for high-concurrency e-commerce event architectures (AWS Lambda, SQS FIFO, DynamoDB streams).

## Architecture

- **Idempotency Guard**: Deterministic SHA-256 deduplication window ensuring exactly-once processing during serverless fan-out retries.
- **Strict Event Sequencer**: Enforces linear order state transitions (`CREATED` → `PAID` → `RESERVED` → `FULFILLED`), routing out-of-order anomalies to a dead-letter buffer.
- **Zero Allocations & Concurrency**: Thread-safe in-memory state evaluation optimized for sub-millisecond execution times in serverless runtimes.

## Usage

```bash
go test -v ./...
```
