# MongoDBee Grafana Dashboard 🐝📊

A ready-to-import Grafana dashboard for monitoring MongoDBee, built on the
OpenTelemetry tracing described in [TELEMETRY.md](../TELEMETRY.md).

MongoDBee emits **traces only** — no metrics. The dashboard is therefore driven
by *span metrics*: RED metrics (Rate, Errors, Duration) derived from MongoDBee
spans by the OpenTelemetry Collector's `spanmetrics` connector. Every span
attribute MongoDBee emits is PII-safe and low-cardinality by design, which is
exactly what makes them usable as Prometheus labels.

```
application (OTel SDK, OTLP export)
     │ traces
     ▼
OTel Collector ──── spanmetrics ────► Prometheus ──► Grafana (this dashboard)
     │ traces
     ▼
   Tempo (optional, trace drill-down via TraceQL)
```

## Files

| File | Purpose |
|------|---------|
| `mongodbee-overview.json` | The dashboard — import it into Grafana |
| `otel-collector.example.yaml` | Collector config deriving the metrics the dashboard expects |

## Quick start

1. **Enable telemetry** on your collections (see [TELEMETRY.md](../TELEMETRY.md)):

   ```typescript
   const users = await collection(db, "users", schema, {
     telemetry: { enabled: true },
   });
   ```

2. **Run an OTel Collector** with `otel-collector.example.yaml` (adapt the
   Tempo exporter or remove it), and point your application's OTLP exporter at
   it (`http://<collector>:4318`).

3. **Scrape the collector** from Prometheus:

   ```yaml
   scrape_configs:
     - job_name: otel-spanmetrics
       static_configs:
         - targets: ["<collector>:8889"]
   ```

4. **Import the dashboard**: Grafana → Dashboards → New → Import → upload
   `mongodbee-overview.json`, then pick your Prometheus data source in the
   `Data source` variable.

## What you get

| Row | Panels |
|-----|--------|
| **Overview** | Operations/s, error rate, p95 latency, transaction abort rate (stat tiles with sparklines) |
| **Latency** | p50/p95/p99 quantiles, bucket heatmap |
| **Throughput** | Rate by operation, rate by collection, top 10 slowest operations (p95) |
| **Errors** | Errors/s by `error.type` (spot `ValiError` = client-side validation), error ratio by collection |
| **Transactions** | Rate by outcome (committed/aborted), transaction duration quantiles |
| **Document types** | Rate and p95 by `mongodbee.doc_type` (multi-collection / scoped) |

Dashboard variables: `Data source`, `Service`, `Database`, `Collection`
(multi-select, default All). The error-rate and latency thresholds on the stat
tiles are generic defaults — tune them to your SLOs.

## Attribute → label mapping

The `spanmetrics` connector turns span attributes into Prometheus labels
(dots become underscores):

| MongoDBee span attribute | Prometheus label |
|--------------------------|------------------|
| `db.system.name` | `db_system_name` (used to isolate MongoDBee spans) |
| `db.namespace` | `db_namespace` |
| `db.collection.name` | `db_collection_name` |
| `db.operation.name` | `db_operation_name` |
| `error.type` | `error_type` |
| `mongodbee.doc_type` | `mongodbee_doc_type` |
| `mongodbee.transaction.outcome` | `mongodbee_transaction_outcome` |
| — (span name, built-in) | `span_name` (`"insertOne users"`, `"mongodb.transaction"`) |
| — (span status, built-in) | `status_code` (`STATUS_CODE_ERROR`, ...) |

`mongodbee.scope` is intentionally **not** a metric dimension: per-tenant label
values can explode Prometheus cardinality (and may be sensitive). Scope stays a
span-level attribute — analyze it in Tempo (below), or uncomment the dimension
in the collector config only if your scope set is small and non-PII.

Numeric attributes (`mongodbee.retry.count`, result counts, batch sizes) are
span-level too: they are per-operation measurements, not dimensions. Use the
TraceQL queries below for retry analysis.

## Using Grafana Cloud / Tempo metrics-generator instead

Tempo's metrics-generator produces the same span metrics under different names:
`traces_spanmetrics_calls_total` and `traces_spanmetrics_latency_*`
(milliseconds) instead of `traces_span_metrics_calls_total` and
`traces_span_metrics_duration_seconds_*`. If you use it instead of the
collector connector, search-and-replace the metric names in the dashboard JSON
and switch the latency panels' unit from `s` to `ms`. You must also add the
`mongodbee.*` / `db.*` attributes to the metrics-generator's
`dimensions` configuration.

## TraceQL cookbook (Tempo)

With traces flowing to Tempo, the span-level attributes enable queries the
metrics can't answer:

```traceql
# Slow MongoDBee operations
{ .db.system.name = "mongodb" && duration > 500ms }

# Operations that hit write-conflict retries
{ .db.system.name = "mongodb" && .mongodbee.retry.count > 0 }

# Transactions that aborted, with their inner operations
{ name = "mongodb.transaction" && .mongodbee.transaction.outcome = "aborted" }

# Potential cross-tenant reads: multi-collection queries issued without a
# scoped view and whose filter has no _scope key (see TELEMETRY.md)
{ .db.system.name = "mongodb" && .mongodbee.scope = nil && .mongodbee.filter.keys !~ ".*_scope.*" }

# Validation failures on a given collection
{ .db.collection.name = "users" && .error.type = "ValiError" }
```

## Alerting starting points

- `error rate > 5%` for 5 min — the Overview panel's red threshold.
- `transaction abort rate > 20%` for 5 min — sustained aborts usually mean
  write contention; correlate with `mongodbee.retry.count` traces.
- `p95 > 1s` for 10 min on a collection that normally serves reads.
