**Benchmark AWS Redshift**

1. Overview of required steps
The CAB benchmark follows roughly these phases:

**Setup phase (`load_setup.js`):**
- Creates the top‐level database (if it doesn’t exist).
- Spins up N schemas or N databases depending on how you want to isolate tenants.
- Creates all tables (TPC-H table definitions) and any necessary file‐format objects/stages for bulk loading from S3.

**Data‐load phase (`load_worker.js`):**
- For each “tenant” database (or schema), generates the scale-specific TPC-H datasets.
- Stages the data or directly streams from S3.
- Copies data into tables (`COPY INTO`) in parallel.

**Query‐stream generation (built into common scripts):**
- The benchmark generator (CAB‐gen) produces five “patterns” of query arrival times (cold start, spiky, uniform, etc.) and templates all 21 TPC-H queries.
- Each tenant (schema/database) has its own “stream” of queries.

**Query‐execution phase (`query_stream_executer.js` or similar):**
- Launches one JavaScript worker per tenant. Each worker:
  - Opens a connection to its schema.
  - Submits queries at the designated arrival times, respecting per-tenant rate (CAB factor).
  - Measures per-query latency.
  - Writes results (timestamps, query IDs, latencies) to a local JSON/CSV file.


## Environment Variables

### AWS Credentials (required for all AWS services)
```bash
export AWS_ACCESS_KEY_ID=''
export AWS_SECRET_ACCESS_KEY=''
export AWS_REGION=''
```

### S3 (required for data loading)
```bash
export S3_BUCKET=''  # e.g., 'my-bucket/cab-data'
```

### Athena
```bash
export ATHENA_S3_OUTPUT=''  # S3 path for query results, e.g., 's3://my-bucket/athena-output'
export ATHENA_DATABASE=''   # Athena database name
```

### Redshift
```bash
export REDSHIFT_DATABASE=''
export REDSHIFT_ENDPOINT=''
export REDSHIFT_DB_USER=''
export REDSHIFT_DB_PASSWORD=''
export PORT=''
export SSL=''
```

You might also need to add your public IP address to the allowed incoming connections of your security group.

**Aggregation phase (`result_combinator.js`):**
- Collects all per-tenant “raw result” files.
- Merges them into a single CSV that contains: tenant_id, query_id, start_time, end_time, latency_ms, warehouse_size, etc.
- Produces summary statistics: p50, p90, p99 latencies; total run time; average CPU credits used; multi-tenant interference metrics; etc.
