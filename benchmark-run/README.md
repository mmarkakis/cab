# CAB Benchmark Runner

A set of JavaScript programs for running and analyzing the CAB benchmark on cloud data warehouses.

## Overview

The CAB benchmark follows these phases:

### Setup Phase (`load_setup.js`)
- Creates the top-level database (if it doesn't exist)
- Spins up N schemas or databases depending on tenant isolation strategy
- Creates all tables (TPC-H definitions) and file-format objects/stages for bulk loading from S3

### Data Load Phase (`load_worker.js`)
- For each tenant database/schema, generates scale-specific TPC-H datasets
- Stages data or streams directly from S3
- Copies data into tables (`COPY INTO`) in parallel

### Query Stream Generation
- The benchmark generator (CAB-gen) produces five patterns of query arrival times (cold start, spiky, uniform, etc.)
- Templates all 21 TPC-H queries
- Each tenant has its own stream of queries

### Query Execution Phase (`query_stream_executer.js`)
- Launches one JavaScript worker per tenant
- Opens a connection to its schema
- Submits queries at designated arrival times, respecting per-tenant rate (CAB factor)
- Measures per-query latency
- Writes results (timestamps, query IDs, latencies) to a local CSV file

### Aggregation Phase (`result_combinator.js`)
- Collects all per-tenant raw result files
- Merges them into a single CSV containing: tenant_id, query_id, start_time, end_time, latency_ms, etc.
- Produces summary statistics: p50, p90, p99 latencies; total run time; multi-tenant interference metrics

---

## Environment Variables

### AWS Credentials (required for all AWS services)
```bash
export AWS_ACCESS_KEY_ID='...'
export AWS_SECRET_ACCESS_KEY='...'
export AWS_REGION='us-east-1'  # Must include region number (e.g., us-east-1, not us-east)
```

### S3 (required for data loading)
```bash
export S3_BUCKET='my-bucket/cab-data'
```

---

## Athena Configuration

```bash
export ATHENA_S3_OUTPUT='s3://my-bucket/athena-output'  # S3 path for query results
export ATHENA_DATABASE='cab'                            # Glue/Athena database name
```

**Prerequisites:**
- Athena database must exist in AWS Glue Data Catalog
- Tables must be created (e.g., `lineitem_1`, `customer_1`, etc.)
- S3 bucket for query results must be accessible

---

## Redshift Configuration (Provisioned and Serverless)

```bash
export REDSHIFT_CLUSTER_IDENTIFIER='my-cluster'
export REDSHIFT_DATABASE='dev'
export REDSHIFT_ENDPOINT='my-cluster.xxxxx.us-east-1.redshift.amazonaws.com'
export REDSHIFT_DB_USER='admin'
export REDSHIFT_DB_PASSWORD='...'
export PORT='5439'
export SSL='true'
export REDSHIFT_ROLE='arn:aws:iam::123456789:role/RedshiftS3Access'
```

**Network Access:**

The easiest way to run a query stream is to:
1. Add your public IP address to the inbound rules of your security group (port 5439)
2. Enable public access on the Redshift cluster

Without public access, you'll need to configure appropriate IAM roles and VPC networking.
