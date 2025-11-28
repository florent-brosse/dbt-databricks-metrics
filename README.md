# dbt Databricks Metric Views Demo

A simple dbt project to test **Unity Catalog Metric Views** on Databricks.

Based on the proposal: https://github.com/databricks/dbt-databricks/issues/1106

## Requirements

- Databricks Runtime **16.4+** (version 0.1) or **17.2+** (version 1.1 with semantic metadata)
- Unity Catalog enabled
- dbt-databricks adapter (`pip install dbt-databricks`)

## Setup

### 1. Install uv (if not already installed)

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 2. Install Dependencies

```bash
uv sync
```

### 3. Configure Environment Variables

```bash
export DATABRICKS_HOST="your-workspace.cloud.databricks.com"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/your-warehouse-id"
export DATABRICKS_TOKEN="your-personal-access-token"
```

### 4. Copy Profile (or use environment)

Either copy `profiles.yml` to `~/.dbt/profiles.yml` or set:

```bash
export DBT_PROFILES_DIR=/path/to/this/project
```

### 5. Update Catalog/Schema

Edit `profiles.yml` to set your target catalog and schema:

```yaml
catalog: main  # Your Unity Catalog
schema: dbt_metrics_demo
```

## Project Structure

```
├── models/
│   ├── staging/
│   │   ├── stg_orders.sql       # TPC-H orders staging
│   │   └── stg_customers.sql    # TPC-H customers staging
│   └── marts/
│       ├── fct_orders.sql       # Order fact table (with metric view)
│       └── fct_customer_summary.sql  # Customer summary (with metric view)
├── macros/
│   ├── generate_metric_views.sql  # Creates metric views on-run-end
│   ├── drop_metric_views.sql      # Utility to drop metric views
└── schema.yml                     # Model definitions with metric_view metadata
```

## Usage

### Run the Project

```bash
# Install dbt packages
uv run dbt deps

# Run all models (metric views are created automatically via on-run-end hook)
uv run --env-file .env dbt run

# Run specific models
uv run --env-file .env dbt run --select fct_orders
```

### Manually Generate Metric Views

```bash
uv run dbt run-operation generate_metric_views
```

### Drop Metric Views

```bash
uv run dbt run-operation drop_metric_views
```

### Refresh Materialized Views (Databricks Workflow)

Use **Databricks Workflows** to orchestrate dbt + refresh as a 2-task pipeline:

```
┌─────────────┐     ┌──────────────────────────┐
│  Task 1:    │ --> │  Task 2:                 │
│  dbt run    │     │  refresh_metric_views.py │
└─────────────┘     └──────────────────────────┘
```

The refresh script triggers the underlying [Lakeflow Spark Declarative Pipeline](https://docs.databricks.com/aws/en/metric-views/materialization#manual-refresh):

```bash
# Discover and refresh metric views
uv run --env-file .env python scripts/refresh_metric_views.py --refresh <catalog>.<schema>.mv_order_metrics

# Or by pipeline ID directly
uv run --env-file .env python scripts/refresh_metric_views.py <pipeline_id>
```

You can also trigger refresh from the **Databricks UI** via the pipeline link in Catalog Explorer.

> **Note:** Materialization uses incremental refresh whenever possible. See [Incremental refresh for materialized views](https://docs.databricks.com/gcp/en/optimizations/incremental-refresh).

## Querying Metric Views

Once created, query the metric views using the `MEASURE()` function:

```sql
-- Query order metrics
SELECT 
    market_segment,
    MEASURE(total_orders),
    MEASURE(total_revenue),
    MEASURE(avg_order_value)
FROM <catalog>.<schema>.mv_order_metrics
GROUP BY market_segment;

-- Query customer metrics
SELECT 
    market_segment,
    MEASURE(total_customers),
    MEASURE(avg_customer_value)
FROM <catalog>.<schema>.mv_customer_metrics
GROUP BY market_segment;
```

## Metric View Configuration

The macro supports three modes:

### Mode Overview

| Mode | Version | Use Case |
|------|---------|----------|
| **Structured** | 0.1 | Simple metrics, no YAML knowledge needed |
| **Raw YAML v0.1** | 0.1 | Materialization, window measures |
| **Raw YAML v1.1** | 1.1 | Semantic metadata (AI/LLM friendly) |

### Mode 1: Raw YAML (Recommended)

Paste the [official Databricks YAML syntax](https://docs.databricks.com/aws/en/metric-views/data-modeling/syntax) directly. Use `__SOURCE__` as a placeholder for the table reference (auto-replaced with `catalog.schema.table`).

```yaml
models:
  - name: fct_orders
    meta:
      metric_view:
        enabled: true
        name: mv_order_metrics
        description: "Order KPIs"
        yaml: |
          version: 1.1
          source: __SOURCE__
          
          dimensions:
            - name: market_segment
              expr: market_segment
              display_name: "Market Segment"
              synonyms:
                - "segment"
          
          measures:
            - name: total_revenue
              expr: sum(total_price)
              display_name: "Total Revenue"
              format:
                type: currency
                currency_code: USD
            
            # Window measure example
            - name: trailing_7d_revenue
              expr: sum(total_price)
              window:
                - order: order_date
                  range: trailing 7 day
                  semiadditive: last
```

**Benefits:**
- Full control over YAML syntax
- Supports all Databricks features (window measures, joins, semantic metadata)
- Copy examples directly from [Databricks documentation](https://docs.databricks.com/aws/en/metric-views/data-modeling/syntax)

### Mode 2: Structured (Simple cases only)

For basic metric views without advanced features:

```yaml
models:
  - name: my_model
    meta:
      metric_view:
        enabled: true
        name: mv_my_metrics
        description: "Simple metrics"
        dimensions:
          - name: category
            expr: category
        measures:
          - name: total_count
            expr: count(*)
          - name: total_amount
            expr: sum(amount)
```

## Databricks YAML Reference

### Version 1.1 Features (Runtime 17.2+)

```yaml
version: 1.1
source: catalog.schema.table

dimensions:
  - name: order_date
    expr: o_orderdate
    display_name: "Order Date"
    comment: "Date of the order"
    synonyms:
      - "date"
      - "order time"

measures:
  - name: total_revenue
    expr: SUM(o_totalprice)
    display_name: "Total Revenue"
    format:
      type: currency
      currency_code: USD
```

### Window Measures

```yaml
measures:
  # Trailing window
  - name: trailing_7d_revenue
    expr: sum(total_price)
    window:
      - order: order_date
        range: trailing 7 day
        semiadditive: last

  # Cumulative (running total)
  - name: cumulative_revenue
    expr: sum(total_price)
    window:
      - order: order_date
        range: cumulative
        semiadditive: last

  # Year-to-date
  - name: ytd_revenue
    expr: sum(total_price)
    window:
      - order: order_date
        range: cumulative
        semiadditive: last
      - order: order_year
        range: current
        semiadditive: last
```

### Joins

```yaml
version: 1.1
source: catalog.schema.orders

joins:
  - name: customer
    source: catalog.schema.customer
    on: source.customer_id = customer.id
    joins:
      - name: nation
        source: catalog.schema.nation
        on: customer.nation_id = nation.id

dimensions:
  - name: customer_name
    expr: customer.name
  - name: nation_name
    expr: customer.nation.name
```

### Materialization (Pre-computed Performance)

> **Note:** Materialization requires **version 0.1** (not compatible with version 1.1 semantic metadata).

Pre-compute aggregations for faster queries:

```yaml
version: 0.1
source: __SOURCE__

dimensions:
  - name: market_segment
    expr: market_segment
  - name: order_year
    expr: order_year
  - name: order_month
    expr: order_month

measures:
  - name: total_revenue
    expr: sum(total_price)
  - name: total_orders
    expr: count(*)

# Materialization configuration
materialization:
  schedule: every 6 hours
  mode: relaxed
  
  materialized_views:
    # Cache raw data (fallback for all queries)
    - name: baseline
      type: unaggregated
    
    # Pre-aggregate by segment (10-100x faster)
    - name: revenue_by_segment
      type: aggregated
      dimensions:
        - market_segment
      measures:
        - total_revenue
        - total_orders
    
    # Pre-aggregate monthly (10-100x faster)
    - name: monthly_revenue
      type: aggregated
      dimensions:
        - order_year
        - order_month
      measures:
        - total_revenue
```

#### Materialization Options

| Option | Values | Description |
|--------|--------|-------------|
| `schedule` | `every N hours/minutes` | Refresh frequency (omit for manual-only) |
| `mode` | `relaxed`, `strict` | `relaxed` allows stale data during refresh |
| `type` | `unaggregated`, `aggregated` | Cache raw data or pre-compute aggregations |

#### Primarily Manual Refresh (Long Schedule)

Databricks **requires** a `schedule` for materialization. Use a very long interval (e.g., 8 weeks) and rely on manual refresh after dbt updates:

```yaml
materialization:
  schedule: every 8 weeks  # Required - use long interval for manual-only workflow
  mode: relaxed
  
  materialized_views:
    - name: baseline
      type: unaggregated
```

Then refresh manually after dbt updates source tables:

```bash
uv run --env-file .env python scripts/refresh_metric_views.py --refresh <catalog>.<schema>.mv_order_metrics
```

#### Verify Materialization

```sql
-- Check materialization properties
SHOW TBLPROPERTIES catalog.schema.mv_order_metrics;

-- View detailed info
DESCRIBE EXTENDED catalog.schema.mv_order_metrics;
```

Expected output:
```
metric_view.materialization.mode = relaxed
metric_view.materialization.schedule = {"type":"periodic","interval":6,"unit":"HOURS"}
```

#### Test Queries

```sql
-- Query using pre-aggregated view (fast - reads ~5 rows)
SELECT 
    market_segment,
    MEASURE(total_revenue),
    MEASURE(total_orders)
FROM catalog.schema.mv_order_metrics
GROUP BY market_segment;

-- Query using baseline cache (medium - reads cached data)
SELECT 
    order_priority,
    MEASURE(total_revenue)
FROM catalog.schema.mv_order_metrics
GROUP BY order_priority;

-- Check query plan to verify materialization usage
EXPLAIN
SELECT market_segment, MEASURE(total_revenue)
FROM catalog.schema.mv_order_metrics
GROUP BY market_segment;
```

#### Performance Comparison

| Query Type | Materialized View Used | Speed |
|------------|----------------------|-------|
| `GROUP BY market_segment` + `total_revenue` | `revenue_by_segment` | ⚡ 10-100x faster |
| `GROUP BY order_year, order_month` + `total_revenue` | `monthly_revenue` | ⚡ 10-100x faster |
| `GROUP BY order_priority` | `baseline` (fallback) | 🚀 ~1x (cached) |
| Window measures (`trailing_7d_revenue`) | Computed on-the-fly | 📊 Normal |

## Version Comparison

| Feature | Version 0.1 | Version 1.1 |
|---------|-------------|-------------|
| **Runtime** | 16.4+ | 17.2+ |
| **Materialization** | ✅ | ❌ |
| **Semantic metadata** | ❌ | ✅ |
| **Window measures** | ✅ | ✅ |
| **Joins** | ✅ | ✅ |

Choose based on your priority:
- **Performance** → Version 0.1 with materialization
- **AI/LLM discovery** → Version 1.1 with semantic metadata

## Notes

- Metric Views are currently in **Public Preview** on Databricks
- Version 0.1 requires Runtime 16.4+
- Version 1.1 (semantic metadata) requires Runtime 17.2+
- The `samples.tpch` catalog is available in all Databricks workspaces

## References

- [Metric Views YAML Syntax](https://docs.databricks.com/aws/en/metric-views/data-modeling/syntax)
- [Window Measures](https://docs.databricks.com/aws/en/metric-views/data-modeling/window-measures)
- [Joins in Metric Views](https://docs.databricks.com/aws/en/metric-views/data-modeling/joins)
- [Semantic Metadata](https://docs.databricks.com/aws/en/metric-views/data-modeling/semantic-metadata)
- [CREATE METRIC VIEW SQL Syntax](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-metric-view.html)
- [dbt-databricks Issue #1106](https://github.com/databricks/dbt-databricks/issues/1106)
