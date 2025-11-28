#!/usr/bin/env python3
"""
Manually refresh metric view materializations.

This script triggers a refresh of the underlying Lakeflow Spark Declarative Pipelines
for metric views with materialization.

Usage:
    # Discover pipeline IDs for metric views
    uv run --env-file .env python scripts/refresh_metric_views.py --discover <catalog>.<schema>.mv_order_metrics
    
    # Refresh specific pipelines by ID
    uv run --env-file .env python scripts/refresh_metric_views.py <pipeline_id_1> <pipeline_id_2>
    
    # Discover and refresh in one go
    uv run --env-file .env python scripts/refresh_metric_views.py --refresh <catalog>.<schema>.mv_order_metrics

Environment:
    Set DATABRICKS_HOST and DATABRICKS_TOKEN in .env file, or use Databricks CLI profile.

See: https://docs.databricks.com/aws/en/metric-views/materialization#manual-refresh
"""

import sys
import re


def get_client():
    """Get Databricks workspace client."""
    try:
        from databricks.sdk import WorkspaceClient
        return WorkspaceClient()
    except ImportError:
        print("Error: databricks-sdk not installed.")
        print("Install with: pip install databricks-sdk")
        sys.exit(1)


def discover_pipeline(metric_view_name: str) -> str | None:
    """Get pipeline ID for a metric view using DESCRIBE EXTENDED."""
    client = get_client()
    
    print(f"Looking up pipeline for: {metric_view_name}")
    
    try:
        # Execute DESCRIBE EXTENDED to get pipeline info
        result = client.statement_execution.execute_statement(
            warehouse_id=_get_warehouse_id(client),
            statement=f"DESCRIBE EXTENDED {metric_view_name}",
            wait_timeout="30s"
        )
        
        if result.result and result.result.data_array:
            for row in result.result.data_array:
                # Look for pipeline URL in the output
                row_str = str(row)
                if "pipelines/" in row_str:
                    # Extract pipeline ID from URL
                    match = re.search(r'pipelines/([a-f0-9-]+)', row_str)
                    if match:
                        pipeline_id = match.group(1)
                        print(f"  Found pipeline ID: {pipeline_id}")
                        return pipeline_id
        
        print("  No pipeline found. Make sure the metric view has materialization enabled.")
        return None
        
    except Exception as e:
        print(f"  Error: {e}")
        print()
        print("  Alternatively, run this SQL in Databricks:")
        print(f"    DESCRIBE EXTENDED {metric_view_name};")
        print("  Look for 'Refresh information' section -> Pipeline URL contains the pipeline_id")
        return None


def _get_warehouse_id(client) -> str:
    """Get a SQL warehouse ID for executing queries."""
    import os
    
    # Try environment variable first
    warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID")
    if warehouse_id:
        return warehouse_id
    
    # Try to get from HTTP path (e.g., /sql/1.0/warehouses/abc123)
    http_path = os.environ.get("DATABRICKS_HTTP_PATH", "")
    match = re.search(r'/warehouses/([a-f0-9]+)', http_path)
    if match:
        return match.group(1)
    
    # List warehouses and use the first running one
    try:
        warehouses = list(client.warehouses.list())
        for wh in warehouses:
            if wh.state and wh.state.value == "RUNNING":
                return wh.id
        if warehouses:
            return warehouses[0].id
    except Exception:
        pass
    
    raise ValueError(
        "Could not determine warehouse ID. "
        "Set DATABRICKS_WAREHOUSE_ID or DATABRICKS_HTTP_PATH environment variable."
    )


def refresh_pipelines(pipeline_ids: list[str]):
    """Trigger refresh for given pipeline IDs."""
    client = get_client()
    
    for pipeline_id in pipeline_ids:
        print(f"Starting refresh for pipeline: {pipeline_id}")
        try:
            client.pipelines.start_update(pipeline_id)
            print(f"  ✓ Refresh started successfully")
        except Exception as e:
            print(f"  ✗ Error: {e}")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        print("\nExamples:")
        print("  # Discover pipeline ID")
        print("  uv run --env-file .env python scripts/refresh_metric_views.py --discover <catalog>.<schema>.mv_order_metrics")
        print()
        print("  # Refresh by pipeline ID")
        print("  uv run --env-file .env python scripts/refresh_metric_views.py 01484540-0a06-414a-b10f-e1b0e8097f15")
        print()
        print("  # Discover and refresh in one go")
        print("  uv run --env-file .env python scripts/refresh_metric_views.py --refresh <catalog>.<schema>.mv_order_metrics")
        sys.exit(1)
    
    if sys.argv[1] == "--discover":
        if len(sys.argv) < 3:
            print("Error: --discover requires a metric view name")
            sys.exit(1)
        for view_name in sys.argv[2:]:
            discover_pipeline(view_name)
            
    elif sys.argv[1] == "--refresh":
        if len(sys.argv) < 3:
            print("Error: --refresh requires metric view name(s)")
            sys.exit(1)
        pipeline_ids = []
        for view_name in sys.argv[2:]:
            pid = discover_pipeline(view_name)
            if pid:
                pipeline_ids.append(pid)
        if pipeline_ids:
            print()
            refresh_pipelines(pipeline_ids)
        else:
            print("No pipelines found to refresh.")
            sys.exit(1)
    else:
        # Assume arguments are pipeline IDs
        pipeline_ids = sys.argv[1:]
        refresh_pipelines(pipeline_ids)


if __name__ == "__main__":
    main()
