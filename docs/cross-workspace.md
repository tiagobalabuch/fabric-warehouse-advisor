# Cross-Workspace Usage

By default, the advisor analyses a warehouse in the **same workspace**
where your Fabric Spark notebook is running. To target a warehouse in a
**different workspace**, you need to provide two additional identifiers.

## Required Parameters

Both IDs are GUIDs that you can find directly in the browser URL when
viewing the item. The URL format is:

```
https://app.fabric.microsoft.com/groups/{WorkspaceID}/warehouses/{WarehouseID}
```

| Parameter | Where to Find |
|-----------|---------------|
| `workspace_id` | The GUID after `/groups/` in the URL when you are in the workspace |
| `warehouse_id` | The GUID after `/warehouses/` in the URL when the warehouse is open |
| `sql_endpoint_id` | The GUID after `/mirroredwarehouses/` in the URL when the SQL Analytics Endpoint is open |

!!! tip "Choose warehouse_id or sql_endpoint_id"
    You must specify either warehouse_id or sql_endpoint_id, depending on where you want the advisor to run. Use `warehouse_id` to run the advisor against a **Warehouse**. Use `sql_endpoint_id` to run the advisor against a **SQL Analytics Endpoint**

## Configuration

Both advisors accept the same cross-workspace parameters.

=== "Data Clustering"

    ```python
    from fabric_warehouse_advisor import DataClusteringAdvisor, DataClusteringConfig

    config = DataClusteringConfig(
        warehouse_name="TargetWarehouse",

        # Cross-workspace: both are required together
        workspace_id="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
        warehouse_id="yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy",
        # sql_endpoint_id="yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy",
    )

    advisor = DataClusteringAdvisor(spark, config)
    result = advisor.run()
    ```

=== "Performance Check"

    ```python
    from fabric_warehouse_advisor import PerformanceCheckAdvisor, PerformanceCheckConfig

    config = PerformanceCheckConfig(
        warehouse_name="TargetWarehouse",

        # Cross-workspace: both are required together
        workspace_id="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
        warehouse_id="yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy",
        # sql_endpoint_id="yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy",
    )

    advisor = PerformanceCheckAdvisor(spark, config)
    result = advisor.run()
    ```

## How It Works

Under the hood, the advisor passes these IDs as Spark reader options:

```python
reader = spark.read \
    .option(Constants.DatabaseName, warehouse_name) \
    .option(Constants.WorkspaceId, workspace_id) \
    .option(Constants.DatawarehouseId, warehouse_id)
```

Where `Constants` comes from `com.microsoft.spark.fabric.Constants` —
the Fabric Spark runtime's built-in connector.

## Requirements

- The identity running the notebook (your Entra ID / service principal)
  must have **at least Read access** on the target warehouse
- The target warehouse / sql endpoint must be in the same Fabric tenant
- Both `workspace_id` and either `warehouse_id` or `sql_endpoint_id` must be specified together — providing only one will not work

## Same-Workspace Usage

When running in the same workspace, simply omit both parameters:

```python
config = DataClusteringConfig(
    warehouse_name="MyWarehouse",
    # No workspace_id or warehouse_id needed
)
```

The Fabric Spark connector automatically resolves the warehouse in the
current workspace.

## Troubleshooting

| Symptom | Cause | Solution |
|---------|-------|----------|
| `RuntimeError: com.microsoft.spark.fabric is not available` | Running outside Fabric Spark | Must run in a Fabric Spark notebook |
| Permission error | Insufficient access on target warehouse | Grant Read access on the target warehouse to the notebook identity |
| Warehouse not found | Incorrect `warehouse_name` or `warehouse_id` | Double-check the name matches exactly (case-sensitive) and the GUID is correct |
| Empty results | Incorrect `workspace_id` | Verify the workspace GUID — it's the workspace containing the target warehouse, not the notebook's workspace |
