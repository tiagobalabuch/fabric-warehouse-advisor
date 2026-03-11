# Troubleshooting

Common issues and their solutions when running advisors from the
Fabric Warehouse Advisor framework.

## Installation Issues

### `ModuleNotFoundError: No module named 'fabric_warehouse_advisor'`

**Cause:** The wheel is not installed in the current Spark session.

**Solution:**

```python
# Option A: install from Lakehouse Files
# Replace 1.2.3 with the version of the wheel you downloaded
%pip install /lakehouse/default/Files/fabric_warehouse_advisor-1.2.3-py3-none-any.whl

# Option B: attach a Fabric Environment with the wheel pre-installed
```

After `%pip install`, you may need to restart the Spark session or
re-import the module.

### `ModuleNotFoundError: No module named 'pyspark'`

**Cause:** Running outside a Fabric Spark notebook (e.g., local Python).

**Solution:** The advisor is designed to run inside a Fabric Spark
notebook where PySpark is pre-installed. It cannot run locally without a
full PySpark + Fabric connector setup.

## Runtime Errors

### `RuntimeError: com.microsoft.spark.fabric is not available`

**Cause:** The Fabric Spark connector is not present in the runtime.

**Solution:** Ensure you're running in an actual Fabric Spark notebook
(Spark pool). The `com.microsoft.spark.fabric.Constants` package is
built into the Fabric runtime and is not available in open-source
PySpark.

### `ValueError: warehouse_name must be set`

**Cause:** Config was created without specifying the warehouse name.

**Solution:**

```python
config = DataClusteringConfig(
    warehouse_name="MyWarehouse",  # Required — must match exactly
)
```

### `ValueError: Score weights must sum to 100`

**Cause:** Custom score weights don't add up to 100.

**Solution:** Ensure `score_weight_table_size + score_weight_predicate_freq + score_weight_cardinality + score_weight_data_type == 100`.

## Data Issues

### No Tables Shown / Empty Results

**Possible causes:**

1. **All tables are below `min_row_count`** — Lower the threshold:
   ```python
   config = DataClusteringConfig(
       warehouse_name="MyWarehouse",
       min_row_count=100_000,  # default is 1,000,000
   )
   ```

2. **`table_names` filter is too restrictive** — Check spelling and
   schema qualification:
   ```python
   # These are equivalent:
   table_names=["Orders"]           # matches any schema
   table_names=["dbo.Orders"]       # matches dbo.Orders specifically
   ```

3. **Query Insights has no data yet** — Queries need to have run against
   the warehouse for Query Insights to collect patterns. Run some
   representative queries and wait a few minutes, then re-run the
   advisor.

### `[WARN] Could not count schema.table`

**Cause:** The `COUNT_BIG(*)` query failed for a specific table. This
can happen with very recently created tables or tables in an
inconsistent state.

**Impact:** The table is skipped (row count set to −1).

**Solution:** Usually transient — re-running the advisor resolves it.

### `[WARN] Could not read queryinsights.frequently_run_queries`

**Cause:** Query Insights is not enabled or the view is empty.

**Solution:** Query Insights is enabled by default on Fabric Warehouses.
If you see this warning:

1. Verify the warehouse is a Fabric Warehouse (not a Lakehouse SQL
   endpoint)
2. Run some queries directly against the warehouse
3. Wait a few minutes for Query Insights to populate
4. Re-run the advisor

### `[WARN] T-SQL cardinality failed for schema.table.column`

**Cause:** `APPROX_COUNT_DISTINCT` failed for a specific column (e.g.,
unsupported type, column doesn't exist).

**Impact:** The advisor falls back to reading through the Spark connector
(slower but functional). If both paths fail, the column gets
`Unknown` cardinality.

## Performance Issues

### Analysis Takes Too Long

The advisor is designed to be fast since all heavy computation runs
server-side via T-SQL. If it's slow:

1. **Too many tables** — Use `table_names` to scope the analysis:
   ```python
   config = DataClusteringConfig(
       warehouse_name="MyWarehouse",
       table_names=["dbo.FactSales", "dbo.FactOrders"],
   )
   ```

2. **Many cardinality estimations** — Phase 6 makes one T-SQL call per
   candidate column. For tables with many columns referenced in
   predicates, this can add up. The batch cardinality path (used for
   full-scan tables) mitigates this.

3. **Spark connector fallback** — If T-SQL passthrough fails, the
   advisor falls back to reading through Spark, which is significantly
   slower. Check the `[WARN]` messages in the output.

## Debugging

Enable verbose mode to see exactly what the advisor is doing at each
phase:

```python
config = DataClusteringConfig(
    warehouse_name="MyWarehouse",
    verbose=True,
)
```

This prints:
- Metadata overview (column count, table count)
- Current clustering configuration
- Row counts per table
- Full-scan query activity
- Predicate frequency per column
- Cardinality estimates with ratios and percentages
- Detailed score table

## Getting Help

If you encounter an issue not covered here:

1. Enable `verbose=True` and review the output
2. Check `[WARN]` messages — they often explain what went wrong
3. Open an issue on the GitHub repository with the verbose output
   (redacting any sensitive table/column names)

---

## Performance Check — Specific Issues

### Edition Detection Fails

**Cause:** `DATABASEPROPERTYEX(DB_NAME(), 'Edition')` returned an
unexpected value or threw an error.

**Impact:** The advisor defaults the edition to `"Unknown"` and some
checks (V-Order) may be skipped.

**Solution:** Ensure the Spark session has access to the connected
warehouse. This usually means running in a Fabric notebook with the
correct identity.

### Query Insights Not Populated (Caching Check)

**Cause:** The caching cold-start analysis queries
`queryinsights.exec_requests_history`, which may be empty if:

- No queries have been executed against the warehouse recently.
- Query Insights has not had time to populate (wait a few minutes).
- The connected item is a Lakehouse SQL Analytics Endpoint (Query
  Insights may not be available).

**Solution:**

1. Run some representative queries against the warehouse.
2. Wait a few minutes for Query Insights to flush.
3. Re-run the Performance Check advisor.

The advisor will report a `no_query_history` INFO finding if the
history is empty — this is informational, not an error.

### DBCC SHOW_STATISTICS Access Denied

**Cause:** The row-drift check uses `DBCC SHOW_STATISTICS` to read the
stats header. Some Fabric SKUs or permission levels may restrict this.

**Impact:** The row-drift sub-check is silently skipped for the affected
table. All other statistics checks continue normally.

**Solution:** Grant the notebook identity `VIEW DATABASE STATE`
permission, or accept that row-drift will not be reported.

### V-Order Flagged as CRITICAL

**Cause:** V-Order is disabled on the warehouse. This is flagged as
CRITICAL because disabling V-Order is **irreversible**.

**Solution:**

- If this is a **staging warehouse** (ETL ingestion only, no reporting),
  this is acceptable. The common pattern is: staging warehouse with
  V-Order OFF → reporting warehouse with V-Order ON.
- If this is a **reporting warehouse**, there is no SQL fix. You must
  create a new warehouse and reload data.

To suppress this finding when V-Order being off is intentional:

```python
config = PerformanceCheckConfig(
    warehouse_name="StagingWarehouse",
    vorder_warn_if_disabled=False,
)
```