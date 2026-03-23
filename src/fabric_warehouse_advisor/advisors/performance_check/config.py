"""
Fabric Warehouse Advisor — Performance Check Configuration
============================================================
All configuration values are exposed as fields of the
``PerformanceCheckConfig`` dataclass with sensible defaults.

Users create a config instance, override what they need, and pass
it to :class:`PerformanceCheckAdvisor`.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class PerformanceCheckConfig:
    """Configuration for the Performance Check Advisor.

    Parameters
    ----------
    warehouse_name : str
        The Fabric Warehouse or Lakehouse SQL Endpoint name.
        **Required** — there is no valid default.

    workspace_id : str
        Optional.  The Fabric Workspace ID (GUID) for cross-workspace
        access.  When empty the advisor assumes same-workspace.

    warehouse_id : str
        Optional.  The Fabric Warehouse item ID (GUID).
        For cross-workspace access together with ``workspace_id``.

    sql_endpoint_id : str
        Optional.  The Fabric SQL Endpoint item ID (GUID).
        For cross-workspace access to a Lakehouse SQL Analytics Endpoint.

    schema_names : list[str]
        Optional list of schemas to restrict analysis to.
        Empty means all user schemas.

    table_names : list[str]
        Optional list of tables to analyse.  Each entry can be
        ``"table_name"`` (any schema) or ``"schema.table_name"``.
        Empty means all tables.



    check_data_types : bool
        Enable the data type analysis check.

    check_caching : bool
        Enable the caching analysis check.

    check_statistics : bool
        Enable the statistics health check.

    check_vorder : bool
        Enable the V-Order state check.
        Auto-skipped for LakeWarehouse (SQL Analytics Endpoint).

    check_collation : bool
        Enable the collation mismatch check.
        Flags columns whose collation differs from the database default.

    varchar_max_warning : bool
        Flag ``VARCHAR(MAX)`` columns.

    char_to_varchar_warning : bool
        Flag ``CHAR(n)`` columns where ``VARCHAR(n)`` may be better.

    nullable_warning : bool
        Flag nullable columns that could potentially be ``NOT NULL``.

    oversized_varchar_threshold : int
        Flag ``VARCHAR(n)`` columns with max_length at or above this
        value.

    decimal_over_precision_threshold : int
        Flag ``DECIMAL`` / ``NUMERIC`` columns with precision above
        this value.

    datetime_as_string_warning : bool
        Flag string columns whose names suggest they store dates
        (e.g. ``*_date``, ``*_time``, ``*_dt``).

    float_for_money_warning : bool
        Flag ``FLOAT`` / ``REAL`` columns whose names suggest they
        store monetary amounts (e.g. ``*_amount``, ``*_price``).

    bigint_where_int_suffices_warning : bool
        Flag ``BIGINT`` columns whose names suggest small-range values
        (e.g. ``*_year``, ``*_month``, ``*_day``).

    result_cache_check : bool
        Check whether result set caching is enabled.

    cold_start_analysis : bool
        Analyse ``data_scanned_remote_storage_mb`` from query insights.

    cold_start_lookback_hours : int
        How many hours back in ``exec_requests_history`` to look
        for cold-start evidence.

    cache_hit_ratio_warning_threshold : float
        Warn if the result-set cache hit ratio is below this fraction.

    stale_stats_threshold_days : int
        Warn if statistics are older than this many days.

    row_drift_pct_threshold : float
        Warn if the actual row count differs from the stats row count
        by more than this percentage.

    proactive_refresh_check : bool
        Check if proactive statistics refresh is enabled.

    tables_without_stats_check : bool
        Check for large tables that have no user or auto statistics.

    vorder_warn_if_disabled : bool
        Warn if V-Order is disabled on the warehouse.

    check_query_regression : bool
        Enable the query regression detection check.
        Compares recent query performance against a historical baseline
        within the 30-day Query Insights window.

    regression_lookback_days : int
        Number of days that define the "recent" window. The baseline
        is the period from 30 days ago up to this many days ago.

    regression_factor_warning : float
        Flag a query as WARNING when its recent median is at least
        this many times the baseline median.

    regression_factor_critical : float
        Flag a query as CRITICAL when its recent median is at least
        this many times the baseline median.

    regression_min_executions : int
        Minimum number of executions a query must have in **both**
        baseline and recent windows to be considered.

    verbose : bool
        If ``True``, print intermediate details for debugging.

    phase_delay : float
        Seconds to pause between phases to reduce HTTP 429 throttling
        from the Fabric control-plane API.  Set to ``0`` to disable.
    """

    # -- Connection --
    warehouse_name: str = ""
    workspace_id: str = ""
    warehouse_id: str = ""
    sql_endpoint_id: str = ""

    # -- REST API (for workspace metadata) --
    fabric_token: str = ""
    use_notebook_token: bool = True

    # -- Scope filtering --
    schema_names: list[str] = field(default_factory=list)
    table_names: list[str] = field(default_factory=list)

    # -- Toggle check categories --
    check_data_types: bool = True
    check_caching: bool = True
    check_statistics: bool = True
    check_vorder: bool = True
    check_collation: bool = True

    # -- Data Type thresholds --
    varchar_max_warning: bool = True
    char_to_varchar_warning: bool = True
    nullable_warning: bool = True
    oversized_varchar_threshold: int = 8000
    decimal_over_precision_threshold: int = 18
    datetime_as_string_warning: bool = True
    float_for_money_warning: bool = True
    bigint_where_int_suffices_warning: bool = True

    # -- Caching settings --
    result_cache_check: bool = True
    cold_start_analysis: bool = True
    cold_start_lookback_hours: int = 24
    cache_hit_ratio_warning_threshold: float = 0.3

    # -- Statistics settings --
    stale_stats_threshold_days: int = 7
    row_drift_pct_threshold: float = 20.0
    proactive_refresh_check: bool = True
    tables_without_stats_check: bool = True

    # -- V-Order settings --
    vorder_warn_if_disabled: bool = True

    # -- Query Regression settings --
    check_query_regression: bool = True
    regression_lookback_days: int = 7
    regression_factor_warning: float = 2.0
    regression_factor_critical: float = 5.0
    regression_min_executions: int = 3

    # -- Output --
    verbose: bool = False

    # -- Throttle protection --
    phase_delay: float = 1.0

    def validate(self) -> None:
        """Raise ``ValueError`` if the configuration is not usable."""
        if not self.warehouse_name or self.warehouse_name == "<your_warehouse_name>":
            raise ValueError(
                "warehouse_name must be set to your actual Fabric "
                "Warehouse or Lakehouse SQL Endpoint name."
            )
