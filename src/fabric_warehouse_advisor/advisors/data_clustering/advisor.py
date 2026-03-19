"""
Fabric Warehouse Advisor — Data Clustering Advisor (Main Orchestrator)
======================================================================
Provides the :class:`DataClusteringAdvisor` class that encapsulates the
full 7-phase analysis pipeline.  Designed to run in **Microsoft Fabric
Spark** (PySpark) notebooks.

Usage
-----
::

    from fabric_warehouse_advisor import DataClusteringAdvisor, DataClusteringConfig

    config = DataClusteringConfig(warehouse_name="MyWarehouse")
    advisor = DataClusteringAdvisor(spark, config)
    result = advisor.run()

    # result.text_report  -> printable text
    # result.scores_df    -> Spark DataFrame with per-column scores
    # result.recommendations -> list of TableRecommendation
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import DataClusteringConfig
from ...core.warehouse_reader import (
    get_full_column_metadata,
    get_current_clustering_config,
    get_table_row_counts,
    get_frequently_run_queries,
    estimate_column_cardinality,
    estimate_batch_column_cardinality,
)
from ...core.predicate_parser import (
    extract_predicates_regex,
    aggregate_predicate_hits,
    QueryPredicateSummary,
)
from .scoring import (
    ColumnScore,
    TableRecommendation,
    score_all_candidates,
    build_table_recommendations,
    scores_to_dataframe,
)
from .report import (
    generate_text_report,
    generate_markdown_report,
    generate_html_report,
)
from ...core.report import save_report
from ..performance_check.checks.warehouse_type import detect_warehouse_edition


# ------------------------------------------------------------------
# Safe display() wrapper — uses the notebook built-in when available,
# falls back to DataFrame.show() outside notebooks.
# ------------------------------------------------------------------

def _display(df: DataFrame) -> None:  # type: ignore[type-arg]
    """Call the Fabric/Databricks ``display()`` built-in if available."""
    try:
        _builtin_display = display  # type: ignore[name-defined]
        _builtin_display(df)
    except NameError:
        df.show(100, truncate=False)


# ------------------------------------------------------------------
# Result container
# ------------------------------------------------------------------

@dataclass
class DataClusteringResult:
    """Container for all outputs produced by a single advisor run."""

    #: Per-column scores (Python list).
    all_scores: List[ColumnScore] = field(default_factory=list)

    #: Per-table recommendations (Python list).
    recommendations: List[TableRecommendation] = field(default_factory=list)

    #: Spark DataFrame with the detailed scores.
    scores_df: Optional[DataFrame] = None

    #: Pre-formatted text report (for ``print()``).
    text_report: str = ""

    #: Markdown report (for saving as ``.md``).
    markdown_report: str = ""

    #: HTML report (for ``displayHTML()`` in Fabric notebooks).
    html_report: str = ""

    #: ISO-8601 timestamp of when the run completed (UTC).
    captured_at: str = ""

    def save(self, path: str, format: str = "html") -> str:
        """Save the report to a file.

        Parameters
        ----------
        path : str
            Destination file path.  Parent directories are created
            automatically.
        format : str
            One of ``"html"``, ``"md"``, ``"txt"``.

        Returns
        -------
        str
            The absolute path that was written.
        """
        content_map = {
            "html": self.html_report,
            "md": self.markdown_report,
            "txt": self.text_report,
        }
        content = content_map.get(format)
        if content is None:
            raise ValueError(
                f"Unknown format '{format}'. Use 'html', 'md', or 'txt'."
            )
        return save_report(content, path, format=format)


# ------------------------------------------------------------------
# Advisor class
# ------------------------------------------------------------------

class DataClusteringAdvisor:
    """Orchestrates the full data-clustering advisory pipeline.

    Parameters
    ----------
    spark : SparkSession
        An active PySpark session (Fabric Spark provides one by default).
    config : DataClusteringConfig
        Configuration object.  Create one and override any defaults you
        need before passing it in.
    """

    def __init__(
        self,
        spark: SparkSession,
        config: DataClusteringConfig | None = None,
    ) -> None:
        self.spark = spark
        self.config = config or DataClusteringConfig()

    # ---- convenience helpers ----

    def _log(self, msg: str) -> None:
        if self.config.verbose:
            print(msg)

    def _log_header(self, title: str) -> None:
        """Print a visually distinct sub-section header (verbose only)."""
        if self.config.verbose:
            print(f"  ┌── {title} ──")

    def _log_footer(self) -> None:
        """Close a verbose sub-section."""
        if self.config.verbose:
            print(f"  └{'─' * 60}")

    def _log_kv(self, key: str, value: object, indent: int = 4) -> None:
        """Print a key-value pair aligned at 30 chars (verbose only)."""
        if self.config.verbose:
            pad = ' ' * indent
            print(f"{pad}{key:<30}: {value}")

    def _log_findings_detail(self, findings) -> None:
        """Print per-finding detail when verbose is enabled."""
        if not self.config.verbose or not findings:
            return
        self._log_header("Findings Detail")
        for f in findings:
            icon = {"CRITICAL": "❌", "HIGH": "🔴", "MEDIUM": "🟠", "LOW": "🟡", "INFO": "✅"}.get(f.level, "•")
            self._log(f"    {icon} [{f.level}] {f.object_name}")
            self._log(f"       Check : {f.check_name}")
            self._log(f"       Msg   : {f.message}")
            if f.detail:
                self._log(f"       Detail: {f.detail}")
            if f.recommendation:
                self._log(f"       Rec   : {f.recommendation}")
            if f.sql_fix:
                self._log(f"       SQL   : {f.sql_fix}")
        self._log_footer()

    @staticmethod
    def _parse_table_names(table_names: list[str]) -> set[tuple[str, str]]:
        """Parse user-supplied table names into (schema_lower, table_lower) pairs.

        An entry with no schema part is stored as ("", table_lower) which
        means "match any schema".
        """
        result: set[tuple[str, str]] = set()
        for entry in table_names:
            parts = entry.replace("[", "").replace("]", "").split(".")
            if len(parts) == 2:
                result.add((parts[0].strip().lower(), parts[1].strip().lower()))
            else:
                result.add(("", parts[0].strip().lower()))
        return result

    @staticmethod
    def _table_filter_condition(filter_set: set[tuple[str, str]]):
        """Build a PySpark Column condition that matches the tables in *filter_set*.

        The DataFrame is expected to have ``schema_name`` and ``table_name`` columns.
        """
        from pyspark.sql.functions import lower as _lower
        cond = None
        for schema, table in filter_set:
            if schema:
                c = (_lower(F.col("schema_name")) == schema) & (_lower(F.col("table_name")) == table)
            else:
                c = _lower(F.col("table_name")) == table
            cond = c if cond is None else (cond | c)
        return cond

    # ---- public API ----

    def run(self) -> DataClusteringResult:
        """Execute the full 7-phase analysis and return a :class:`DataClusteringResult`.

        Raises
        ------
        ValueError
            If the configuration is invalid (e.g. missing warehouse name).
        """
        cfg = self.config
        cfg.validate()
        spark = self.spark

        print("╔══════════════════════════════════════════════════╗")
        print("║  Fabric Warehouse Data Clustering Advisor        ║")
        print("╚══════════════════════════════════════════════════╝")
        print(f"  Warehouse : {cfg.warehouse_name}")
        if cfg.workspace_id:
            print(f"  Workspace : {cfg.workspace_id} (cross-workspace)")
        print(f"  Timestamp : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print()

        # Verbose: show active configuration
        self._log_header("Configuration")
        self._log_kv("Schemas filter", cfg.schema_names or "(all)")
        self._log_kv("Tables filter", cfg.table_names or "(all)")
        self._log_kv("min_row_count", f"{cfg.min_row_count:,}")
        self._log_kv("large_table_rows", f"{cfg.large_table_rows:,}")
        self._log_kv("min_predicate_hits", cfg.min_predicate_hits)
        self._log_kv("min_query_runs", cfg.min_query_runs)
        self._log_kv("max_clustering_columns", cfg.max_clustering_columns)
        self._log_kv("min_recommendation_score", cfg.min_recommendation_score)
        self._log_kv("cardinality_sample_fraction", cfg.cardinality_sample_fraction)
        self._log_kv("generate_ctas", cfg.generate_ctas)
        self._log_footer()

        _run_start = time.perf_counter()
        _phase_timings: Dict[str, float] = {}

        # ---- Phase 0: Edition gate ----
        _phase_start = time.perf_counter()
        print("Phase 0: Detecting warehouse edition ...")
        edition, edition_findings = detect_warehouse_edition(
            spark, cfg.warehouse_name, cfg.workspace_id, cfg.warehouse_id,
        )
        self._log(f"  Edition: {edition}")
        _phase_timings["Phase 0: Edition detection"] = time.perf_counter() - _phase_start
        self._log(f"  \u23f1 Phase 0 completed in {_phase_timings['Phase 0: Edition detection']:.2f}s")
        self._log_findings_detail(edition_findings)

        if edition != "DataWarehouse":
            print(
                f"\n\u2717 Data Clustering Advisor aborted.\n"
                f"  Detected edition: {edition}\n"
                f"  Data clustering is only supported on Fabric Data Warehouse.\n"
                f"  SQL Analytics Endpoints (Lakehouse) do not support data clustering."
            )
            raise RuntimeError(
                f"Data Clustering Advisor requires a DataWarehouse but "
                f"detected edition '{edition}'. Data clustering is not "
                f"supported on SQL Analytics Endpoints (Lakehouse)."
            )

        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)

        # ---- Phase 1: Metadata ----
        _phase_start = time.perf_counter()
        print("Phase 1: Collecting table and column metadata ...")
        full_metadata = get_full_column_metadata(
            spark, cfg.warehouse_name, cfg.workspace_id, cfg.warehouse_id
        )

        # Apply schema_names filter
        _schema_set = {s.lower() for s in cfg.schema_names} if cfg.schema_names else set()
        if _schema_set:
            full_metadata = full_metadata.filter(
                F.lower(F.col("schema_name")).isin(_schema_set)
            )
            self._log(f"  Filtered to schema(s): {cfg.schema_names}")

        # Build reusable filter condition when table_names is specified
        _table_filter = None
        if cfg.table_names:
            _filter_set = self._parse_table_names(cfg.table_names)
            _table_filter = self._table_filter_condition(_filter_set)
            full_metadata = full_metadata.filter(_table_filter)
            self._log(f"  Filtered to {len(cfg.table_names)} specified table(s).")

        if cfg.verbose:
            self._log_header("Metadata Overview")
            col_count = full_metadata.count()
            distinct_tables = (
                full_metadata
                .select("schema_name", "table_name")
                .distinct()
                .count()
            )
            self._log_kv("Total column entries", f"{col_count:,}")
            self._log_kv("Distinct tables", f"{distinct_tables:,}")
            _display(full_metadata)
            self._log_footer()
        full_metadata.cache()
        _phase_timings["Phase 1: Metadata"] = time.perf_counter() - _phase_start
        self._log(f"  \u23f1 Phase 1 completed in {_phase_timings['Phase 1: Metadata']:.2f}s")

        # Early exit: if no tables match the scope filters, skip all
        # remaining phases (avoids unnecessary Spark/SQL round-trips).
        _table_count = (
            full_metadata.select("schema_name", "table_name")
            .distinct()
            .count()
        )
        if _table_count == 0:
            _total_elapsed = time.perf_counter() - _run_start
            scope_parts = []
            if cfg.schema_names:
                scope_parts.append(f"schema_names={cfg.schema_names}")
            if cfg.table_names:
                scope_parts.append(f"table_names={cfg.table_names}")
            scope_msg = " with " + ", ".join(scope_parts) if scope_parts else ""
            print(
                f"\n\u2717 No tables found{scope_msg}.\n"
                f"  Skipping Phases 2-7 — nothing to analyse.\n"
                f"  Verify your filter values match actual schema/table names "
                f"in the warehouse."
            )
            captured_at = datetime.now(timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S UTC"
            )
            return DataClusteringResult(
                all_scores=[],
                recommendations=[],
                scores_df=scores_to_dataframe(spark, []),
                text_report="No tables matched the configured scope filters.",
                markdown_report="No tables matched the configured scope filters.",
                html_report="<p>No tables matched the configured scope filters.</p>",
                captured_at=captured_at,
            )

        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)

        # ---- Phase 2: Current clustering ----
        _phase_start = time.perf_counter()
        print("Phase 2: Reading current data clustering configuration ...")
        current_clustering = get_current_clustering_config(
            spark, cfg.warehouse_name, cfg.workspace_id, cfg.warehouse_id
        )
        # Apply the same scope filters so only selected tables are inspected
        if _schema_set:
            current_clustering = current_clustering.filter(
                F.lower(F.col("schema_name")).isin(_schema_set)
            )
        if _table_filter is not None:
            current_clustering = current_clustering.filter(_table_filter)
        num_clustered = current_clustering.count()
        if cfg.verbose:
            self._log_header("Current Clustering")
            self._log_kv("Columns in CLUSTER BY", num_clustered)
            if num_clustered > 0:
                _display(current_clustering)
            else:
                self._log("    (No tables are currently using data clustering.)")
            self._log_footer()

        # Emit warnings for potentially sub-optimal clustering choices
        if num_clustered > 0:
            # Check column count per table vs max_clustering_columns
            from collections import Counter as _Counter
            _tbl_col_counts: dict = {}
            for crow in current_clustering.collect():
                tbl_id = f"{crow.schema_name}.{crow.table_name}"
                _tbl_col_counts[tbl_id] = _tbl_col_counts.get(tbl_id, 0) + 1
                dt = (crow.data_type or "").strip().lower()
                col_id = f"{tbl_id}.{crow.column_name}"
                if dt in ("char", "varchar") and crow.max_length > 32:
                    self._log(
                        f"  \u26a0 {col_id}: Warning: max_length="
                        f"{crow.max_length} > 32; only the first 32 "
                        f"characters are used for column statistics."
                    )
                if dt in ("decimal", "numeric") and crow.precision > 18:
                    self._log(
                        f"  \u26a0 {col_id}: Warning: precision="
                        f"{crow.precision} > 18; predicates won't push "
                        f"down to storage."
                    )
                if dt in ("bit", "varbinary", "uniqueidentifier"):
                    self._log(
                        f"  \u26a0 {col_id}: Warning: {dt} is not "
                        f"supported for data clustering."
                    )
            for tbl_id, cnt in _tbl_col_counts.items():
                if cnt > cfg.max_clustering_columns:
                    self._log(
                        f"  \u26a0 {tbl_id}: Warning: {cnt} clustered "
                        f"columns exceeds recommended max of "
                        f"{cfg.max_clustering_columns}."
                    )

        current_clustering.cache()
        _phase_timings["Phase 2: Current clustering"] = time.perf_counter() - _phase_start
        self._log(f"  \u23f1 Phase 2 completed in {_phase_timings['Phase 2: Current clustering']:.2f}s")

        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)

        # ---- Phase 3: Row counts ----
        _phase_start = time.perf_counter()
        print(f"Phase 3: Counting rows per table (min threshold: {cfg.min_row_count:,}) ...")
        row_counts = get_table_row_counts(
            spark, cfg.warehouse_name, full_metadata, min_rows=cfg.min_row_count,
            workspace_id=cfg.workspace_id, warehouse_id=cfg.warehouse_id,
            verbose=cfg.verbose,
        )
        if cfg.verbose:
            self._log_header("Row Counts")
            self._log_kv("Tables above threshold", row_counts.count())
            _display(row_counts.orderBy(F.desc("row_count")))
            self._log_footer()
        row_counts.cache()
        _phase_timings["Phase 3: Row counts"] = time.perf_counter() - _phase_start
        self._log(f"  \u23f1 Phase 3 completed in {_phase_timings['Phase 3: Row counts']:.2f}s")

        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)

        # ---- Phase 4: Query patterns ----
        _phase_start = time.perf_counter()
        print("Phase 4: Analysing query patterns from Query Insights ...")
        self._log("  (Including full-scan queries \u2014 data clustering is most effective")
        self._log("   on large tables even when scanning the full dataset.)")
        freq_queries = get_frequently_run_queries(
            spark, cfg.warehouse_name, min_runs=cfg.min_query_runs,
            workspace_id=cfg.workspace_id, warehouse_id=cfg.warehouse_id,
        )

        # Build table-name -> schema mapping for full-scan tracking
        table_schema_map: Dict[str, str] = {}
        for rc_row in row_counts.select("schema_name", "table_name").collect():
            table_schema_map[rc_row.table_name] = rc_row.schema_name

        table_names_bc = spark.sparkContext.broadcast(
            list(table_schema_map.keys())
        )

        where_queries_rows = []
        fullscan_queries_rows = []
        full_scan_tables: Dict[Tuple[str, str], int] = {}

        for qrow in freq_queries.collect():
            cmd = qrow.last_run_command or ""
            cmd_upper = cmd.upper()
            # Skip internal system queries
            if "COUNT_BIG(*)" in cmd_upper and "INFORMATION_SCHEMA" in cmd_upper:
                continue

            # Identify which large tables this query references
            referenced_tables = [
                tname for tname in table_names_bc.value
                if tname.lower() in cmd.lower()
            ]
            if not referenced_tables:
                continue

            if " WHERE " in cmd_upper:
                where_queries_rows.append(qrow)
            else:
                fullscan_queries_rows.append(qrow)
                # Track full-scan query counts per referenced table
                num_runs = int(getattr(qrow, "number_of_runs", 1))
                for tname in referenced_tables:
                    schema = table_schema_map.get(tname, "dbo")
                    key = (schema, tname)
                    full_scan_tables[key] = full_scan_tables.get(
                        key, 0
                    ) + num_runs

        self._log(
            f"  Frequently-run queries with WHERE that reference large tables: "
            f"{len(where_queries_rows)}"
        )
        self._log(
            f"  Frequently-run full-scan queries (no WHERE) on large tables: "
            f"{len(fullscan_queries_rows)}"
        )

        if full_scan_tables and cfg.verbose:
            self._log_header("Full-Scan Query Activity")
            self._log(f"    {'Schema.Table':<40} {'Total Runs':>12}")
            self._log(f"    {'─' * 40} {'─' * 12}")
            for (sch, tbl), cnt in sorted(
                full_scan_tables.items(), key=lambda x: -x[1]
            ):
                self._log(f"    {sch}.{tbl:<38} {cnt:>12,}")
            self._log_footer()

        _phase_timings["Phase 4: Query patterns"] = time.perf_counter() - _phase_start
        self._log(f"  \u23f1 Phase 4 completed in {_phase_timings['Phase 4: Query patterns']:.2f}s")

        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)

        # ---- Phase 5: Predicate columns ----
        _phase_start = time.perf_counter()
        print("Phase 5: Extracting predicate columns from query text ...")
        known_columns: List[Tuple[str, str, str]] = [
            (row.schema_name, row.table_name, row.column_name)
            for row in full_metadata.select(
                "schema_name", "table_name", "column_name"
            ).distinct().collect()
        ]

        summaries: List[QueryPredicateSummary] = []
        for qrow in where_queries_rows:
            _query_start = time.perf_counter()
            cmd = qrow.last_run_command or ""
            qhash = str(getattr(qrow, "query_hash", ""))
            summary = extract_predicates_regex(
                sql_text=cmd,
                known_columns=known_columns,
                query_hash=qhash,
                number_of_runs=int(getattr(qrow, "number_of_runs", 1)),
            )
            summaries.append(summary)
            _query_elapsed = time.perf_counter() - _query_start
            if cfg.verbose:
                if summary.hits:
                    for h in summary.hits:
                        self._log(
                            f"    \u251c\u2500 {h.table_name}.{h.column_name}  "
                            f"op={h.compare_op}  origin={h.predicate_origin}"
                        )
                self._log(f"    \u2514 query {qhash[:12]}... parsed in {_query_elapsed:.3f}s")

        predicate_agg = aggregate_predicate_hits(summaries)
        self._log(f"  Unique (table, column) predicate candidates: {len(predicate_agg)}")

        if cfg.verbose and predicate_agg:
            self._log_header("Predicate Frequency (weighted by number_of_runs)")
            self._log(f"    {'Schema.Table.Column':<50} {'Hits':>8}")
            self._log(f"    {'─' * 50} {'─' * 8}")
            for (sch, tbl, col), hits in sorted(
                predicate_agg.items(), key=lambda x: -x[1]
            ):
                self._log(f"    {sch}.{tbl}.{col:<46} {hits:>8}")
            self._log_footer()

        _phase_timings["Phase 5: Predicate columns"] = time.perf_counter() - _phase_start
        self._log(f"  \u23f1 Phase 5 completed in {_phase_timings['Phase 5: Predicate columns']:.2f}s")

        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)

        # ---- Phase 6: Cardinality ----
        _phase_start = time.perf_counter()
        print("Phase 6: Estimating column cardinality for candidates ...")
        cardinality_cache: Dict[Tuple[str, str, str], Tuple[int, int, float]] = {}

        clustered_cols_set = set()
        for crow in current_clustering.collect():
            clustered_cols_set.add(
                (crow.schema_name, crow.table_name, crow.column_name)
            )

        candidate_cols = set(predicate_agg.keys()) | clustered_cols_set

        for schema, table, col in candidate_cols:
            _cardinality_start = time.perf_counter()
            self._log(f"  Estimating cardinality: {schema}.{table}.{col} ...")
            total, distinct, ratio = estimate_column_cardinality(
                spark,
                cfg.warehouse_name,
                schema,
                table,
                col,
                sample_fraction=cfg.cardinality_sample_fraction,
                workspace_id=cfg.workspace_id,
                warehouse_id=cfg.warehouse_id,
            )
            cardinality_cache[(schema, table, col)] = (total, distinct, ratio)
            _card_elapsed = time.perf_counter() - _cardinality_start
            self._log(f"    \u23f1 {schema}.{table}.{col} in {_card_elapsed:.2f}s")
            if cfg.verbose and total > 0:
                pct = f"{ratio * 100:.2f}%" if ratio >= 0 else "N/A"
                self._log(
                    f"    ├─ {col:<28} total={total:>12,}  "
                    f"distinct~={distinct:>12,}  ratio={ratio:.6f}  ({pct})"
                )

        # Batch cardinality for columns on full-scan tables
        if full_scan_tables:
            from .data_type_support import assess_data_type as _assess_dt

            fs_cols_by_table: Dict[Tuple[str, str], List[str]] = {}
            for meta in full_metadata.collect():
                tbl_key = (meta.schema_name, meta.table_name)
                if tbl_key not in full_scan_tables:
                    continue
                col_key = (
                    meta.schema_name, meta.table_name, meta.column_name
                )
                if col_key in cardinality_cache:
                    continue  # already estimated individually
                dt_check = _assess_dt(
                    meta.data_type, meta.max_length, meta.precision
                )
                if not dt_check.is_supported:
                    continue
                fs_cols_by_table.setdefault(tbl_key, []).append(
                    meta.column_name
                )

            for (schema, table), cols in fs_cols_by_table.items():
                _batch_start = time.perf_counter()
                self._log(
                    f"  Batch cardinality for full-scan table "
                    f"{schema}.{table} ({len(cols)} columns) ..."
                )
                batch_result = estimate_batch_column_cardinality(
                    spark,
                    cfg.warehouse_name,
                    schema,
                    table,
                    cols,
                    sample_fraction=cfg.cardinality_sample_fraction,
                    workspace_id=cfg.workspace_id,
                    warehouse_id=cfg.warehouse_id,
                )
                for col_name, (total, distinct, ratio) in batch_result.items():
                    cardinality_cache[(schema, table, col_name)] = (
                        total, distinct, ratio,
                    )
                    if cfg.verbose and total > 0:
                        pct = f"{ratio * 100:.2f}%" if ratio >= 0 else "N/A"
                        self._log(
                            f"    ├─ {col_name:<28} total={total:>12,}  "
                            f"distinct~={distinct:>12,}  ratio={ratio:.6f}  ({pct})"
                        )
                _batch_elapsed = time.perf_counter() - _batch_start
                self._log(f"    \u23f1 {schema}.{table} batch in {_batch_elapsed:.2f}s")
        self._log(f"  Cardinality estimated for {len(cardinality_cache)} columns.")
        _phase_timings["Phase 6: Cardinality"] = time.perf_counter() - _phase_start
        self._log(f"  \u23f1 Phase 6 completed in {_phase_timings['Phase 6: Cardinality']:.2f}s")

        if cfg.phase_delay > 0:
            time.sleep(cfg.phase_delay)

        # ---- Phase 7: Scoring & recommendations ----
        _phase_start = time.perf_counter()
        print("Phase 7: Scoring candidates and generating recommendations ...")

        all_scores = score_all_candidates(
            spark=spark,
            full_metadata=full_metadata,
            row_counts=row_counts,
            predicate_agg=predicate_agg,
            cardinality_cache=cardinality_cache,
            current_clustering=current_clustering,
            full_scan_tables=set(full_scan_tables.keys()),
            large_table_rows=cfg.large_table_rows,
            min_predicate_hits=cfg.min_predicate_hits,
            weight_table_size=cfg.score_weight_table_size,
            weight_predicate_freq=cfg.score_weight_predicate_freq,
            weight_cardinality=cfg.score_weight_cardinality,
            weight_data_type=cfg.score_weight_data_type,
            low_cardinality_upper=cfg.low_cardinality_upper,
            high_cardinality_lower=cfg.high_cardinality_lower,
            low_cardinality_abs_max=cfg.low_cardinality_abs_max,
            min_recommendation_score=cfg.min_recommendation_score,
        )

        table_recommendations = build_table_recommendations(
            scores=all_scores,
            max_columns=cfg.max_clustering_columns,
            min_score=cfg.min_recommendation_score,
            warehouse_name=cfg.warehouse_name,
            generate_ctas=cfg.generate_ctas,
        )

        self._log(f"  Scored columns       : {len(all_scores)}")
        self._log(f"  Tables with candidates: {len(table_recommendations)}")

        # ---- Outputs ----
        scores_df = scores_to_dataframe(spark, all_scores)
        if cfg.verbose:
            self._log_header("Detailed Scores (sorted by composite score)")
            _display(scores_df)
            self._log_footer()

        captured_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        text_report = generate_text_report(
            table_recommendations,
            min_score=cfg.min_recommendation_score,
            captured_at=captured_at,
            warehouse_name=cfg.warehouse_name,
        )

        markdown_report = generate_markdown_report(
            table_recommendations,
            min_score=cfg.min_recommendation_score,
            captured_at=captured_at,
            warehouse_name=cfg.warehouse_name,
        )

        html_report = generate_html_report(
            table_recommendations,
            min_score=cfg.min_recommendation_score,
            captured_at=captured_at,
            warehouse_name=cfg.warehouse_name,
        )

        _phase_timings["Phase 7: Scoring & reports"] = time.perf_counter() - _phase_start
        self._log(f"  ⏱ Phase 7 completed in {_phase_timings['Phase 7: Scoring & reports']:.2f}s")

        _total_elapsed = time.perf_counter() - _run_start
        if cfg.verbose:
            self._log("\n" + "═" * 60)
            self._log("  ⏱ Timing Summary")
            self._log("═" * 60)
            phase_col_width = 35
            elapsed_col_width = 8
            pct_col_width = 7
            for phase_name, elapsed in _phase_timings.items():
                pct = (elapsed / _total_elapsed * 100) if _total_elapsed > 0 else 0
                self._log(
                    f"  {phase_name:<{phase_col_width}} "
                    f"{elapsed:>{elapsed_col_width}.2f}s  ({pct:>5.1f}%)"
                )
            _separator = "─"
            self._log(
                f"  {_separator * phase_col_width} "
                f"{_separator * elapsed_col_width}  "
                f"{_separator * pct_col_width}"
            )
            self._log(f"  {'Total':<{phase_col_width}} {_total_elapsed:>{elapsed_col_width}.2f}s")
            self._log("═" * 60)

        print("\n✓ Data Clustering Advisor completed successfully.")
        print("  Use  displayHTML(result.html_report)  for a rich HTML view.")
        print("  Use  result.save('report.html')  to save as HTML (default).")
        print("  Other formats: result.save('report.md', format='md') or result.save('report.txt', format='txt')")

        return DataClusteringResult(
            all_scores=all_scores,
            recommendations=table_recommendations,
            scores_df=scores_df,
            text_report=text_report,
            markdown_report=markdown_report,
            html_report=html_report,
            captured_at=captured_at,
        )
