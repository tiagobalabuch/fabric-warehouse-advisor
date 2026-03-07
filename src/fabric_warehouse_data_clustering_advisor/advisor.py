"""
Fabric Warehouse Data Clustering Advisor - Main Orchestrator
=============================================================
Provides the :class:`DataClusteringAdvisor` class that encapsulates the
full 7-phase analysis pipeline.  Designed to run in **Microsoft Fabric
Spark** (PySpark) notebooks.

Usage
-----
::

    from fabric_warehouse_data_clustering_advisor import DataClusteringAdvisor, DataClusteringAdvisorConfig

    config = DataClusteringAdvisorConfig(warehouse_name="MyWarehouse")
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

from .config import DataClusteringAdvisorConfig
from .warehouse_reader import (
    get_full_column_metadata,
    get_current_clustering_config,
    get_table_row_counts,
    get_frequently_run_queries,
    estimate_column_cardinality,
    estimate_batch_column_cardinality,
)
from .predicate_parser import (
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
    save_report,
)


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
class AdvisorResult:
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
    config : DataClusteringAdvisorConfig
        Configuration object.  Create one and override any defaults you
        need before passing it in.
    """

    def __init__(
        self,
        spark: SparkSession,
        config: DataClusteringAdvisorConfig | None = None,
    ) -> None:
        self.spark = spark
        self.config = config or DataClusteringAdvisorConfig()

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

    def run(self) -> AdvisorResult:
        """Execute the full 7-phase analysis and return an :class:`AdvisorResult`.

        Raises
        ------
        ValueError
            If the configuration is invalid (e.g. missing warehouse name).
        """
        cfg = self.config
        cfg.validate()
        spark = self.spark

        print(f"Fabric Warehouse Data Clustering Advisor")
        print(f"Warehouse : {cfg.warehouse_name}")
        if cfg.workspace_id:
            print(f"Workspace : {cfg.workspace_id} (cross-workspace)")
        print(f"Timestamp : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print()

        _run_start = time.perf_counter()
        _phase_timings: Dict[str, float] = {}

        # ---- Phase 1: Metadata ----
        _phase_start = time.perf_counter()
        print("Phase 1: Collecting table and column metadata ...")
        full_metadata = get_full_column_metadata(
            spark, cfg.warehouse_name, cfg.workspace_id, cfg.warehouse_id
        )

        # Build reusable filter condition when table_names is specified
        _table_filter = None
        if cfg.table_names:
            _filter_set = self._parse_table_names(cfg.table_names)
            _table_filter = self._table_filter_condition(_filter_set)
            full_metadata = full_metadata.filter(_table_filter)
            print(f"  Filtered to {len(cfg.table_names)} specified table(s).")

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
        print(f"  ⏱ Phase 1 completed in {_phase_timings['Phase 1: Metadata']:.2f}s")

        # ---- Phase 2: Current clustering ----
        _t0 = time.perf_counter()
        print("Phase 2: Reading current data clustering configuration ...")
        current_clustering = get_current_clustering_config(
            spark, cfg.warehouse_name, cfg.workspace_id, cfg.warehouse_id
        )
        # Apply the same table filter so only selected tables are inspected
        if _table_filter is not None:
            current_clustering = current_clustering.filter(_table_filter)
        num_clustered = current_clustering.count()
        if cfg.verbose:
            self._log_header("Current Clustering")
            self._log_kv("Columns in CLUSTER BY", num_clustered)
            if num_clustered > 0:
                _display(current_clustering)
            else:
                print("    (No tables are currently using data clustering.)")
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
                    print(
                        f"  \u26a0 {col_id}: Warning: max_length="
                        f"{crow.max_length} > 32; only the first 32 "
                        f"characters are used for column statistics."
                    )
                if dt in ("decimal", "numeric") and crow.precision > 18:
                    print(
                        f"  \u26a0 {col_id}: Warning: precision="
                        f"{crow.precision} > 18; predicates won't push "
                        f"down to storage."
                    )
                if dt in ("bit", "varbinary", "uniqueidentifier"):
                    print(
                        f"  \u26a0 {col_id}: Warning: {dt} is not "
                        f"supported for data clustering."
                    )
            for tbl_id, cnt in _tbl_col_counts.items():
                if cnt > cfg.max_clustering_columns:
                    print(
                        f"  \u26a0 {tbl_id}: Warning: {cnt} clustered "
                        f"columns exceeds recommended max of "
                        f"{cfg.max_clustering_columns}."
                    )

        current_clustering.cache()
        _phase_timings["Phase 2: Current clustering"] = time.perf_counter() - _t0
        print(f"  ⏱ Phase 2 completed in {_phase_timings['Phase 2: Current clustering']:.2f}s")

        # ---- Phase 3: Row counts ----
        _t0 = time.perf_counter()
        print(f"Phase 3: Counting rows per table (min threshold: {cfg.min_row_count:,}) ...")
        row_counts = get_table_row_counts(
            spark, cfg.warehouse_name, full_metadata, min_rows=cfg.min_row_count,
            workspace_id=cfg.workspace_id, warehouse_id=cfg.warehouse_id,
        )
        if cfg.verbose:
            self._log_header("Row Counts")
            self._log_kv("Tables above threshold", row_counts.count())
            _display(row_counts.orderBy(F.desc("row_count")))
            self._log_footer()
        row_counts.cache()
        _phase_timings["Phase 3: Row counts"] = time.perf_counter() - _t0
        print(f"  ⏱ Phase 3 completed in {_phase_timings['Phase 3: Row counts']:.2f}s")

        # ---- Phase 4: Query patterns ----
        _t0 = time.perf_counter()
        print("Phase 4: Analysing query patterns from Query Insights ...")
        print("  (Including full-scan queries \u2014 data clustering is most effective")
        print("   on large tables even when scanning the full dataset.)")
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

        print(
            f"  Frequently-run queries with WHERE that reference large tables: "
            f"{len(where_queries_rows)}"
        )
        print(
            f"  Frequently-run full-scan queries (no WHERE) on large tables: "
            f"{len(fullscan_queries_rows)}"
        )
        if full_scan_tables and cfg.verbose:
            self._log_header("Full-Scan Query Activity")
            print(f"    {'Schema.Table':<40} {'Total Runs':>12}")
            print(f"    {'─' * 40} {'─' * 12}")
            for (sch, tbl), cnt in sorted(
                full_scan_tables.items(), key=lambda x: -x[1]
            ):
                print(f"    {sch}.{tbl:<38} {cnt:>12,}")
            self._log_footer()

        _phase_timings["Phase 4: Query patterns"] = time.perf_counter() - _t0
        print(f"  ⏱ Phase 4 completed in {_phase_timings['Phase 4: Query patterns']:.2f}s")

        # ---- Phase 5: Predicate columns ----
        _t0 = time.perf_counter()
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
            _qt_elapsed = time.perf_counter() - _query_start
            if cfg.verbose:
                if summary.hits:
                    for h in summary.hits:
                        self._log(
                            f"    \u251c\u2500 {h.table_name}.{h.column_name}  "
                            f"op={h.compare_op}  origin={h.predicate_origin}"
                        )
                self._log(f"    \u2514 query {qhash[:12]}... parsed in {_qt_elapsed:.3f}s")

        predicate_agg = aggregate_predicate_hits(summaries)
        print(f"  Unique (table, column) predicate candidates: {len(predicate_agg)}")

        if cfg.verbose and predicate_agg:
            self._log_header("Predicate Frequency (weighted by number_of_runs)")
            print(f"    {'Schema.Table.Column':<50} {'Hits':>8}")
            print(f"    {'─' * 50} {'─' * 8}")
            for (sch, tbl, col), hits in sorted(
                predicate_agg.items(), key=lambda x: -x[1]
            ):
                print(f"    {sch}.{tbl}.{col:<46} {hits:>8}")
            self._log_footer()

        _phase_timings["Phase 5: Predicate columns"] = time.perf_counter() - _t0
        print(f"  ⏱ Phase 5 completed in {_phase_timings['Phase 5: Predicate columns']:.2f}s")

        # ---- Phase 6: Cardinality ----
        _t0 = time.perf_counter()
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
            print(f"  Estimating cardinality: {schema}.{table}.{col} ...")
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
            _ct_elapsed = time.perf_counter() - _cardinality_start
            print(f"    \u23f1 {schema}.{table}.{col} in {_ct_elapsed:.2f}s")
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
                print(
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
                _bt_elapsed = time.perf_counter() - _batch_start
                print(f"    \u23f1 {schema}.{table} batch in {_bt_elapsed:.2f}s")
        print(f"  Cardinality estimated for {len(cardinality_cache)} columns.")
        _phase_timings["Phase 6: Cardinality"] = time.perf_counter() - _t0
        print(f"  ⏱ Phase 6 completed in {_phase_timings['Phase 6: Cardinality']:.2f}s")

        # ---- Phase 7: Scoring & recommendations ----
        _t0 = time.perf_counter()
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

        print(f"  Scored columns       : {len(all_scores)}")
        print(f"  Tables with candidates: {len(table_recommendations)}")

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
        )
        print(text_report)

        markdown_report = generate_markdown_report(
            table_recommendations,
            min_score=cfg.min_recommendation_score,
            captured_at=captured_at,
        )

        html_report = generate_html_report(
            table_recommendations,
            min_score=cfg.min_recommendation_score,
            captured_at=captured_at,
        )

        _phase_timings["Phase 7: Scoring & reports"] = time.perf_counter() - _t0
        print(f"  ⏱ Phase 7 completed in {_phase_timings['Phase 7: Scoring & reports']:.2f}s")

        _total_elapsed = time.perf_counter() - _run_start

        print("\n" + "═" * 60)
        print("  ⏱ Timing Summary")
        print("═" * 60)
        phase_col_width = 35
        elapsed_col_width = 8
        pct_col_width = 7
        for phase_name, elapsed in _phase_timings.items():
            pct = (elapsed / _total_elapsed * 100) if _total_elapsed > 0 else 0
            print(
                f"  {phase_name:<{phase_col_width}} "
                f"{elapsed:>{elapsed_col_width}.2f}s  ({pct:>5.1f}%)"
            )
        _separator = "─"
        print(
            f"  {_separator * phase_col_width} "
            f"{_separator * elapsed_col_width}  "
            f"{_separator * pct_col_width}"
        )
        print(f"  {'Total':<{phase_col_width}} {_total_elapsed:>{elapsed_col_width}.2f}s")
        print("═" * 60)

        print("\n✓ Data Clustering Advisor completed successfully.")
        print("  Use  displayHTML(result.html_report)  for a rich HTML view.")
        print("  Use  result.save('path.html')  to save the report to a file.")

        return AdvisorResult(
            all_scores=all_scores,
            recommendations=table_recommendations,
            scores_df=scores_df,
            text_report=text_report,
            markdown_report=markdown_report,
            html_report=html_report,
            captured_at=captured_at,
        )
