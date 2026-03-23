"""
Fabric Warehouse Advisor — Data Clustering Configuration
=========================================================
All configuration values are exposed as fields of the
``DataClusteringConfig`` dataclass with sensible defaults.

Users create a config instance, override what they need, and pass
it to :class:`DataClusteringAdvisor`.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class DataClusteringConfig:
    """Configuration for the Data Clustering Advisor.

    Every public constant that was previously in the flat ``config.py``
    module is now a field here, with the same default value.  Pass an
    instance of this class to :pyclass:`DataClusteringAdvisor` to run the
    analysis.

    Parameters
    ----------
    warehouse_name : str
        The Fabric Warehouse name (three-part name prefix used by the
        Spark connector).  **Required** — there is no valid default.

    workspace_id : str
        Optional.  The Fabric Workspace ID (GUID) where the warehouse
        lives.  Only needed for **cross-workspace** access.  When empty
        the advisor assumes it is running in the same workspace.

    warehouse_id : str
        Optional.  The Fabric Warehouse item ID (GUID).  Only needed
        for cross-workspace access together with ``workspace_id``.

    min_row_count : int
        Minimum row count for a table to be considered for data clustering
        analysis.  Tables smaller than this are skipped entirely.

    large_table_rows : int
        Tables above this row count get the highest "large table" score
        boost.  Default is 50 million.

    min_predicate_hits : int
        Minimum number of times a column must appear in WHERE predicates
        to be treated as a candidate.

    min_query_runs : int
        Minimum number of runs for a query to be considered "frequent".

    low_cardinality_upper : float
        Cardinality ratio below which a column is classified as *Low*.

    high_cardinality_lower : float
        Cardinality ratio at or above which a column is classified as
        *High*.

    low_cardinality_abs_max : int
        Number of distinct values below which cardinality is always
        "Low" regardless of the ratio.

    cardinality_sample_fraction : float
        Fraction of the table to sample for cardinality estimation.
        Set to 1.0 for full-table reads (more accurate, slower).

    score_weight_table_size : int
        Maximum score points for the table-size factor.

    score_weight_predicate_freq : int
        Maximum score points for predicate frequency.

    score_weight_cardinality : int
        Maximum score points for column cardinality.

    score_weight_data_type : int
        Maximum score points for data-type support.

    max_clustering_columns : int
        Warn when a table already exceeds this many clustered columns.
        Does **not** limit how many columns are included in CTAS output.

    min_recommendation_score : int
        Minimum composite score (0-100) to surface a recommendation.

    generate_ctas : bool
        If ``True``, generate one CTAS DDL statement per recommended
        column.  Defaults to ``False``; set to ``True`` when you want
        ready-to-run DDL in the report.

    table_names : list[str]
        Optional list of tables to analyse.  When non-empty, **only**
        these tables are included (all others are skipped).  Each entry
        can be:

        * ``"table_name"``          — matches any schema
        * ``"schema.table_name"``   — matches exact schema + table

        Leave empty (the default) to analyse every table that meets
        the ``min_row_count`` threshold.

    schema_names : list[str]
        Optional list of schemas to restrict analysis to.
        Empty means all user schemas.

    verbose : bool
        If ``True``, display intermediate DataFrames for debugging.

    phase_delay : float
        Seconds to pause between phases to reduce HTTP 429 throttling
        from the Fabric control-plane API.  Set to ``0`` to disable.
    """

    # -- Connection --
    warehouse_name: str = ""
    workspace_id: str = ""
    warehouse_id: str = ""

    # -- Thresholds --
    min_row_count: int = 1_000_000
    large_table_rows: int = 50_000_000
    min_predicate_hits: int = 2
    min_query_runs: int = 2

    # -- Cardinality classification --
    low_cardinality_upper: float = 0.001
    high_cardinality_lower: float = 0.05
    low_cardinality_abs_max: int = 50

    # -- Sampling --
    cardinality_sample_fraction: float = 1.0

    # -- Scoring weights (total theoretical max = 100) --
    score_weight_table_size: int = 30
    score_weight_predicate_freq: int = 30
    score_weight_cardinality: int = 25
    score_weight_data_type: int = 15

    # -- Recommendation --
    max_clustering_columns: int = 3
    min_recommendation_score: int = 40
    generate_ctas: bool = False

    # -- Scope filtering --
    schema_names: list[str] = field(default_factory=list)
    table_names: list[str] = field(default_factory=list)

    # -- REST API (for workspace metadata) --
    fabric_token: str = ""
    use_notebook_token: bool = True

    # -- Output --
    verbose: bool = False

    # -- Throttle protection --
    phase_delay: float = 1.0

    def validate(self) -> None:
        """Raise ``ValueError`` if the configuration is not usable."""
        if not self.warehouse_name or self.warehouse_name == "<your_warehouse_name>":
            raise ValueError(
                "warehouse_name must be set to your actual Fabric Warehouse name."
            )
        if self.cardinality_sample_fraction <= 0 or self.cardinality_sample_fraction > 1.0:
            raise ValueError(
                "cardinality_sample_fraction must be in the range (0, 1.0]."
            )
        total_weight = (
            self.score_weight_table_size
            + self.score_weight_predicate_freq
            + self.score_weight_cardinality
            + self.score_weight_data_type
        )
        if total_weight != 100:
            raise ValueError(
                f"Score weights must sum to 100, but got {total_weight}."
            )
