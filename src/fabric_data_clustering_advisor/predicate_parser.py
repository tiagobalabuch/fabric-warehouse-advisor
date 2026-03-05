"""
Fabric Warehouse Data Clustering Advisor - SQL Predicate Parser
======================================================
Extracts column names that appear in WHERE-clause predicates from
raw SQL query text.  Two strategies are provided:

 1. **Regex-based heuristic** (default, zero dependencies)
 2. **Execution-plan XML parser** (optional, parses ShowPlanXML)
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple


# ------------------------------------------------------------------
# Data classes
# ------------------------------------------------------------------

@dataclass
class PredicateHit:
    """A column reference found in a WHERE predicate."""
    schema_name: Optional[str] = None
    table_name: Optional[str] = None
    column_name: Optional[str] = None
    compare_op: Optional[str] = None
    predicate_origin: str = "FILTER"
    rhs_value: Optional[str] = None
    query_hash: Optional[str] = None


@dataclass
class QueryPredicateSummary:
    """All predicate hits extracted from a single query."""
    query_hash: Optional[str] = None
    query_text: str = ""
    number_of_runs: int = 0
    hits: List[PredicateHit] = field(default_factory=list)


# ==================================================================
# STRATEGY 1 - Regex-based heuristic parser
# ==================================================================

_BRACKET_RE = re.compile(r"\[([^\]]*)\]")

_WHERE_RE = re.compile(
    r"\bWHERE\b(.*?)(?:\bORDER\s+BY\b|\bGROUP\s+BY\b|\bHAVING\b|\bUNION\b|\bEXCEPT\b|\bINTERSECT\b|;|$)",
    re.IGNORECASE | re.DOTALL,
)

_COMPARE_OPS = re.compile(
    r"(>=|<=|<>|!=|=|>|<|\bBETWEEN\b|\bIN\b|\bLIKE\b|\bIS\s+NOT\b|\bIS\b)",
    re.IGNORECASE,
)


def _unquote(identifier: str) -> str:
    return _BRACKET_RE.sub(r"\1", identifier).strip()


def _extract_where_clauses(sql: str) -> List[str]:
    return [m.group(1).strip() for m in _WHERE_RE.finditer(sql)]


def _build_column_lookup(
    known_columns: List[Tuple[str, str, str]],
) -> Dict[str, Tuple[str, str, str]]:
    lookup: Dict[str, Tuple[str, str, str]] = {}
    for schema, table, col in known_columns:
        key_full = f"{table}.{col}".lower()
        key_bare = col.lower()
        lookup[key_full] = (schema, table, col)
        lookup[key_bare] = (schema, table, col)
        key_3part = f"{schema}.{table}.{col}".lower()
        lookup[key_3part] = (schema, table, col)
    return lookup


def _find_identifiers_in_text(text: str) -> List[str]:
    pattern = re.compile(
        r"(?:\[?\w+\]?\.){0,2}\[?\w+\]?",
        re.IGNORECASE,
    )
    matches = pattern.findall(text)
    results = []
    for m in matches:
        cleaned = _unquote(m)
        if cleaned and not cleaned.isdigit() and not _is_sql_keyword(cleaned):
            results.append(cleaned)
    return results


_SQL_KEYWORDS = frozenset({
    "select", "from", "where", "and", "or", "not", "in", "between",
    "like", "is", "null", "exists", "case", "when", "then", "else",
    "end", "as", "on", "inner", "outer", "left", "right", "full",
    "cross", "join", "having", "group", "by", "order", "asc", "desc",
    "top", "distinct", "union", "all", "insert", "into", "values",
    "update", "set", "delete", "create", "alter", "drop", "table",
    "index", "view", "with", "nolock", "count", "sum", "avg", "min",
    "max", "cast", "convert", "isnull", "coalesce", "count_big",
})


def _is_sql_keyword(token: str) -> bool:
    return token.lower() in _SQL_KEYWORDS


def extract_predicates_regex(
    sql_text: str,
    known_columns: List[Tuple[str, str, str]],
    query_hash: Optional[str] = None,
    number_of_runs: int = 1,
) -> QueryPredicateSummary:
    """
    Heuristic extraction of predicate columns from raw SQL.
    """
    lookup = _build_column_lookup(known_columns)
    where_clauses = _extract_where_clauses(sql_text)

    hits: List[PredicateHit] = []
    seen: Set[Tuple[str, str, str]] = set()

    for where_body in where_clauses:
        conditions = re.split(r"\bAND\b|\bOR\b", where_body, flags=re.IGNORECASE)

        for condition in conditions:
            condition = condition.strip()
            if not condition:
                continue

            op_match = _COMPARE_OPS.search(condition)
            compare_op = op_match.group(1).strip().upper() if op_match else None

            identifiers = _find_identifiers_in_text(condition)

            for ident in identifiers:
                parts = _unquote(ident).split(".")
                lookup_keys = []
                if len(parts) == 3:
                    lookup_keys.append(f"{parts[0]}.{parts[1]}.{parts[2]}".lower())
                    lookup_keys.append(f"{parts[1]}.{parts[2]}".lower())
                    lookup_keys.append(parts[2].lower())
                elif len(parts) == 2:
                    lookup_keys.append(f"{parts[0]}.{parts[1]}".lower())
                    lookup_keys.append(parts[1].lower())
                elif len(parts) == 1:
                    lookup_keys.append(parts[0].lower())

                for key in lookup_keys:
                    if key in lookup:
                        schema, table, col = lookup[key]
                        dedup_key = (table, col, compare_op or "?")
                        if dedup_key not in seen:
                            seen.add(dedup_key)

                            origin = "FILTER"
                            if compare_op == "=" or compare_op == "EQ":
                                if op_match:
                                    rhs = condition[op_match.end():].strip()
                                    rhs_idents = _find_identifiers_in_text(rhs)
                                    rhs_has_col = any(
                                        _unquote(ri).split(".")[-1].lower() in {
                                            v[2].lower() for v in lookup.values()
                                        }
                                        for ri in rhs_idents[:1]
                                    )
                                    if rhs_has_col:
                                        origin = "JOIN (equality)"
                                        continue

                            hits.append(
                                PredicateHit(
                                    schema_name=schema,
                                    table_name=table,
                                    column_name=col,
                                    compare_op=compare_op,
                                    predicate_origin=origin,
                                    query_hash=query_hash,
                                )
                            )
                        break

    return QueryPredicateSummary(
        query_hash=query_hash,
        query_text=sql_text,
        number_of_runs=number_of_runs,
        hits=hits,
    )


# ==================================================================
# STRATEGY 2 - ShowPlanXML parser
# ==================================================================

_SHOWPLAN_NS = "http://schemas.microsoft.com/sqlserver/2004/07/showplan"


def _ns(tag: str) -> str:
    return f"{{{_SHOWPLAN_NS}}}{tag}"


def _local(element: ET.Element) -> str:
    tag = element.tag
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _find_all_recursive(root: ET.Element, local_name: str) -> List[ET.Element]:
    results = []
    for elem in root.iter():
        if _local(elem) == local_name:
            results.append(elem)
    return results


def parse_showplan_predicates(plan_xml: str) -> List[PredicateHit]:
    """
    Parse a ShowPlanXML string and extract all Compare predicates.
    """
    if not plan_xml or not plan_xml.strip():
        return []

    try:
        root = ET.fromstring(plan_xml)
    except ET.ParseError as exc:
        print(f"  [WARN] Failed to parse ShowPlanXML: {exc}")
        return []

    hits: List[PredicateHit] = []

    join_col_set: Set[Tuple[str, str, str]] = set()
    for tag_name in ("HashKeysBuild", "HashKeysProbe"):
        for node in _find_all_recursive(root, tag_name):
            for cr in _find_all_recursive(node, "ColumnReference"):
                db = cr.get("Database", "")
                table = cr.get("Table", "")
                col = cr.get("Column", "")
                if col:
                    join_col_set.add((db, table, col))

    context_tags = {"Predicate": "FILTER", "ProbeResidual": "JOIN (residual)"}

    for context_name, default_origin in context_tags.items():
        for context_node in _find_all_recursive(root, context_name):
            for compare_node in _find_all_recursive(context_node, "Compare"):
                compare_op = compare_node.get("CompareOp", "")

                scalar_ops = [
                    child for child in compare_node
                    if _local(child) == "ScalarOperator"
                ]

                if len(scalar_ops) < 2:
                    continue

                left_cr = None
                for cr in _find_all_recursive(scalar_ops[0], "ColumnReference"):
                    left_cr = cr
                    break

                if left_cr is None:
                    continue

                l_db = left_cr.get("Database", "")
                l_schema = left_cr.get("Schema", "")
                l_table = left_cr.get("Table", "")
                l_col = left_cr.get("Column", "")

                rhs_value = None
                right_const = None
                for const in _find_all_recursive(scalar_ops[1], "Const"):
                    right_const = const.get("ConstValue", "")
                    rhs_value = right_const
                    break

                if right_const is None:
                    for cr in _find_all_recursive(scalar_ops[1], "ColumnReference"):
                        r_full = ".".join(filter(None, [
                            cr.get("Database", ""),
                            cr.get("Schema", ""),
                            cr.get("Table", ""),
                            cr.get("Column", ""),
                        ]))
                        rhs_value = r_full
                        break

                origin = default_origin
                if context_name == "Predicate":
                    if compare_op in ("IS", "IS NOT"):
                        const_val = (right_const or "").replace("(", "").replace(")", "").strip().upper()
                        if const_val == "NULL" and (l_db, l_table, l_col) in join_col_set:
                            origin = "INFERRED (NullReject from INNER JOIN)"

                if compare_op in ("EQ", "IS NOT"):
                    continue

                clean_schema = l_schema.strip("[]")
                clean_table = l_table.strip("[]")
                clean_col = l_col

                hits.append(
                    PredicateHit(
                        schema_name=clean_schema,
                        table_name=clean_table,
                        column_name=clean_col,
                        compare_op=compare_op,
                        predicate_origin=origin,
                        rhs_value=rhs_value,
                    )
                )

    return hits


# ==================================================================
# Aggregate predicate hits across multiple queries
# ==================================================================

def aggregate_predicate_hits(
    summaries: List[QueryPredicateSummary],
) -> Dict[Tuple[str, str, str], int]:
    """
    Aggregate predicate hits across all analysed queries.

    Returns a dict of (schema, table, column) -> weighted_hit_count.
    """
    agg: Dict[Tuple[str, str, str], int] = {}

    for summary in summaries:
        seen_in_query: Set[Tuple[str, str, str]] = set()
        for hit in summary.hits:
            if "JOIN" in (hit.predicate_origin or "").upper():
                continue
            key = (
                hit.schema_name or "",
                hit.table_name or "",
                hit.column_name or "",
            )
            if key not in seen_in_query:
                seen_in_query.add(key)
                agg[key] = agg.get(key, 0) + summary.number_of_runs

    return agg
