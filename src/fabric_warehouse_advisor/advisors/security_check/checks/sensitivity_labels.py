"""
Security Check — Sensitivity Labels  (SEC-010)
================================================
Checks whether a Microsoft Purview sensitivity label is applied
to the warehouse item.

The label data is already available in the ``list_warehouses``
response (``sensitivityLabel`` field), so this check requires
**no additional API call**.

Checks
------
* No sensitivity label applied                → MEDIUM
* Sensitivity label applied                   → INFO
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from ..config import SecurityCheckConfig
from ..findings import (
    Finding,
    LEVEL_INFO,
    LEVEL_HIGH,
    CATEGORY_SENSITIVITY_LABELS,
)


def check_sensitivity_labels(
    warehouse_info: Dict[str, Any],
    config: SecurityCheckConfig,
) -> List[Finding]:
    """Check whether a sensitivity label is applied to the warehouse.

    Parameters
    ----------
    warehouse_info : dict
        The warehouse object from the ``list_warehouses`` response.
        Expected keys: ``displayName``, ``id``, ``sensitivityLabel``.
    config : SecurityCheckConfig
        Advisor configuration.

    Returns
    -------
    list[Finding]
        Findings related to sensitivity labels.
    """
    findings: List[Finding] = []
    wh_name = warehouse_info.get("displayName", "(unknown)")
    wh_id = warehouse_info.get("id", "")

    label: Optional[Dict[str, Any]] = warehouse_info.get("sensitivityLabel")

    # The warehouse API returns labelId; the SQL endpoint API returns id.
    label_id = ""
    if label:
        label_id = label.get("labelId") or label.get("id") or ""

    item = config.item_label
    Item = item[0].upper() + item[1:]

    if not label or not label_id:
        findings.append(Finding(
            level=LEVEL_HIGH,
            category=CATEGORY_SENSITIVITY_LABELS,
            check_name="no_sensitivity_label",
            object_name=wh_name,
            message=(
                f"{Item} has no Microsoft Purview sensitivity "
                f"label applied."
            ),
            detail=(
                "Sensitivity labels help classify and protect data "
                "based on its sensitivity level. Without a label, "
                "downstream consumers and governance tools cannot "
                "automatically enforce data protection policies."
            ),
            recommendation=(
                f"Apply an appropriate sensitivity label to this "
                f"{item} in the Fabric portal (item settings → "
                f"Sensitivity label) or via Microsoft Purview."
            ),
        ))
    else:
        findings.append(Finding(
            level=LEVEL_INFO,
            category=CATEGORY_SENSITIVITY_LABELS,
            check_name="sensitivity_label_applied",
            object_name=wh_name,
            message=f"{Item} has a sensitivity label applied.",
            detail=f"Label ID: {label_id}.",
        ))

    return findings
