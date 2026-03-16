"""
Fabric Warehouse Advisor — Fabric REST API Client
====================================================
Zero-dependency HTTP client for the Microsoft Fabric REST API.
Uses stdlib ``urllib.request`` for HTTP and ``notebookutils`` for
token acquisition inside Fabric notebooks.

Handles the three Fabric API patterns:

* **Throttling** — automatic retry on HTTP 429 with ``Retry-After``
* **Pagination** — follows ``continuationUri``/``continuationToken``
* **Long Running Operations** — polls ``/operations/{id}`` until done
"""

from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

# ── Token acquisition ──
# notebookutils is only available inside Fabric notebooks.
# Guard the import exactly like warehouse_reader.py guards FabricConstants.
try:
    from notebookutils import credentials as _nb_credentials
except ImportError:
    _nb_credentials = None  # running outside Fabric (e.g. local tests)


_FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
_FABRIC_AUDIENCE = "https://api.fabric.microsoft.com"


# ------------------------------------------------------------------
# Exception
# ------------------------------------------------------------------

class FabricRestError(Exception):
    """Raised when a Fabric REST API call fails."""

    def __init__(
        self,
        message: str,
        status_code: int = 0,
        error_code: str = "",
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.error_code = error_code


# ------------------------------------------------------------------
# Client
# ------------------------------------------------------------------

class FabricRestClient:
    """Thin wrapper around the Fabric REST API (v1).

    Parameters
    ----------
    token : str
        User-supplied Fabric REST token.  Takes precedence over
        notebook-acquired tokens.
    use_notebook_token : bool
        If *True* (default) and running inside a Fabric notebook,
        acquire the token automatically via ``notebookutils``.
    max_retries : int
        Maximum retry attempts on HTTP 429 and 5xx errors.
    verbose : bool
        Print diagnostic messages during HTTP calls.
    """

    def __init__(
        self,
        token: str = "",
        use_notebook_token: bool = True,
        max_retries: int = 5,
        verbose: bool = False,
    ) -> None:
        self._token = token
        self._use_notebook_token = use_notebook_token
        self._max_retries = max_retries
        self._verbose = verbose

    def _log(self, msg: str) -> None:
        if self._verbose:
            print(msg)

    # ── Token ─────────────────────────────────────────────────────

    def _get_token(self) -> str:
        """Resolve an access token (user-supplied or notebook-acquired)."""
        if self._token:
            return self._token
        if self._use_notebook_token and _nb_credentials is not None:
            return _nb_credentials.getToken(_FABRIC_AUDIENCE)
        raise FabricRestError(
            "No Fabric REST API token available. "
            "Set fabric_token in config or run inside a Fabric notebook "
            "with use_notebook_token=True."
        )

    def is_available(self) -> bool:
        """Return *True* if a token can be obtained."""
        try:
            self._get_token()
            return True
        except FabricRestError:
            return False

    @staticmethod
    def get_current_workspace_id(spark: Any = None) -> Optional[str]:
        """Return the current workspace ID from Spark config.

        Uses ``spark.conf.get("trident.artifact.workspace.id")``
        which is available in all Fabric notebook sessions.

        Parameters
        ----------
        spark
            An active SparkSession.  Required.

        Returns
        -------
        str or None
            The workspace ID, or ``None`` if unavailable.
        """
        if spark is None:
            return None
        try:
            return spark.conf.get("trident.artifact.workspace.id")
        except Exception:
            return None

    # ── Low-level HTTP ────────────────────────────────────────────

    @staticmethod
    def _parse_retry_after(value: str, default: int = 30) -> int:
        """Parse a Retry-After header value to seconds.

        The HTTP spec allows either an integer (seconds) or an
        HTTP-date.  We only support the integer form; non-numeric
        values fall back to *default*.
        """
        try:
            return int(value)
        except (ValueError, TypeError):
            return default

    def _request(
        self,
        url: str,
        method: str = "GET",
        body: Optional[bytes] = None,
    ) -> Tuple[int, Dict[str, str], bytes]:
        """Execute a single HTTP request with *Authorization* header.

        Returns ``(status_code, headers_dict, response_body_bytes)``.
        Never raises for HTTP error codes — the caller decides.
        """
        token = self._get_token()
        req = Request(url, method=method, data=body)
        req.add_header("Authorization", f"Bearer {token}")
        req.add_header("Accept", "application/json")
        if body:
            req.add_header("Content-Type", "application/json")

        try:
            with urlopen(req) as resp:
                headers = {k.lower(): v for k, v in resp.getheaders()}
                return resp.status, headers, resp.read()
        except HTTPError as exc:
            headers = {k.lower(): v for k, v in exc.headers.items()}
            body_bytes = exc.read()
            return exc.code, headers, body_bytes
        except URLError as exc:
            raise FabricRestError(
                f"Network error: {str(exc.reason)}", status_code=0,
            ) from exc

    # ── GET with retry ────────────────────────────────────────────

    def get(self, url: str) -> Dict[str, Any]:
        """HTTP GET with automatic retry on 429 and 5xx.

        Returns the parsed JSON response body.

        Raises
        ------
        FabricRestError
            On authentication failure (401/403), not-found (404),
            or if all retries are exhausted.
        """
        for attempt in range(self._max_retries):
            status, headers, body = self._request(url)

            # -- 429 Too Many Requests → respect Retry-After ------
            if status == 429:
                retry_after = self._parse_retry_after(
                    headers.get("retry-after", ""), default=30,
                )
                if attempt < self._max_retries - 1:
                    self._log(
                        f"  ⚠ HTTP 429 throttled — retrying in "
                        f"{retry_after}s (attempt {attempt + 1}/"
                        f"{self._max_retries})..."
                    )
                    time.sleep(retry_after)
                    continue
                raise FabricRestError(
                    f"Throttled after {self._max_retries} retries",
                    status_code=429,
                )

            # -- 401 / 403 → auth error (do not retry) -----------
            if status in (401, 403):
                detail = self._parse_error_message(body)
                raise FabricRestError(
                    f"Authentication/authorization error "
                    f"(HTTP {status}). {detail}",
                    status_code=status,
                    error_code=(
                        "Unauthorized" if status == 401 else "Forbidden"
                    ),
                )

            # -- 404 → resource not found -------------------------
            if status == 404:
                raise FabricRestError(
                    f"Resource not found (HTTP 404): {url}",
                    status_code=404,
                    error_code="NotFound",
                )

            # -- 5xx → server error (retry with back-off) ---------
            if status >= 500:
                if attempt < self._max_retries - 1:
                    wait = 4 * 2 ** attempt
                    self._log(
                        f"  ⚠ Server error {status} — retrying in {wait}s..."
                    )
                    time.sleep(wait)
                    continue
                raise FabricRestError(
                    f"Server error after {self._max_retries} retries "
                    f"(HTTP {status})",
                    status_code=status,
                )

            # -- 2xx → success ------------------------------------
            if 200 <= status < 300:
                if not body:
                    return {}
                return json.loads(body)

            # -- Unexpected status --------------------------------
            raise FabricRestError(
                f"Unexpected HTTP {status} from {url}",
                status_code=status,
            )

        raise FabricRestError("Request failed after all retries")

    # ── GET with pagination ───────────────────────────────────────

    def get_paginated(self, url: str) -> List[Dict[str, Any]]:
        """HTTP GET with automatic Fabric pagination.

        Follows ``continuationUri`` until all pages are collected.

        Returns the concatenated ``value`` arrays from all pages.
        """
        all_items: List[Dict[str, Any]] = []
        current_url: Optional[str] = url

        while current_url:
            data = self.get(current_url)
            items = data.get("value", [])
            all_items.extend(items)
            current_url = data.get("continuationUri")

        return all_items

    # ── Long Running Operation polling ────────────────────────────

    def poll_lro(
        self,
        operation_id: str,
        poll_interval: int = 5,
        max_polls: int = 60,
    ) -> Dict[str, Any]:
        """Poll a Long Running Operation until completion.

        Parameters
        ----------
        operation_id : str
            The operation ID from the ``x-ms-operation-id`` response
            header.
        poll_interval : int
            Default seconds between polls (overridden by Retry-After).
        max_polls : int
            Maximum number of poll iterations before giving up.

        Returns
        -------
        dict
            The final operation state (or result if available).

        Raises
        ------
        FabricRestError
            If the operation fails, is cancelled, or times out.
        """
        state_url = f"{_FABRIC_API_BASE}/operations/{operation_id}"

        for _ in range(max_polls):
            status, headers, body = self._request(state_url)

            if status == 429:
                retry_after = self._parse_retry_after(
                    headers.get("retry-after", ""), default=poll_interval,
                )
                time.sleep(retry_after)
                continue

            if status != 200:
                raise FabricRestError(
                    f"LRO poll failed with HTTP {status}",
                    status_code=status,
                )

            data = json.loads(body) if body else {}
            op_status = data.get("status", "").lower()

            if op_status in ("succeeded", "completed"):
                result_url = (
                    f"{_FABRIC_API_BASE}/operations/{operation_id}/result"
                )
                try:
                    return self.get(result_url)
                except FabricRestError:
                    return data

            if op_status in ("failed", "cancelled"):
                error = data.get("error", {})
                raise FabricRestError(
                    f"LRO {op_status}: "
                    f"{error.get('message', 'unknown error')}",
                    error_code=error.get("code", ""),
                )

            # Still running — wait and poll again
            retry_after = self._parse_retry_after(
                headers.get("retry-after", ""), default=poll_interval,
            )
            self._log(
                f"  ⏳ LRO status: {op_status} — polling in "
                f"{retry_after}s..."
            )
            time.sleep(retry_after)

        raise FabricRestError(
            f"LRO timed out after {max_polls} poll iterations"
        )

    # ── Convenience methods for specific Fabric APIs ──────────────

    def get_workspace_role_assignments(
        self,
        workspace_id: str,
    ) -> List[Dict[str, Any]]:
        """Retrieve all workspace role assignments (paginated).

        ``GET /v1/workspaces/{id}/roleAssignments``

        Requires **Member** or higher workspace role.
        """
        url = (
            f"{_FABRIC_API_BASE}/workspaces/{workspace_id}/roleAssignments"
        )
        return self.get_paginated(url)

    def get_network_communication_policy(
        self,
        workspace_id: str,
    ) -> Dict[str, Any]:
        """Retrieve the workspace network communication policy.

        ``GET /v1/workspaces/{id}/networking/communicationPolicy``

        Requires **Viewer** or higher workspace role.
        """
        url = (
            f"{_FABRIC_API_BASE}/workspaces/{workspace_id}"
            f"/networking/communicationPolicy"
        )
        return self.get(url)

    def list_warehouses(
        self,
        workspace_id: str,
    ) -> List[Dict[str, Any]]:
        """List all warehouses in a workspace (paginated).

        ``GET /v1/workspaces/{id}/warehouses``

        Requires **Viewer** or higher workspace role.
        """
        url = f"{_FABRIC_API_BASE}/workspaces/{workspace_id}/warehouses"
        return self.get_paginated(url)

    def resolve_warehouse_id(
        self,
        workspace_id: str,
        warehouse_name: str,
    ) -> Optional[str]:
        """Resolve a warehouse display name to its item ID.

        Calls :meth:`list_warehouses` and matches by ``displayName``
        (case-insensitive).  Returns ``None`` if no match is found.

        Parameters
        ----------
        workspace_id : str
            Target workspace ID.
        warehouse_name : str
            Display name of the warehouse.

        Returns
        -------
        str or None
            The warehouse item ID (GUID) or ``None``.
        """
        warehouses = self.list_warehouses(workspace_id)
        name_lower = warehouse_name.lower()
        for wh in warehouses:
            if wh.get("displayName", "").lower() == name_lower:
                return wh.get("id")
        return None

    def resolve_warehouse(
        self,
        workspace_id: str,
        warehouse_name: str,
    ) -> Optional[Dict[str, Any]]:
        """Resolve a warehouse display name to its full API object.

        Like :meth:`resolve_warehouse_id` but returns the entire
        warehouse dict (including ``sensitivityLabel``, ``id``,
        ``displayName``, etc.).

        Returns ``None`` if no match is found.
        """
        warehouses = self.list_warehouses(workspace_id)
        name_lower = warehouse_name.lower()
        for wh in warehouses:
            if wh.get("displayName", "").lower() == name_lower:
                return wh
        return None

    def get_sql_audit_settings(
        self,
        workspace_id: str,
        warehouse_id: str,
    ) -> Dict[str, Any]:
        """Retrieve SQL audit settings for a warehouse.

        ``GET /v1/workspaces/{workspaceId}/warehouses/{itemId}/settings/sqlAudit``

        Requires **Reader** or higher item permission.

        Returns
        -------
        dict
            Keys: ``state`` ("Enabled"/"Disabled"),
            ``retentionDays`` (int, 0 = indefinite),
            ``auditActionsAndGroups`` (list[str]).
        """
        url = (
            f"{_FABRIC_API_BASE}/workspaces/{workspace_id}"
            f"/warehouses/{warehouse_id}/settings/sqlAudit"
        )
        return self.get(url)

    # ── SQL Endpoint APIs ─────────────────────────────────────────

    def list_sql_endpoints(
        self,
        workspace_id: str,
    ) -> List[Dict[str, Any]]:
        """List all SQL endpoints in a workspace (paginated).

        ``GET /v1/workspaces/{workspaceId}/sqlEndpoints``

        Requires **Viewer** or higher workspace role.
        """
        url = f"{_FABRIC_API_BASE}/workspaces/{workspace_id}/sqlEndpoints"
        return self.get_paginated(url)

    def resolve_sql_endpoint(
        self,
        workspace_id: str,
        endpoint_name: str,
    ) -> Optional[Dict[str, Any]]:
        """Resolve a SQL endpoint display name to its full API object.

        Calls :meth:`list_sql_endpoints` and matches by
        ``displayName`` (case-insensitive).  Returns the entire dict
        (including ``sensitivityLabel``, ``id``, ``displayName``, etc.)
        or ``None`` if no match is found.
        """
        endpoints = self.list_sql_endpoints(workspace_id)
        name_lower = endpoint_name.lower()
        for ep in endpoints:
            if ep.get("displayName", "").lower() == name_lower:
                return ep
        return None

    def get_sql_endpoint_audit_settings(
        self,
        workspace_id: str,
        item_id: str,
    ) -> Dict[str, Any]:
        """Retrieve SQL audit settings for a SQL endpoint.

        ``GET /v1/workspaces/{workspaceId}/sqlEndpoints/{itemId}/settings/sqlAudit``

        Requires **Reader** or higher item permission.

        Returns
        -------
        dict
            Keys: ``state`` ("Enabled"/"Disabled"),
            ``retentionDays`` (int, 0 = indefinite),
            ``auditActionsAndGroups`` (list[str]).
        """
        url = (
            f"{_FABRIC_API_BASE}/workspaces/{workspace_id}"
            f"/sqlEndpoints/{item_id}/settings/sqlAudit"
        )
        return self.get(url)

    def list_item_access_details(
        self,
        workspace_id: str,
        item_id: str,
        item_type: str = "Warehouse",
    ) -> Dict[str, Any]:
        """Retrieve item-level access details via the **Admin API**.

        ``GET /v1/admin/workspaces/{workspaceId}/items/{itemId}/users``

        .. important::

           Requires **Fabric Administrator** role or a service
           principal with ``Tenant.Read.All`` scope.

        Parameters
        ----------
        workspace_id : str
            Target workspace ID.
        item_id : str
            Target item (warehouse) ID.
        item_type : str
            Optional item type filter (default ``"Warehouse"``).

        Returns
        -------
        dict
            ``{"accessDetails": [ {"principal": …, "itemAccessDetails": …} ]}``

        Raises
        ------
        FabricRestError
            On HTTP 401/403 if the caller is not an admin, or any
            other API error.
        """
        url = (
            f"{_FABRIC_API_BASE}/admin/workspaces/{workspace_id}"
            f"/items/{item_id}/users"
        )
        if item_type:
            url += f"?type={item_type}"
        return self.get(url)

    # ── Private helpers ───────────────────────────────────────────

    @staticmethod
    def _parse_error_message(body: bytes) -> str:
        """Extract a human-readable message from a Fabric error body."""
        if not body:
            return ""
        try:
            err = json.loads(body)
            return err.get("message", "") or err.get("errorCode", "")
        except (json.JSONDecodeError, KeyError):
            return ""
