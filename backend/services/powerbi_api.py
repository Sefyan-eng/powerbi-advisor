from typing import Optional

import httpx
from fastapi import HTTPException

from models import PowerBIConfig, ReportRequest, Table
from services.exporter import (
    PANDAS_TO_REST_DTYPE,
    CROSS_FILTER_REST_MAP,
    _resolve_column_dtypes,
)


async def _get_powerbi_token(config: PowerBIConfig) -> str:
    """Acquire an access token via client credentials flow."""
    token_url = f"https://login.microsoftonline.com/{config.tenant_id}/oauth2/v2.0/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": config.client_id,
        "client_secret": config.client_secret,
        "scope": "https://analysis.windows.net/powerbi/api/.default",
    }
    async with httpx.AsyncClient() as client:
        resp = await client.post(token_url, data=payload)
        if resp.status_code != 200:
            raise HTTPException(401, f"Azure AD auth failed: {resp.text}")
        return resp.json()["access_token"]


def _build_rest_dataset(data: ReportRequest, session_id: Optional[str] = None) -> dict:
    """Build a Power BI REST API push-dataset payload."""
    tables_rest = []
    measures_assigned = False

    for t in data.tables:
        col_dtypes = _resolve_column_dtypes(t, session_id)
        columns = []
        for col_name in t.columns:
            pandas_dt = col_dtypes.get(col_name, "object")
            rest_dt = PANDAS_TO_REST_DTYPE.get(pandas_dt, "String")
            columns.append({"name": col_name, "dataType": rest_dt})

        table_obj = {"name": t.name, "columns": columns}

        # Attach measures to first Fact table (or first table)
        if not measures_assigned and (t.type == "Fact" or not any(tt.type == "Fact" for tt in data.tables)):
            table_obj["measures"] = [
                {"name": m.name, "expression": m.dax, **({"description": m.description} if m.description else {})}
                for m in data.measures_suggested
            ]
            measures_assigned = True

        tables_rest.append(table_obj)

    # If measures not yet assigned (all tables processed but none was Fact), assign to first
    if not measures_assigned and data.measures_suggested and tables_rest:
        tables_rest[0]["measures"] = [
            {"name": m.name, "expression": m.dax, **({"description": m.description} if m.description else {})}
            for m in data.measures_suggested
        ]

    relationships_rest = []
    for r in data.relationships:
        cf = CROSS_FILTER_REST_MAP.get(r.cross_filter, "OneDirection")
        relationships_rest.append({
            "name": f"{r.from_table}_{r.from_column}_{r.to_table}_{r.to_column}",
            "fromTable": r.from_table,
            "fromColumn": r.from_column,
            "toTable": r.to_table,
            "toColumn": r.to_column,
            "crossFilteringBehavior": cf,
        })

    model_name = data.filename.replace(".xlsx", "").replace(".xls", "").replace(".csv", "").replace(" ", "_")

    return {
        "name": model_name,
        "defaultMode": "Push",
        "tables": tables_rest,
        "relationships": relationships_rest,
    }
