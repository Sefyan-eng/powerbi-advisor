import httpx
from fastapi import APIRouter, HTTPException

from models import PushRequest
from services.analyzer import _session_data
from services.powerbi_api import _get_powerbi_token, _build_rest_dataset

router = APIRouter()


@router.post("/push-to-powerbi")
async def push_to_powerbi(req: PushRequest):
    """Create a push dataset in a Power BI workspace, optionally push data rows."""
    token = await _get_powerbi_token(req.config)
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    base_url = f"https://api.powerbi.com/v1.0/myorg/groups/{req.config.workspace_id}"

    dataset_payload = _build_rest_dataset(req.model, req.session_id)

    async with httpx.AsyncClient(timeout=60) as client:
        # 1. Create the dataset
        resp = await client.post(
            f"{base_url}/datasets?defaultRetentionPolicy=basicFIFO",
            headers=headers,
            json=dataset_payload,
        )
        if resp.status_code not in (200, 201, 202):
            raise HTTPException(resp.status_code, f"Failed to create dataset: {resp.text}")

        ds_result = resp.json()
        dataset_id = ds_result.get("id")

        # 2. Optionally push data rows from the stored session
        rows_pushed = {}
        if req.push_data and req.session_id and req.session_id in _session_data:
            frames = _session_data[req.session_id]
            for t in req.model.tables:
                source_df = frames.get(t.source_sheet)
                if source_df is None:
                    continue
                # Only push columns that exist in the model
                available_cols = [c for c in t.columns if c in source_df.columns]
                if not available_cols:
                    continue
                subset = source_df[available_cols].copy()
                # Convert datetime to ISO string for JSON
                for col in subset.select_dtypes(include=["datetime64"]).columns:
                    subset[col] = subset[col].dt.strftime("%Y-%m-%dT%H:%M:%S")
                subset = subset.fillna("")

                # Push in batches of 10,000 rows
                total_rows = len(subset)
                batch_size = 10_000
                pushed = 0
                for start in range(0, total_rows, batch_size):
                    batch = subset.iloc[start:start + batch_size]
                    rows = batch.to_dict(orient="records")
                    push_resp = await client.post(
                        f"{base_url}/datasets/{dataset_id}/tables/{t.name}/rows",
                        headers=headers,
                        json={"rows": rows},
                    )
                    if push_resp.status_code in (200, 201):
                        pushed += len(rows)
                rows_pushed[t.name] = pushed

    return {
        "success": True,
        "dataset_id": dataset_id,
        "dataset_name": dataset_payload["name"],
        "workspace_id": req.config.workspace_id,
        "url": f"https://app.powerbi.com/groups/{req.config.workspace_id}/datasets/{dataset_id}",
        "rows_pushed": rows_pushed,
    }
