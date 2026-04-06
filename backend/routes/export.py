import json
import os
import tempfile
import uuid

from fastapi import APIRouter
from fastapi.responses import FileResponse

from models import ReportRequest, PbipRequest
from services.exporter import generate_html_report, _build_bim, _build_te_script, _build_pbip_zip

router = APIRouter()


@router.post("/generate-report")
async def generate_report(data: ReportRequest):
    html_content = generate_html_report(data)
    tmp_dir = tempfile.gettempdir()
    filename = f"rapport_powerbi_{uuid.uuid4().hex[:8]}.html"
    filepath = os.path.join(tmp_dir, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(html_content)
    return FileResponse(path=filepath, media_type="text/html",
        filename=f"rapport_powerbi_{data.filename.replace('.xlsx','').replace('.csv','')}.html",
        headers={"Content-Disposition": "attachment; filename=rapport_powerbi.html"})


@router.post("/generate-bim")
async def generate_bim(data: ReportRequest):
    """Generate a downloadable .bim (Tabular Model) file."""
    bim = _build_bim(data, session_id=None)
    tmp_dir = tempfile.gettempdir()
    filename = f"model_{uuid.uuid4().hex[:8]}.bim"
    filepath = os.path.join(tmp_dir, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(bim, f, indent=2, ensure_ascii=False)
    safe_name = data.filename.replace(".xlsx", "").replace(".xls", "").replace(".csv", "")
    return FileResponse(
        path=filepath,
        media_type="application/json",
        filename=f"{safe_name}.bim",
        headers={"Content-Disposition": f'attachment; filename="{safe_name}.bim"'},
    )


@router.post("/generate-te-script")
async def generate_te_script(data: ReportRequest):
    """Generate a downloadable Tabular Editor C# script."""
    script = _build_te_script(data)
    tmp_dir = tempfile.gettempdir()
    filename = f"setup_{uuid.uuid4().hex[:8]}.csx"
    filepath = os.path.join(tmp_dir, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(script)
    safe_name = data.filename.replace(".xlsx", "").replace(".xls", "").replace(".csv", "")
    return FileResponse(
        path=filepath,
        media_type="text/plain",
        filename=f"{safe_name}_setup.csx",
        headers={"Content-Disposition": f'attachment; filename="{safe_name}_setup.csx"'},
    )


@router.post("/generate-pbip")
async def generate_pbip(req: PbipRequest):
    """Generate a downloadable PBIP (Power BI Project) as a ZIP file."""
    zip_path = _build_pbip_zip(req.model, req.session_id, req.file_path)
    safe_name = req.model.filename.replace(".xlsx", "").replace(".xls", "").replace(".csv", "").replace(" ", "_")
    return FileResponse(
        path=zip_path,
        media_type="application/zip",
        filename=f"{safe_name}_PowerBI.zip",
        headers={"Content-Disposition": f'attachment; filename="{safe_name}_PowerBI.zip"'},
    )
