import os
import re
import logging

from fastapi import APIRouter, HTTPException

from models import DashboardRequest
from services.mcp_client import MCPClient
from services.dashboard import get_dashboard_data

logger = logging.getLogger("powerbi-advisor")

router = APIRouter()


@router.post("/dashboard-data")
async def dashboard_data(req: DashboardRequest):
    """Query the connected Power BI Desktop model and return chart-ready data
    with Python matplotlib scripts for PBI Python visuals."""
    if not os.path.isfile(req.mcp_exe_path):
        raise HTTPException(400, f"MCP server not found at: {req.mcp_exe_path}")

    mcp = MCPClient(req.mcp_exe_path)

    try:
        # Start MCP and connect to PBI Desktop
        await mcp.start()

        instances_resp = await mcp.list_local_instances()
        instances_text = mcp._extract_text(instances_resp)
        port_match = re.search(r"localhost:(\d+)", instances_text)
        if not port_match:
            raise RuntimeError(
                "No running Power BI Desktop instance found. "
                "Open PBI Desktop and load your data first."
            )

        await mcp.connect(port_match.group(0))

        # Get dashboard data
        result = await get_dashboard_data(mcp)
        result["success"] = True
        return result

    except Exception as e:
        err_msg = str(e) or repr(e)
        logger.error(f"Dashboard data failed: {err_msg}", exc_info=True)
        return {
            "success": False,
            "kpis": [],
            "charts": [],
            "python_scripts": [],
            "errors": [err_msg],
        }
    finally:
        await mcp.stop()
