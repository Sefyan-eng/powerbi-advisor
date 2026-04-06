from pydantic import BaseModel
from typing import List, Optional, Dict, Any


class Table(BaseModel):
    name: str
    type: str
    source_sheet: str
    columns: List[str]
    primary_key: Optional[str] = None
    description: str = ""


class Relationship(BaseModel):
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    cardinality: str
    cross_filter: str


class Measure(BaseModel):
    name: str
    dax: str
    description: str = ""


class ReportRequest(BaseModel):
    filename: str
    model_type: str
    summary: str
    tables: List[Table]
    relationships: List[Relationship]
    measures_suggested: List[Measure]
    warnings: List[str] = []
    best_practices: List[str] = []


class PowerBIConfig(BaseModel):
    tenant_id: str
    client_id: str
    client_secret: str
    workspace_id: str


class PushRequest(BaseModel):
    config: PowerBIConfig
    model: ReportRequest
    session_id: Optional[str] = None
    push_data: bool = False


class PbipRequest(BaseModel):
    model: ReportRequest
    session_id: Optional[str] = None
    file_path: Optional[str] = None


class DeployRequest(BaseModel):
    model: ReportRequest
    mcp_exe_path: str  # e.g. "C:\\MCPServers\\PowerBIModelingMCP\\...\\powerbi-modeling-mcp.exe"
    pbi_port: Optional[int] = None  # local AS port if known


class PromptRequest(BaseModel):
    prompt: str
    mcp_exe_path: str
    conversation: List[Dict[str, str]] = []  # prior messages for multi-turn


class DashboardRequest(BaseModel):
    mcp_exe_path: str
    model: Optional[ReportRequest] = None
