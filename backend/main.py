from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import pandas as pd
import anthropic
import httpx
import json, io, os, re, tempfile, uuid, zipfile, asyncio, subprocess, threading
from datetime import datetime
from pathlib import Path
import logging

# Auto-load .env from project root (works on Windows without bash)
_env_file = Path(__file__).resolve().parent.parent / ".env"
if _env_file.exists():
    for line in _env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

logger = logging.getLogger("powerbi-advisor")

app = FastAPI(title="Power BI Model Advisor")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

MAX_FILE_SIZE = 20 * 1024 * 1024

# ── In-memory store for parsed Excel data (keyed by analysis session) ──
_session_data: Dict[str, Dict[str, pd.DataFrame]] = {}


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

def get_client():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise HTTPException(500, "ANTHROPIC_API_KEY non définie.")
    return anthropic.Anthropic(api_key=api_key)

def analyze_excel(file_bytes: bytes) -> dict:
    all_sheets = pd.read_excel(io.BytesIO(file_bytes), sheet_name=None)
    schema = {}
    for sheet_name, df in all_sheets.items():
        cols = []
        for col in df.columns:
            sample_vals = df[col].dropna().head(3).tolist()
            cols.append({
                "name": str(col),
                "dtype": str(df[col].dtype),
                "nulls": int(df[col].isnull().sum()),
                "unique": int(df[col].nunique()),
                "sample": [str(v) for v in sample_vals],
                "rows": len(df),
            })
        schema[sheet_name] = cols
    return schema

def parse_json_response(raw: str) -> dict:
    cleaned = re.sub(r"^```(?:json)?\s*", "", raw.strip(), flags=re.MULTILINE)
    cleaned = re.sub(r"\s*```$", "", cleaned.strip(), flags=re.MULTILINE)
    return json.loads(cleaned.strip())

def build_prompt(schema: dict) -> str:
    schema_txt = json.dumps(schema, ensure_ascii=False, indent=2)
    return f"""Tu es un expert Power BI et modélisation de données.
Voici le schéma d'un fichier Excel :

{schema_txt}

Réponds UNIQUEMENT en JSON valide (sans markdown) :

{{
  "model_type": "Star Schema | Snowflake Schema | Flat Table | Composite",
  "summary": "Explication concise",
  "tables": [
    {{"name":"NomTable","type":"Fact|Dimension|Bridge","source_sheet":"Feuille","columns":["col1"],"primary_key":"col_pk ou null","description":"rôle"}}
  ],
  "relationships": [
    {{"from_table":"T1","from_column":"col","to_table":"T2","to_column":"col","cardinality":"Many-to-One","cross_filter":"Single"}}
  ],
  "measures_suggested": [
    {{"name":"Mesure","dax":"DAX formula","description":"desc"}}
  ],
  "warnings": ["warning"],
  "best_practices": ["conseil"]
}}"""

def generate_html_report(data: ReportRequest) -> str:
    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    tables_html = ""
    for t in data.tables:
        type_color = {"Fact": "#ff6b35", "Dimension": "#4fc3f7", "Bridge": "#ab47bc"}.get(t.type, "#888")
        pk_col = t.primary_key or ""
        cols_html = "".join([
            f'<tr><td style="font-family:monospace;padding:6px 12px;border-bottom:1px solid #2a2a3a;">{"🔑 " if c == pk_col else ""}{c}</td>'
            f'<td style="padding:6px 12px;border-bottom:1px solid #2a2a3a;color:#888;font-size:12px;">{"PRIMARY KEY" if c == pk_col else ""}</td></tr>'
            for c in t.columns
        ])
        tables_html += f"""
        <div style="background:#111118;border:1px solid #2a2a3a;border-radius:12px;overflow:hidden;margin-bottom:16px;">
          <div style="background:#18181f;padding:14px 18px;display:flex;align-items:center;gap:10px;border-bottom:1px solid #2a2a3a;">
            <span style="background:{type_color}22;color:{type_color};font-size:10px;padding:3px 8px;border-radius:4px;font-weight:600;text-transform:uppercase;">{t.type}</span>
            <span style="font-weight:700;font-size:16px;">{t.name}</span>
            <span style="margin-left:auto;font-size:11px;color:#888;">📄 {t.source_sheet}</span>
          </div>
          <table style="width:100%;border-collapse:collapse;">{cols_html}</table>
          <div style="padding:10px 18px;border-top:1px solid #2a2a3a;color:#888;font-size:12px;">{t.description}</div>
        </div>"""

    rels_html = "".join([
        f'<div style="background:#111118;border:1px solid #2a2a3a;border-radius:10px;padding:14px 18px;display:flex;align-items:center;gap:10px;flex-wrap:wrap;font-family:monospace;font-size:13px;margin-bottom:10px;">'
        f'<span style="font-weight:500;">{r.from_table}</span><span style="color:#f7c52e;">[{r.from_column}]</span>'
        f'<span style="color:#888;font-size:18px;">→</span>'
        f'<span style="font-weight:500;">{r.to_table}</span><span style="color:#f7c52e;">[{r.to_column}]</span>'
        f'<span style="margin-left:auto;font-size:10px;color:#888;border:1px solid #2a2a3a;padding:2px 8px;border-radius:20px;">{r.cardinality} · {r.cross_filter}</span></div>'
        for r in data.relationships
    ])

    measures_html = "".join([
        f'<div style="background:#111118;border:1px solid #2a2a3a;border-radius:10px;overflow:hidden;margin-bottom:12px;">'
        f'<div style="background:#18181f;padding:12px 16px;border-bottom:1px solid #2a2a3a;"><div style="font-weight:700;font-size:14px;">📐 {m.name}</div><div style="color:#888;font-size:12px;">{m.description}</div></div>'
        f'<div style="padding:12px 16px;font-family:monospace;font-size:12px;color:#7ec8e3;background:#0d1117;white-space:pre-wrap;">{m.dax}</div></div>'
        for m in data.measures_suggested
    ])

    warn_html = "".join([f'<div style="background:rgba(255,107,53,.08);border:1px solid rgba(255,107,53,.2);border-radius:8px;padding:12px 16px;margin-bottom:8px;">⚠️ {w}</div>' for w in data.warnings])
    tips_html = "".join([f'<div style="background:rgba(102,187,106,.08);border:1px solid rgba(102,187,106,.2);border-radius:8px;padding:12px 16px;margin-bottom:8px;">✅ {b}</div>' for b in data.best_practices])

    return f"""<!DOCTYPE html>
<html lang="fr"><head><meta charset="UTF-8"/>
<title>Rapport Power BI — {data.filename}</title>
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@700;800&family=DM+Mono&family=Inter:wght@400;500&display=swap" rel="stylesheet"/>
<style>*{{box-sizing:border-box;margin:0;padding:0;}}body{{background:#0a0a0f;color:#e8e8f0;font-family:'Inter',sans-serif;padding:40px 24px;}}body::before{{content:'';position:fixed;inset:0;background-image:linear-gradient(rgba(247,197,46,0.03) 1px,transparent 1px),linear-gradient(90deg,rgba(247,197,46,0.03) 1px,transparent 1px);background-size:40px 40px;pointer-events:none;}}h2{{font-family:'Syne',sans-serif;font-size:13px;text-transform:uppercase;letter-spacing:2px;color:#888;margin:40px 0 16px;}}.container{{max-width:1100px;margin:0 auto;position:relative;}}.print-btn{{position:fixed;bottom:24px;right:24px;background:#f7c52e;color:#000;border:none;padding:12px 24px;border-radius:10px;font-weight:700;cursor:pointer;font-size:14px;box-shadow:0 8px 24px rgba(247,197,46,.3);}}@media print{{.print-btn{{display:none;}}}}</style>
</head><body><div class="container">
<div style="display:flex;align-items:center;gap:16px;padding-bottom:24px;border-bottom:1px solid #2a2a3a;margin-bottom:32px;">
  <div style="width:48px;height:48px;background:#f7c52e;border-radius:10px;display:flex;align-items:center;justify-content:center;font-family:'DM Mono',monospace;font-weight:500;color:#000;">PB</div>
  <div><div style="font-family:'Syne',sans-serif;font-weight:800;font-size:24px;">Rapport Power BI</div><div style="color:#888;font-size:13px;">📄 {data.filename} · {now}</div></div>
  <div style="margin-left:auto;background:rgba(247,197,46,.1);border:1px solid rgba(247,197,46,.3);border-radius:10px;padding:10px 20px;text-align:center;">
    <div style="font-size:10px;color:#888;text-transform:uppercase;">Modèle</div>
    <div style="font-family:'Syne',sans-serif;font-weight:800;font-size:18px;color:#f7c52e;">{data.model_type}</div>
  </div>
</div>
<h2>Résumé</h2>
<div style="background:#111118;border:1px solid #2a2a3a;border-radius:12px;padding:20px;color:#aaa;font-size:15px;line-height:1.7;margin-bottom:32px;">{data.summary}</div>
<h2>Tables ({len(data.tables)})</h2>
<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:16px;margin-bottom:32px;">{tables_html}</div>
<h2>Relations ({len(data.relationships)})</h2>
<div style="margin-bottom:32px;">{rels_html}</div>
<h2>Mesures DAX ({len(data.measures_suggested)})</h2>
<div style="margin-bottom:32px;">{measures_html}</div>
<h2>Avertissements</h2><div style="margin-bottom:32px;">{warn_html}</div>
<h2>Bonnes pratiques</h2><div style="margin-bottom:32px;">{tips_html}</div>
<div style="border-top:1px solid #2a2a3a;padding-top:24px;display:flex;justify-content:space-between;color:#555;font-size:12px;"><span>Power BI Model Advisor · Claude AI</span><span>{now}</span></div>
</div>
<button class="print-btn" onclick="window.print()">🖨️ Imprimer / PDF</button>
</body></html>"""

@app.post("/analyze")
async def analyze(file: UploadFile = File(...)):
    if not file.filename.endswith((".xlsx", ".xls", ".csv")):
        raise HTTPException(400, "Fichier non supporté.")
    content = await file.read()
    if len(content) > MAX_FILE_SIZE:
        raise HTTPException(413, "Fichier trop volumineux (max 20 MB).")

    session_id = uuid.uuid4().hex[:12]

    try:
        if file.filename.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(content))
            frames = {"Sheet1": df}
            schema = {"Sheet1": [{"name": c, "dtype": str(df[c].dtype), "nulls": int(df[c].isnull().sum()),
                "unique": int(df[c].nunique()), "sample": [str(v) for v in df[c].dropna().head(3).tolist()],
                "rows": len(df)} for c in df.columns]}
        else:
            frames = pd.read_excel(io.BytesIO(content), sheet_name=None)
            schema = analyze_excel(content)
    except Exception as e:
        raise HTTPException(400, f"Erreur lecture : {str(e)}")

    # Store parsed DataFrames for later push-to-powerbi
    _session_data[session_id] = frames

    client = get_client()
    message = client.messages.create(
        model="claude-sonnet-4-5",
        max_tokens=4096,
        messages=[{"role": "user", "content": build_prompt(schema)}]
    )
    try:
        result = parse_json_response(message.content[0].text)
    except Exception as e:
        raise HTTPException(500, f"Erreur parsing : {str(e)}")

    # ── Validate AI output against real data ──
    # Build set of all real column names across all sheets
    all_real_columns: Dict[str, set] = {}  # sheet_name -> {col1, col2, ...}
    all_columns_flat: set = set()
    for sheet_name, col_list in schema.items():
        cols = {c["name"] for c in col_list}
        all_real_columns[sheet_name] = cols
        all_columns_flat.update(cols)
    real_sheet_names = set(schema.keys())

    # Validate tables: fix source_sheet, strip fake columns
    validated_tables = []
    valid_table_names = set()
    for t in result.get("tables", []):
        src = t.get("source_sheet", "")
        # Fix source_sheet if it doesn't exist
        if src not in real_sheet_names:
            src = list(real_sheet_names)[0] if real_sheet_names else src
            t["source_sheet"] = src
        # Only keep columns that exist in the source sheet (or any sheet)
        sheet_cols = all_real_columns.get(src, all_columns_flat)
        real_cols = [c for c in t.get("columns", []) if c in sheet_cols]
        if not real_cols:
            # Table has zero real columns — skip it entirely
            continue
        t["columns"] = real_cols
        # Fix primary_key if it doesn't exist
        if t.get("primary_key") and t["primary_key"] not in real_cols:
            t["primary_key"] = None
        validated_tables.append(t)
        valid_table_names.add(t["name"])
    result["tables"] = validated_tables

    # Validate relationships: only keep if both tables and columns exist
    validated_rels = []
    table_columns = {t["name"]: set(t["columns"]) for t in validated_tables}
    for r in result.get("relationships", []):
        ft, fc = r.get("from_table"), r.get("from_column")
        tt, tc = r.get("to_table"), r.get("to_column")
        if (ft in table_columns and fc in table_columns[ft] and
                tt in table_columns and tc in table_columns[tt]):
            validated_rels.append(r)
    result["relationships"] = validated_rels

    # Validate measures: check DAX for references to non-existent table[column] pairs
    validated_measures = []
    # Pattern matches Table[Column] references in DAX
    dax_ref_pattern = re.compile(r"(\w+)\[(\w+)\]")
    for m in result.get("measures_suggested", []):
        dax = m.get("dax", "")
        refs = dax_ref_pattern.findall(dax)
        bad_refs = []
        for tbl, col in refs:
            if tbl in table_columns and col not in table_columns[tbl]:
                bad_refs.append(f"{tbl}[{col}]")
        if bad_refs:
            # Rewrite the measure description to warn, but still include it
            m.setdefault("description", "")
            m["description"] += f" ⚠️ DAX references unknown columns: {', '.join(bad_refs)}"
            result.setdefault("warnings", []).append(
                f"Measure '{m.get('name')}' references columns not in source data: {', '.join(bad_refs)}"
            )
        validated_measures.append(m)
    result["measures_suggested"] = validated_measures

    result["schema"] = schema
    result["filename"] = file.filename
    result["session_id"] = session_id
    return result

@app.post("/generate-report")
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

@app.get("/health")
def health():
    return {"status": "ok", "api_key_configured": bool(os.environ.get("ANTHROPIC_API_KEY"))}


# ── Power BI Integration ─────────────────────────────────────────────────────

PANDAS_TO_BIM_DTYPE = {
    "int64": "int64", "int32": "int64", "int16": "int64", "int8": "int64",
    "uint64": "int64", "uint32": "int64", "uint16": "int64", "uint8": "int64",
    "float64": "double", "float32": "double",
    "object": "string", "string": "string", "category": "string",
    "bool": "boolean", "boolean": "boolean",
    "datetime64[ns]": "dateTime", "datetime64": "dateTime",
}

PANDAS_TO_REST_DTYPE = {
    "int64": "Int64", "int32": "Int64", "int16": "Int64", "int8": "Int64",
    "uint64": "Int64", "uint32": "Int64", "uint16": "Int64", "uint8": "Int64",
    "float64": "Double", "float32": "Double",
    "object": "String", "string": "String", "category": "String",
    "bool": "Boolean", "boolean": "Boolean",
    "datetime64[ns]": "DateTime", "datetime64": "DateTime",
}

CARDINALITY_MAP = {
    "Many-to-One": ("many", "one"),
    "One-to-Many": ("one", "many"),
    "One-to-One": ("one", "one"),
    "Many-to-Many": ("many", "many"),
}

CROSS_FILTER_MAP = {
    "Single": "oneDirection",
    "Both": "bothDirections",
}

CROSS_FILTER_REST_MAP = {
    "Single": "OneDirection",
    "Both": "BothDirections",
}


def _resolve_column_dtypes(table: Table, session_id: Optional[str]) -> dict:
    """Build {col_name: pandas_dtype_str} from stored session data."""
    dtypes = {}
    frames = _session_data.get(session_id, {}) if session_id else {}
    source_df = frames.get(table.source_sheet)
    for col_name in table.columns:
        if source_df is not None and col_name in source_df.columns:
            dtypes[col_name] = str(source_df[col_name].dtype)
        else:
            dtypes[col_name] = "object"
    return dtypes


def _make_lineage_tag() -> str:
    return str(uuid.uuid4())


def _guess_bim_dtype_from_name(col_name: str) -> str:
    """Infer a reasonable BIM data type from column name heuristics."""
    name = col_name.lower().strip()
    # IDs and keys
    if name.endswith("id") or name.endswith("_id") or name == "id" or name.endswith("key"):
        return "int64"
    # Dates
    if any(kw in name for kw in ("date", "time", "created", "updated", "timestamp", "jour", "mois")):
        return "dateTime"
    # Booleans
    if any(kw in name for kw in ("is_", "has_", "flag", "active", "enabled", "bool")):
        return "boolean"
    # Monetary / numeric
    if any(kw in name for kw in ("amount", "price", "cost", "total", "revenue", "qty", "quantity",
                                   "count", "number", "num", "montant", "prix", "quantite",
                                   "sales", "profit", "margin", "discount", "tax", "weight",
                                   "rate", "score", "percent", "ratio", "budget", "salary")):
        return "double"
    return "string"


def _build_bim(data: ReportRequest, session_id: Optional[str] = None) -> dict:
    """Generate a Tabular Model .bim JSON structure."""
    tables_bim = []
    for t in data.tables:
        col_dtypes = _resolve_column_dtypes(t, session_id)

        columns = []
        for col_name in t.columns:
            pandas_dt = col_dtypes.get(col_name, "object")
            # Use session dtype if available, otherwise infer from column name
            if pandas_dt != "object":
                bim_dt = PANDAS_TO_BIM_DTYPE.get(pandas_dt, "string")
            else:
                bim_dt = _guess_bim_dtype_from_name(col_name)

            col_obj = {
                "name": col_name,
                "dataType": bim_dt,
                "sourceColumn": col_name,
                "lineageTag": _make_lineage_tag(),
                "summarizeBy": "none" if col_name == t.primary_key else ("sum" if bim_dt in ("int64", "double", "decimal") else "none"),
            }
            if col_name == t.primary_key:
                col_obj["isKey"] = True
            columns.append(col_obj)

        # Build M expression — single string with newlines (TE3 compatible)
        col_names_m = ", ".join([f'"{c}"' for c in t.columns])
        m_expr = f'let\n    Source = #table({{{col_names_m}}}, {{}})\nin\n    Source'

        # Measures live on their parent table (first fact table gets all measures)
        measures = []
        if t.type == "Fact":
            for m in data.measures_suggested:
                msr = {
                    "name": m.name,
                    "expression": m.dax,
                    "lineageTag": _make_lineage_tag(),
                }
                if m.description:
                    msr["description"] = m.description
                measures.append(msr)

        table_obj = {
            "name": t.name,
            "description": t.description,
            "lineageTag": _make_lineage_tag(),
            "columns": columns,
            "partitions": [{
                "name": t.name,
                "mode": "import",
                "source": {"type": "m", "expression": m_expr},
            }],
        }
        if measures:
            table_obj["measures"] = measures

        tables_bim.append(table_obj)

    # If no fact table, attach measures to first table
    if data.measures_suggested and not any(t.type == "Fact" for t in data.tables) and tables_bim:
        measures = [{
            "name": m.name,
            "expression": m.dax,
            "lineageTag": _make_lineage_tag(),
            **({"description": m.description} if m.description else {}),
        } for m in data.measures_suggested]
        tables_bim[0].setdefault("measures", []).extend(measures)

    relationships_bim = []
    for i, r in enumerate(data.relationships):
        from_card, to_card = CARDINALITY_MAP.get(r.cardinality, ("many", "one"))
        cf = CROSS_FILTER_MAP.get(r.cross_filter, "oneDirection")
        relationships_bim.append({
            "name": f"{r.from_table}_{r.from_column}_{r.to_table}_{r.to_column}",
            "fromTable": r.from_table,
            "fromColumn": r.from_column,
            "toTable": r.to_table,
            "toColumn": r.to_column,
            "fromCardinality": from_card,
            "toCardinality": to_card,
            "crossFilteringBehavior": cf,
            "isActive": True,
        })

    model_name = data.filename.replace(".xlsx", "").replace(".xls", "").replace(".csv", "").replace(" ", "_")

    return {
        "name": model_name,
        "id": model_name,
        "compatibilityLevel": 1550,
        "model": {
            "culture": "en-US",
            "defaultPowerBIDataSourceVersion": "powerBI_V3",
            "defaultMode": "import",
            "tables": tables_bim,
            "relationships": relationships_bim,
            "annotations": [
                {"name": "PBI_GeneratedBy", "value": "PowerBI-Model-Advisor"},
            ],
        },
    }


@app.post("/generate-bim")
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


def _build_te_script(data: ReportRequest) -> str:
    """Generate a Tabular Editor C# script that creates relationships and measures.

    Uses the verified TE3 scripting API:
    - Column.RelateTo() for relationships (FromTable/ToTable are read-only)
    - Table.AddMeasure(name, expression) for DAX measures
    - Duplicate checks before creating anything
    - No Model.SaveChanges() needed (UI save via Ctrl+S)
    """
    lines = [
        "// ═══════════════════════════════════════════════════════════════",
        "// Power BI Model Advisor — Tabular Editor Setup Script",
        "// ═══════════════════════════════════════════════════════════════",
        "//",
        "// HOW TO USE:",
        "//   1. Open Power BI Desktop → Get Data → Excel → load your file",
        "//   2. Go to External Tools tab → click Tabular Editor",
        "//   3. In Tabular Editor → Advanced Scripting tab (bottom panel)",
        "//   4. Paste this entire script → click Run (▶ play button)",
        "//   5. Press Ctrl+S to save changes back to Power BI Desktop",
        "//",
        "// ═══════════════════════════════════════════════════════════════",
        "",
        "var errors = new System.Collections.Generic.List<string>();",
        "var created = 0;",
        "",
    ]

    # ── Relationships ──
    if data.relationships:
        lines.append("// ── Relationships ──")
        lines.append("")

    for r in data.relationships:
        cf_val = "OneDirection" if r.cross_filter == "Single" else "BothDirections"
        label = f'{r.from_table}[{r.from_column}] → {r.to_table}[{r.to_column}]'
        # Escape C# strings
        ft = r.from_table.replace('"', '\\"')
        fc = r.from_column.replace('"', '\\"')
        tt = r.to_table.replace('"', '\\"')
        tc = r.to_column.replace('"', '\\"')

        lines.append(f'// {label} ({r.cardinality})')
        lines.append("try {")
        # Validate tables and columns exist
        lines.append(f'    if (!Model.Tables.Contains("{ft}")) throw new Exception("Table \\"{ft}\\" not found in model.");')
        lines.append(f'    if (!Model.Tables.Contains("{tt}")) throw new Exception("Table \\"{tt}\\" not found in model.");')
        lines.append(f'    var fromCol = Model.Tables["{ft}"].Columns["{fc}"];')
        lines.append(f'    var toCol = Model.Tables["{tt}"].Columns["{tc}"];')
        lines.append(f'    if (fromCol == null) throw new Exception("Column \\"{fc}\\" not found in table \\"{ft}\\".");')
        lines.append(f'    if (toCol == null) throw new Exception("Column \\"{tc}\\" not found in table \\"{tt}\\".");')
        # Check for duplicate
        lines.append(f'    if (!Model.Relationships.Any(r => r.FromColumn == fromCol && r.ToColumn == toCol)) {{')
        lines.append(f'        var rel = fromCol.RelateTo(toCol);')
        lines.append(f'        rel.CrossFilteringBehavior = CrossFilteringBehavior.{cf_val};')
        lines.append(f'        created++;')
        lines.append(f'        Info("✓ Relationship: {ft}[{fc}] → {tt}[{tc}]");')
        lines.append(f'    }} else {{')
        lines.append(f'        Info("⊘ Relationship already exists: {ft}[{fc}] → {tt}[{tc}]");')
        lines.append(f'    }}')
        lines.append("} catch (Exception ex) {")
        lines.append(f'    errors.Add("Relationship {ft}→{tt}: " + ex.Message);')
        lines.append("}")
        lines.append("")

    # ── DAX Measures ──
    if data.measures_suggested:
        lines.append("// ── DAX Measures ──")
        lines.append("")

    # Find target table for measures: first Fact table, or first table
    measure_table = None
    for t in data.tables:
        if t.type == "Fact":
            measure_table = t.name
            break
    if not measure_table and data.tables:
        measure_table = data.tables[0].name

    if measure_table:
        mt_escaped = measure_table.replace('"', '\\"')
        for m in data.measures_suggested:
            name_escaped = m.name.replace('"', '\\"')
            # Use verbatim string @"..." for DAX — double any internal quotes
            dax_verbatim = m.dax.replace('"', '""')
            desc_verbatim = (m.description or "").replace('"', '""')

            lines.append(f'// Measure: {m.name}')
            lines.append("try {")
            lines.append(f'    var tbl = Model.Tables["{mt_escaped}"];')
            # Check for duplicate measure
            lines.append(f'    if (tbl.Measures.Contains("{name_escaped}")) {{')
            lines.append(f'        Info("⊘ Measure already exists: {name_escaped}");')
            lines.append(f'    }} else {{')
            lines.append(f'        var m = tbl.AddMeasure("{name_escaped}", @"{dax_verbatim}");')
            if m.description:
                lines.append(f'        m.Description = @"{desc_verbatim}";')
            lines.append(f'        created++;')
            lines.append(f'        Info("✓ Measure: {name_escaped}");')
            lines.append(f'    }}')
            lines.append("} catch (Exception ex) {")
            lines.append(f'    errors.Add("Measure {name_escaped}: " + ex.Message);')
            lines.append("}")
            lines.append("")

    # ── Summary ──
    lines.append("// ── Summary ──")
    lines.append('if (errors.Count == 0) {')
    lines.append('    Info("\\n══ All done! " + created + " objects created. Press Ctrl+S to save. ══");')
    lines.append('} else {')
    lines.append('    Info("\\n══ Completed with " + errors.Count + " error(s):");')
    lines.append('    foreach (var e in errors) Warning("  • " + e);')
    lines.append('    Info("Successfully created " + created + " objects. Fix errors above, then Ctrl+S to save. ══");')
    lines.append("}")

    return "\n".join(lines)


@app.post("/generate-te-script")
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


def _build_m_expression(table: Table, filename: str, is_csv: bool,
                        valid_sheets: List[str] = None,
                        valid_columns: Optional[List[str]] = None) -> str:
    """Build a Power Query M expression for a table that reads from the source file.

    If the table's source sheet doesn't exist or none of its columns exist in the
    real data, generates an empty typed table instead of a broken query.
    """
    sheet = table.source_sheet or "Sheet1"
    sheet_exists = (not valid_sheets) or (sheet in valid_sheets)

    # Check how many columns actually exist in the source
    if valid_columns:
        real_cols = [c for c in table.columns if c in valid_columns]
    else:
        real_cols = table.columns

    # If the sheet doesn't exist OR none of the columns exist in real data,
    # create an empty placeholder table so PBI Desktop doesn't error
    if valid_sheets and (not sheet_exists and not real_cols):
        col_defs = ", ".join([f'{{"{c}", type text}}' for c in table.columns])
        return f'#table(type table [{", ".join([f"{c} = text" for c in table.columns])}], {{}})'

    # If sheet exists but some columns are invented, only select real ones
    # and fall back to the correct sheet
    if valid_sheets and not sheet_exists:
        sheet = valid_sheets[0]
    if valid_columns and real_cols:
        use_cols = real_cols
    else:
        use_cols = table.columns

    cols_list = ", ".join([f'"{c}"' for c in use_cols])

    if is_csv:
        return (
            "let\n"
            f'    Source = Csv.Document(File.Contents(ExcelFilePath), [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.None]),\n'
            f'    Headers = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),\n'
            f'    Selected = Table.SelectColumns(Headers, {{{cols_list}}})\n'
            "in\n"
            "    Selected"
        )
    else:
        return (
            "let\n"
            f'    Source = Excel.Workbook(File.Contents(ExcelFilePath), null, true),\n'
            f'    SheetData = Source{{[Item="{sheet}",Kind="Sheet"]}}[Data],\n'
            f'    Headers = Table.PromoteHeaders(SheetData, [PromoteAllScalars=true]),\n'
            f'    Selected = Table.SelectColumns(Headers, {{{cols_list}}})\n'
            "in\n"
            "    Selected"
        )


def _build_pbip_zip(data: ReportRequest, session_id: Optional[str] = None, file_path: Optional[str] = None) -> str:
    """Generate a PBIP project folder as a ZIP file. Returns the ZIP file path."""
    project_name = data.filename.replace(".xlsx", "").replace(".xls", "").replace(".csv", "").replace(" ", "_")
    is_csv = data.filename.lower().endswith(".csv")
    # Use provided absolute path or fall back to filename
    abs_file_path = file_path or data.filename

    # Get valid sheet names from session data
    valid_sheets = []
    if session_id and session_id in _session_data:
        valid_sheets = list(_session_data[session_id].keys())

    # ── model.bim ──
    tables_bim = []
    for t in data.tables:
        col_dtypes = _resolve_column_dtypes(t, session_id)
        columns = []
        for col_name in t.columns:
            pandas_dt = col_dtypes.get(col_name, "object")
            if pandas_dt != "object":
                bim_dt = PANDAS_TO_BIM_DTYPE.get(pandas_dt, "string")
            else:
                bim_dt = _guess_bim_dtype_from_name(col_name)
            col_obj = {
                "name": col_name,
                "dataType": bim_dt,
                "sourceColumn": col_name,
                "lineageTag": _make_lineage_tag(),
                "summarizeBy": "none" if col_name == t.primary_key else (
                    "sum" if bim_dt in ("int64", "double", "decimal") else "none"
                ),
            }
            if col_name == t.primary_key:
                col_obj["isKey"] = True
            columns.append(col_obj)

        # Get valid column names from actual source sheet
        source_cols = None
        if session_id and session_id in _session_data:
            frames = _session_data[session_id]
            # Try exact sheet, then fallback to first sheet
            src_sheet = t.source_sheet
            if src_sheet and src_sheet in frames:
                source_cols = list(frames[src_sheet].columns)
            elif valid_sheets:
                source_cols = list(frames[valid_sheets[0]].columns)

        m_expr = _build_m_expression(t, data.filename, is_csv, valid_sheets, source_cols)

        table_obj = {
            "name": t.name,
            "description": t.description,
            "lineageTag": _make_lineage_tag(),
            "columns": columns,
            "partitions": [{
                "name": t.name,
                "mode": "import",
                "source": {"type": "m", "expression": m_expr},
            }],
        }
        tables_bim.append(table_obj)

    # Attach measures to first Fact table or first table
    measure_target = None
    for i, t in enumerate(data.tables):
        if t.type == "Fact":
            measure_target = i
            break
    if measure_target is None and tables_bim:
        measure_target = 0

    if measure_target is not None and data.measures_suggested:
        tables_bim[measure_target]["measures"] = [{
            "name": m.name,
            "expression": m.dax,
            "lineageTag": _make_lineage_tag(),
            **({"description": m.description} if m.description else {}),
        } for m in data.measures_suggested]

    # ExcelFilePath parameter table
    param_table = {
        "name": "ExcelFilePath",
        "lineageTag": _make_lineage_tag(),
        "columns": [{
            "name": "ExcelFilePath",
            "dataType": "string",
            "lineageTag": _make_lineage_tag(),
            "sourceColumn": "ExcelFilePath",
            "summarizeBy": "none",
            "isHidden": True,
        }],
        "partitions": [{
            "name": "ExcelFilePath",
            "mode": "import",
            "source": {
                "type": "m",
                "expression": f'"{abs_file_path}" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]',
            },
        }],
        "isHidden": True,
    }
    tables_bim.append(param_table)

    # Relationships
    relationships_bim = []
    for r in data.relationships:
        from_card, to_card = CARDINALITY_MAP.get(r.cardinality, ("many", "one"))
        cf = CROSS_FILTER_MAP.get(r.cross_filter, "oneDirection")
        relationships_bim.append({
            "name": f"{r.from_table}_{r.from_column}_{r.to_table}_{r.to_column}",
            "fromTable": r.from_table,
            "fromColumn": r.from_column,
            "toTable": r.to_table,
            "toColumn": r.to_column,
            "fromCardinality": from_card,
            "toCardinality": to_card,
            "crossFilteringBehavior": cf,
            "isActive": True,
        })

    model_bim = {
        "name": project_name,
        "compatibilityLevel": 1550,
        "model": {
            "culture": "en-US",
            "defaultPowerBIDataSourceVersion": "powerBI_V3",
            "defaultMode": "import",
            "tables": tables_bim,
            "relationships": relationships_bim,
            "annotations": [
                {"name": "PBI_GeneratedBy", "value": "PowerBI-Model-Advisor"},
            ],
        },
    }

    # ── Build ZIP ──
    prefix = f"{project_name}"
    sm_dir = f"{prefix}.SemanticModel"
    rpt_dir = f"{prefix}.Report"

    # ── .pbip entry point ──
    pbip_json = json.dumps({
        "version": "1.0",
        "artifacts": [{"report": {"path": rpt_dir}}],
        "settings": {"enableAutoRecovery": True},
    }, indent=2)

    # ── Semantic Model: definition.pbism ──
    # version "1.0" = TMSL format (model.bim), NOT "4.0" which is TMDL
    pbism_json = json.dumps({
        "version": "1.0",
        "settings": {},
    }, indent=2)

    # ── Report: definition.pbir ──
    # version "4.0" = PBIR enhanced format (definition/ folder)
    pbir_json = json.dumps({
        "version": "4.0",
        "datasetReference": {"byPath": {"path": f"../{sm_dir}"}},
    }, indent=2)

    bim_json = json.dumps(model_bim, indent=2, ensure_ascii=False)

    tmp_dir = tempfile.gettempdir()
    zip_path = os.path.join(tmp_dir, f"{project_name}_{uuid.uuid4().hex[:8]}.zip")

    # ── PBIR Report files ──
    page_name = "ReportSection"

    # version.json — correct schema URL is "versionMetadata", version is semver
    version_json = json.dumps({
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/versionMetadata/1.0.0/schema.json",
        "version": "1.0.0",
    }, indent=2)

    # report.json — theme + required layoutOptimization
    report_def_json = json.dumps({
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/report/1.0.0/schema.json",
        "layoutOptimization": "None",
        "themeCollection": {
            "baseTheme": {
                "name": "CY24SU06",
                "reportVersionAtImport": "5.55",
                "type": "SharedResources",
            },
        },
        "resourcePackages": [
            {
                "name": "SharedResources",
                "type": "SharedResources",
                "items": [
                    {
                        "name": "CY24SU06",
                        "path": "BaseThemes/CY24SU06.json",
                        "type": "BaseTheme",
                    },
                ],
            },
        ],
        "settings": {
            "useDefaultAggregateDisplayName": True,
            "defaultDrillFilterOtherVisuals": True,
        },
    }, indent=2)

    # pages.json — page ordering
    pages_meta_json = json.dumps({
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/pagesMetadata/1.0.0/schema.json",
        "pageOrder": [page_name],
        "activePageName": page_name,
    }, indent=2)

    # page.json — single blank page
    page_json = json.dumps({
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/1.0.0/schema.json",
        "name": page_name,
        "displayName": "Page 1",
        "displayOption": "FitToPage",
        "height": 720,
        "width": 1280,
    }, indent=2)

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"{prefix}/{prefix}.pbip", pbip_json)
        zf.writestr(f"{prefix}/{sm_dir}/definition.pbism", pbism_json)
        zf.writestr(f"{prefix}/{sm_dir}/model.bim", bim_json)
        zf.writestr(f"{prefix}/{rpt_dir}/definition.pbir", pbir_json)
        zf.writestr(f"{prefix}/{rpt_dir}/definition/version.json", version_json)
        zf.writestr(f"{prefix}/{rpt_dir}/definition/report.json", report_def_json)
        zf.writestr(f"{prefix}/{rpt_dir}/definition/pages/pages.json", pages_meta_json)
        zf.writestr(f"{prefix}/{rpt_dir}/definition/pages/{page_name}/page.json", page_json)
        zf.writestr(f"{prefix}/.gitignore", "**/.pbi/localSettings.json\n**/.pbi/cache.abf\n")
        # Include a README for the user
        zf.writestr(f"{prefix}/README.txt",
            "Power BI Model Advisor — Auto-Generated Project\n"
            "================================================\n\n"
            "HOW TO OPEN:\n"
            "1. Extract this ZIP folder\n"
            f"2. Make sure your data file is at: {abs_file_path}\n"
            f"3. Double-click '{prefix}.pbip' to open in Power BI Desktop\n"
            "4. If prompted, enable PBIP preview: File > Options > Preview features > Power BI Project (.pbip)\n"
            "5. Click Refresh to load data — your model is ready!\n\n"
            f"DATA FILE PATH: {abs_file_path}\n"
            "If you moved the file, update the ExcelFilePath parameter:\n"
            "  Home > Transform Data > ExcelFilePath > set new path > Close & Apply\n\n"
            "WHAT'S INCLUDED:\n"
            f"- {len(data.tables)} tables with column definitions\n"
            f"- {len(data.relationships)} relationships\n"
            f"- {len(data.measures_suggested)} DAX measures\n"
            "- Power Query M expressions to load data from your file\n"
        )

    return zip_path


@app.post("/generate-pbip")
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


# ── Power BI Desktop MCP Integration ─────────────────────────────────────────

class DeployRequest(BaseModel):
    model: ReportRequest
    mcp_exe_path: str  # e.g. "C:\\MCPServers\\PowerBIModelingMCP\\...\\powerbi-modeling-mcp.exe"
    pbi_port: Optional[int] = None  # local AS port if known


class MCPClient:
    """Communicate with the Power BI Modeling MCP server via stdio JSON-RPC.

    Protocol: JSON-RPC 2.0 over stdio, newline-delimited JSON (ndjson).
    The .NET ModelContextProtocol StdioServerTransport reads one JSON object per line.
    Uses subprocess.Popen + threads for Windows compatibility (asyncio subprocess
    pipes don't work reliably under uvicorn's event loop on Windows).
    """

    def __init__(self, exe_path: str):
        self.exe_path = exe_path
        self.process: Optional[subprocess.Popen] = None
        self._id = 0
        self._timeout = 30
        self._stdout_lines: list = []
        self._stdout_event = threading.Event()
        self._reader_thread: Optional[threading.Thread] = None

    async def start(self):
        """Launch the MCP server subprocess and complete the MCP handshake."""
        self.process = subprocess.Popen(
            [self.exe_path, "--start", "--skipconfirmation"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
        )
        # Start background thread to read stdout lines
        self._reader_thread = threading.Thread(target=self._stdout_reader, daemon=True)
        self._reader_thread.start()

        # Wait for the server to finish initializing (reads stderr)
        await self._wait_for_startup()

        # MCP handshake
        init_resp = await self._send("initialize", {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "powerbi-advisor", "version": "1.0"},
        })
        logger.info(f"MCP initialized: {json.dumps(init_resp, default=str)[:300]}")
        await self._send_notification("notifications/initialized", {})

    def _stdout_reader(self):
        """Background thread: read lines from stdout and queue them."""
        try:
            buf = b""
            while self.process and self.process.stdout:
                chunk = self.process.stdout.read(1)
                if not chunk:
                    break
                buf += chunk
                if chunk == b"\n":
                    self._stdout_lines.append(buf.decode("utf-8", errors="replace"))
                    buf = b""
                    self._stdout_event.set()
        except Exception:
            pass

    async def _wait_for_startup(self):
        """Read stderr until the server signals it's ready."""
        loop = asyncio.get_event_loop()
        def _read_stderr():
            while True:
                line = self.process.stderr.readline()
                if not line:
                    break
                text = line.decode("utf-8", errors="replace").strip()
                logger.debug(f"MCP startup: {text}")
                if "Application started" in text or "transport reading messages" in text:
                    return
        try:
            await asyncio.wait_for(loop.run_in_executor(None, _read_stderr), timeout=20)
        except asyncio.TimeoutError:
            logger.warning("MCP startup: timed out, proceeding anyway")

    def _next_id(self) -> int:
        self._id += 1
        return self._id

    def _write(self, data: str):
        """Write a line to the MCP server stdin."""
        self.process.stdin.write((data + "\n").encode("utf-8"))
        self.process.stdin.flush()

    async def _send_notification(self, method: str, params: dict):
        msg = json.dumps({"jsonrpc": "2.0", "method": method, "params": params})
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._write, msg)

    async def _send(self, method: str, params: dict) -> dict:
        """Send a JSON-RPC request and wait for the matching response."""
        req_id = self._next_id()
        msg = json.dumps({
            "jsonrpc": "2.0",
            "id": req_id,
            "method": method,
            "params": params,
        })
        if not self.process or not self.process.stdin:
            raise RuntimeError("MCP server not started")

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._write, msg)

        # Read response(s) — skip notifications until we get our matching response
        while True:
            resp = await asyncio.wait_for(self._read_message(), timeout=self._timeout)
            if not resp:
                continue
            if "id" in resp:
                if resp.get("error"):
                    err = resp["error"]
                    raise RuntimeError(f"MCP error {err.get('code')}: {err.get('message')}")
                return resp.get("result", {})

    async def _read_message(self) -> dict:
        """Read one JSON-RPC message from the stdout queue."""
        loop = asyncio.get_event_loop()
        def _wait_for_line():
            while not self._stdout_lines:
                if not self._stdout_event.wait(timeout=1):
                    if self.process and self.process.poll() is not None:
                        raise RuntimeError("MCP server process exited")
                    continue
                self._stdout_event.clear()
            return self._stdout_lines.pop(0)

        line = await asyncio.wait_for(loop.run_in_executor(None, _wait_for_line), timeout=self._timeout)
        line = line.strip()
        if not line:
            return {}
        return json.loads(line)

    async def call_tool(self, tool_name: str, request: dict) -> dict:
        """Call an MCP tool. All tools take a single 'request' parameter."""
        logger.info(f"MCP call: {tool_name}({json.dumps(request, default=str)[:300]})")
        resp = await self._send("tools/call", {
            "name": tool_name,
            "arguments": {"request": request},
        })
        logger.info(f"MCP response: {json.dumps(resp, default=str)[:300]}")
        # Check for tool-level errors in content
        if resp.get("isError"):
            text = self._extract_text(resp)
            raise RuntimeError(f"MCP tool error: {text}")
        return resp

    def _extract_text(self, resp: dict) -> str:
        """Extract text content from an MCP tool response."""
        content = resp.get("content", [])
        if isinstance(content, list):
            parts = [item.get("text", "") for item in content if isinstance(item, dict)]
            return "\n".join(parts)
        return str(content)

    # ── High-level operations matching microsoft/powerbi-modeling-mcp v0.1.9 ──
    # All tools take {request: {operation, ...}} — verified against actual tool schemas

    async def list_local_instances(self) -> dict:
        """Find running Power BI Desktop instances."""
        return await self.call_tool("connection_operations", {
            "operation": "ListLocalInstances",
        })

    async def connect(self, data_source: str, initial_catalog: str = None) -> dict:
        """Connect to a PBI Desktop instance via localhost:<port>."""
        req = {"operation": "Connect", "dataSource": data_source}
        if initial_catalog:
            req["initialCatalog"] = initial_catalog
        return await self.call_tool("connection_operations", req)

    async def list_connections(self) -> dict:
        return await self.call_tool("connection_operations", {"operation": "ListConnections"})

    async def list_tables(self) -> dict:
        return await self.call_tool("table_operations", {"operation": "List"})

    async def list_relationships(self) -> dict:
        return await self.call_tool("relationship_operations", {"operation": "List"})

    async def list_measures(self) -> dict:
        return await self.call_tool("measure_operations", {"operation": "List"})

    async def create_relationship(self, from_table: str, from_col: str,
                                   to_table: str, to_col: str,
                                   cross_filter: str = "OneDirection") -> dict:
        """Create a single relationship using relationship_operations Create."""
        return await self.call_tool("relationship_operations", {
            "operation": "Create",
            "relationshipDefinition": {
                "fromTable": from_table,
                "fromColumn": from_col,
                "toTable": to_table,
                "toColumn": to_col,
                "crossFilteringBehavior": cross_filter,
                "isActive": True,
            },
        })

    async def create_measure(self, table_name: str, name: str, expression: str,
                              description: str = "") -> dict:
        """Create a single DAX measure using measure_operations Create."""
        req = {
            "operation": "Create",
            "createDefinition": {
                "tableName": table_name,
                "name": name,
                "expression": expression,
            },
        }
        if description:
            req["createDefinition"]["description"] = description
        return await self.call_tool("measure_operations", req)

    async def create_calculated_table(self, table_name: str, dax_expression: str) -> dict:
        """Create a calculated table using a DAX expression."""
        return await self.call_tool("table_operations", {
            "operation": "Create",
            "tableName": table_name,
            "createDefinition": {
                "daxExpression": dax_expression,
            },
        })

    async def execute_dax(self, query: str) -> dict:
        return await self.call_tool("dax_query_operations", {
            "operation": "Execute",
            "query": query,
        })

    async def get_table_columns(self, table_name: str) -> dict:
        """Get full column details for a table."""
        return await self.call_tool("table_operations", {
            "operation": "GetSchema",
            "tableName": table_name,
        })

    async def get_model(self) -> dict:
        return await self.call_tool("model_operations", {"operation": "Get"})

    async def stop(self):
        if self.process:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
            self.process = None


@app.post("/deploy-to-desktop")
async def deploy_to_desktop(req: DeployRequest):
    """Deploy relationships and DAX measures to a running Power BI Desktop instance via MCP.

    Flow: Start MCP → find PBI Desktop → connect → create relationships → create measures.
    Requires: Power BI Desktop running with the data file loaded (Get Data → Excel).
    """
    if not os.path.isfile(req.mcp_exe_path):
        raise HTTPException(400, f"MCP server not found at: {req.mcp_exe_path}")

    mcp = MCPClient(req.mcp_exe_path)
    results = {"connected": False, "relationships": [], "measures": [], "errors": []}

    try:
        # 1. Start MCP server and complete protocol handshake
        await mcp.start()
        logger.info("MCP server started and initialized")

        # 2. Find running PBI Desktop instances
        instances_resp = await mcp.list_local_instances()
        instances_text = mcp._extract_text(instances_resp)
        logger.info(f"Local PBI instances: {instances_text[:500]}")

        # Parse the port from ListLocalInstances response (format: "localhost:PORT")
        port_match = re.search(r"localhost:(\d+)", instances_text)
        if not port_match:
            raise RuntimeError(
                f"No running Power BI Desktop instance found. "
                f"Please open Power BI Desktop and load your data file first. "
                f"Server response: {instances_text[:200]}"
            )

        # 3. Connect to the PBI Desktop instance
        pbi_port = port_match.group(0)  # "localhost:12345"
        logger.info(f"Connecting to PBI Desktop at {pbi_port}")
        conn_resp = await mcp.connect(pbi_port)
        conn_text = mcp._extract_text(conn_resp)
        logger.info(f"Connected: {conn_text[:300]}")
        results["connected"] = True
        results["instance"] = pbi_port

        # 4. List existing tables and extract names for validation
        tables_resp = await mcp.list_tables()
        tables_text = mcp._extract_text(tables_resp)
        logger.info(f"Tables in model: {tables_text[:500]}")

        # Parse table names from MCP response
        existing_tables = set()
        try:
            tables_data = json.loads(tables_text)
            for t in tables_data.get("data", []):
                existing_tables.add(t.get("name", ""))
        except (json.JSONDecodeError, TypeError):
            # Try to extract table names from text
            for match in re.findall(r'"name"\s*:\s*"([^"]+)"', tables_text):
                existing_tables.add(match)

        results["pbi_tables"] = sorted(existing_tables)
        logger.info(f"Tables found in PBI Desktop: {existing_tables}")

        # 5. Get source table column schema (needed for validation)
        source_columns_cache: Dict[str, set] = {}
        for tbl_name in list(existing_tables):
            try:
                schema_resp = await mcp.get_table_columns(tbl_name)
                schema_text = mcp._extract_text(schema_resp)
                schema_data = json.loads(schema_text)
                source_columns_cache[tbl_name] = {
                    c["name"] for c in schema_data.get("data", {}).get("Columns", [])
                    if not c.get("isHidden", False)
                }
            except Exception:
                source_columns_cache[tbl_name] = set()

        # Collect ALL columns referenced in relationships — dimensions MUST include these
        rel_columns_needed: Dict[str, set] = {}  # table_name -> {columns needed for relationships}
        for r in req.model.relationships:
            rel_columns_needed.setdefault(r.from_table, set()).add(r.from_column)
            rel_columns_needed.setdefault(r.to_table, set()).add(r.to_column)

        # 6. Create missing tables as DAX calculated tables
        results["tables_created"] = []
        for t in req.model.tables:
            if t.name in existing_tables:
                continue  # Table already exists

            # Find the source table in PBI Desktop
            source = t.source_sheet
            if source not in existing_tables:
                source = next(iter(existing_tables), None)
            if not source:
                results["errors"].append(f"Cannot create table '{t.name}': no source table in PBI Desktop.")
                continue

            src_cols = source_columns_cache.get(source, set())

            # Start with the AI-recommended columns, filtered to real ones
            valid_cols = [c for c in t.columns if c in src_cols]

            # AUTO-ADD: columns needed for relationships that exist in source
            extra_rel_cols = rel_columns_needed.get(t.name, set())
            for rc in extra_rel_cols:
                if rc in src_cols and rc not in valid_cols:
                    valid_cols.append(rc)

            if not valid_cols:
                results["errors"].append(
                    f"Cannot create table '{t.name}': none of its columns exist in '{source}'."
                )
                continue

            # Build DAX expression
            col_refs = ", ".join([f'"{c}", \'{source}\'[{c}]' for c in valid_cols])
            if t.type == "Dimension":
                dax = f"DISTINCT(SELECTCOLUMNS('{source}', {col_refs}))"
            else:
                dax = f"SELECTCOLUMNS('{source}', {col_refs})"

            try:
                resp = await mcp.create_calculated_table(t.name, dax)
                resp_text = mcp._extract_text(resp)
                try:
                    resp_data = json.loads(resp_text)
                    if resp_data.get("success") is False:
                        raise RuntimeError(resp_data.get("message", "Failed"))
                except json.JSONDecodeError:
                    pass
                results["tables_created"].append({
                    "name": t.name, "type": t.type,
                    "columns": valid_cols, "source": source, "status": "created",
                })
                existing_tables.add(t.name)
                source_columns_cache[t.name] = set(valid_cols)
                logger.info(f"Created calculated table: {t.name} ({t.type}) with {len(valid_cols)} cols from {source}")
            except Exception as e:
                results["errors"].append(f"Table {t.name}: {e}")
                logger.error(f"Failed to create table {t.name}: {e}")

        results["pbi_tables"] = sorted(existing_tables)

        # 7. Create relationships — skip if tables/columns don't exist
        for r in req.model.relationships:
            label = f"{r.from_table}[{r.from_column}] -> {r.to_table}[{r.to_column}]"

            # Pre-validate: both tables and columns must exist
            from_cols = source_columns_cache.get(r.from_table, set())
            to_cols = source_columns_cache.get(r.to_table, set())
            if r.from_table not in existing_tables:
                results["errors"].append(f"Relationship {label}: table '{r.from_table}' does not exist, skipped.")
                continue
            if r.to_table not in existing_tables:
                results["errors"].append(f"Relationship {label}: table '{r.to_table}' does not exist, skipped.")
                continue
            if from_cols and r.from_column not in from_cols:
                results["errors"].append(f"Relationship {label}: column '{r.from_column}' not in '{r.from_table}', skipped.")
                continue
            if to_cols and r.to_column not in to_cols:
                results["errors"].append(f"Relationship {label}: column '{r.to_column}' not in '{r.to_table}', skipped.")
                continue

            try:
                cf = "OneDirection" if r.cross_filter == "Single" else "BothDirections"
                resp = await mcp.create_relationship(
                    r.from_table, r.from_column,
                    r.to_table, r.to_column, cf
                )
                resp_text = mcp._extract_text(resp)
                try:
                    resp_data = json.loads(resp_text)
                    if resp_data.get("success") is False:
                        msg = resp_data.get("message", "Unknown error")
                        if "already exists" in msg.lower():
                            results["relationships"].append({"rel": label, "status": "exists"})
                            continue
                        raise RuntimeError(msg)
                except json.JSONDecodeError:
                    pass
                results["relationships"].append({"rel": label, "status": "created"})
                logger.info(f"Created relationship: {label}")
            except Exception as e:
                err_str = str(e)
                if "already exists" in err_str.lower():
                    results["relationships"].append({"rel": label, "status": "exists"})
                else:
                    results["errors"].append(f"Relationship {label}: {e}")
                    logger.error(f"Failed relationship {label}: {e}")

        # 8. Create DAX measures — validate references first
        measure_table = None
        for t in req.model.tables:
            if t.type == "Fact" and t.name in existing_tables:
                measure_table = t.name
                break
        if not measure_table:
            for t in req.model.tables:
                if t.name in existing_tables:
                    measure_table = t.name
                    break
        if not measure_table and existing_tables:
            measure_table = sorted(existing_tables)[0]

        if measure_table:
            # Pre-validate DAX: check Table[Column] references exist
            dax_ref_pattern = re.compile(r"(\w+)\[(\w+)\]")
            for m in req.model.measures_suggested:
                dax = m.dax
                # Check for references to non-existent tables/columns
                refs = dax_ref_pattern.findall(dax)
                bad_refs = []
                for tbl, col in refs:
                    if tbl in source_columns_cache:
                        if col not in source_columns_cache[tbl]:
                            bad_refs.append(f"{tbl}[{col}]")
                    elif tbl not in existing_tables and tbl not in ("SELECTEDVALUE", "SWITCH", "IF"):
                        bad_refs.append(f"{tbl}[{col}] (table not found)")
                if bad_refs:
                    results["errors"].append(
                        f"Measure '{m.name}' skipped — DAX references non-existent columns: {', '.join(bad_refs)}"
                    )
                    continue

                try:
                    resp = await mcp.create_measure(measure_table, m.name, dax, m.description)
                    resp_text = mcp._extract_text(resp)
                    try:
                        resp_data = json.loads(resp_text)
                        if resp_data.get("success") is False:
                            msg = resp_data.get("message", "Unknown error")
                            if "already exists" in msg.lower():
                                results["measures"].append({"name": m.name, "table": measure_table, "status": "exists"})
                                continue
                            raise RuntimeError(msg)
                    except json.JSONDecodeError:
                        pass
                    results["measures"].append({
                        "name": m.name, "table": measure_table, "status": "created",
                    })
                    logger.info(f"Created measure: {m.name}")
                except Exception as e:
                    err_str = str(e)
                    if "already exists" in err_str.lower():
                        results["measures"].append({"name": m.name, "table": measure_table, "status": "exists"})
                    else:
                        results["errors"].append(f"Measure '{m.name}': {e}")
                        logger.error(f"Failed measure {m.name}: {e}")

        # 7. Verify: re-read the model to confirm changes
        verify_measures = await mcp.list_measures()
        verify_text = mcp._extract_text(verify_measures)
        logger.info(f"Post-deploy measures: {verify_text[:500]}")
        try:
            verify_data = json.loads(verify_text)
            results["verified_measures"] = verify_data.get("data", [])
        except (json.JSONDecodeError, TypeError):
            pass

        verify_rels = await mcp.list_relationships()
        verify_rels_text = mcp._extract_text(verify_rels)
        logger.info(f"Post-deploy relationships: {verify_rels_text[:500]}")
        try:
            verify_rels_data = json.loads(verify_rels_text)
            results["verified_relationships"] = verify_rels_data.get("data", [])
        except (json.JSONDecodeError, TypeError):
            pass

        results["success"] = len(results["errors"]) == 0

    except Exception as e:
        err_msg = str(e) or repr(e)
        # Capture stderr from MCP process for diagnostics
        if mcp.process and mcp.process.stderr:
            try:
                stderr_bytes = await asyncio.wait_for(mcp.process.stderr.read(4096), timeout=2)
                stderr_text = stderr_bytes.decode("utf-8", errors="replace").strip()
                if stderr_text:
                    err_msg += f" | MCP stderr: {stderr_text[:500]}"
            except Exception:
                pass
        logger.error(f"MCP deploy failed: {err_msg}", exc_info=True)
        results["success"] = False
        results["errors"].append(err_msg)
    finally:
        await mcp.stop()

    return results


class PromptRequest(BaseModel):
    prompt: str
    mcp_exe_path: str
    conversation: List[Dict[str, str]] = []  # prior messages for multi-turn


@app.post("/prompt-model")
async def prompt_model(req: PromptRequest):
    """Natural language interface to modify Power BI Desktop model via MCP.

    Flow: connect to PBI Desktop → read current model state → send to Claude
    with user prompt → Claude returns structured actions → execute via MCP.
    """
    if not os.path.isfile(req.mcp_exe_path):
        raise HTTPException(400, f"MCP server not found at: {req.mcp_exe_path}")

    mcp = MCPClient(req.mcp_exe_path)
    results = {"actions": [], "errors": [], "reply": "", "model_state": None}

    try:
        # 1. Start MCP and connect to PBI Desktop
        await mcp.start()

        instances_resp = await mcp.list_local_instances()
        instances_text = mcp._extract_text(instances_resp)
        port_match = re.search(r"localhost:(\d+)", instances_text)
        if not port_match:
            raise RuntimeError("No running Power BI Desktop instance found. Open PBI Desktop and load your data first.")

        await mcp.connect(port_match.group(0))

        # 2. Read current model state (tables + columns for each table)
        tables_resp = await mcp.list_tables()
        tables_text = mcp._extract_text(tables_resp)

        # Get column details for each table
        table_details = []
        try:
            tables_data = json.loads(tables_text)
            for t in tables_data.get("data", []):
                tname = t.get("name", "")
                if tname:
                    try:
                        schema_resp = await mcp.get_table_columns(tname)
                        schema_text = mcp._extract_text(schema_resp)
                        table_details.append(f"Table '{tname}': {schema_text}")
                    except Exception:
                        table_details.append(f"Table '{tname}': columns unknown")
        except (json.JSONDecodeError, TypeError):
            table_details.append(f"Tables raw: {tables_text}")

        rels_resp = await mcp.list_relationships()
        rels_text = mcp._extract_text(rels_resp)

        measures_resp = await mcp.list_measures()
        measures_text = mcp._extract_text(measures_resp)

        model_state = (
            f"TABLES AND COLUMNS:\n" + "\n".join(table_details) +
            f"\n\nRELATIONSHIPS:\n{rels_text}\n\nMEASURES:\n{measures_text}"
        )
        results["model_state"] = model_state[:3000]

        # 3. Ask Claude to plan operations
        system_prompt = f"""Tu es un expert Power BI connecté à un modèle sémantique Power BI Desktop via MCP.
Voici l'état actuel du modèle :

{model_state}

L'utilisateur veut modifier ce modèle. Analyse sa demande et réponds en JSON valide (sans markdown) :

{{
  "reply": "Explication courte de ce que tu vas faire (en français)",
  "actions": [
    {{
      "type": "create_measure",
      "table": "NomTable",
      "name": "NomMesure",
      "expression": "FORMULE DAX",
      "description": "description"
    }},
    {{
      "type": "create_relationship",
      "from_table": "T1",
      "from_column": "col1",
      "to_table": "T2",
      "to_column": "col2",
      "cross_filter": "OneDirection"
    }},
    {{
      "type": "execute_dax",
      "query": "EVALUATE ..."
    }},
    {{
      "type": "info",
      "message": "Information ou conseil sans action"
    }}
  ]
}}

Règles :
- Utilise UNIQUEMENT les tables et colonnes qui existent dans le modèle ci-dessus
- Pour les mesures DAX, utilise la syntaxe correcte
- Si la demande n'est pas claire, retourne un "info" avec une question de clarification
- Si aucune action n'est nécessaire, retourne actions vide et reply avec l'explication
- Réponds UNIQUEMENT en JSON valide"""

        messages = []
        for msg in req.conversation:
            messages.append({"role": msg.get("role", "user"), "content": msg.get("content", "")})
        messages.append({"role": "user", "content": req.prompt})

        client = get_client()
        claude_resp = client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=4096,
            system=system_prompt,
            messages=messages,
        )

        try:
            plan = parse_json_response(claude_resp.content[0].text)
        except Exception:
            results["reply"] = claude_resp.content[0].text
            results["success"] = True
            return results

        results["reply"] = plan.get("reply", "")
        actions = plan.get("actions", [])

        # 4. Execute planned actions
        for action in actions:
            action_type = action.get("type")
            try:
                if action_type == "create_measure":
                    await mcp.create_measure(
                        action["table"], action["name"],
                        action["expression"], action.get("description", "")
                    )
                    results["actions"].append({
                        "type": "create_measure",
                        "name": action["name"],
                        "table": action["table"],
                        "status": "done",
                    })

                elif action_type == "create_relationship":
                    await mcp.create_relationship(
                        action["from_table"], action["from_column"],
                        action["to_table"], action["to_column"],
                        action.get("cross_filter", "OneDirection"),
                    )
                    results["actions"].append({
                        "type": "create_relationship",
                        "rel": f"{action['from_table']}[{action['from_column']}] -> {action['to_table']}[{action['to_column']}]",
                        "status": "done",
                    })

                elif action_type == "execute_dax":
                    dax_resp = await mcp.execute_dax(action["query"])
                    dax_text = mcp._extract_text(dax_resp)
                    results["actions"].append({
                        "type": "execute_dax",
                        "query": action["query"],
                        "result": dax_text[:1000],
                        "status": "done",
                    })

                elif action_type == "info":
                    results["actions"].append({
                        "type": "info",
                        "message": action.get("message", ""),
                        "status": "done",
                    })

            except Exception as e:
                results["actions"].append({
                    "type": action_type,
                    "status": "error",
                    "error": str(e),
                })
                results["errors"].append(f"{action_type}: {e}")

        results["success"] = len(results["errors"]) == 0

    except Exception as e:
        err_msg = str(e) or repr(e)
        logger.error(f"Prompt-model failed: {err_msg}", exc_info=True)
        results["success"] = False
        results["errors"].append(err_msg)
    finally:
        await mcp.stop()

    return results


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


@app.post("/push-to-powerbi")
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


frontend_path = os.path.join(os.path.dirname(__file__), "..", "frontend")
if os.path.isdir(frontend_path):
    app.mount("/", StaticFiles(directory=frontend_path, html=True), name="frontend")
