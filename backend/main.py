from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import pandas as pd
import anthropic
import httpx
import json, io, os, re, tempfile, uuid, zipfile
from datetime import datetime

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


def _build_m_expression(table: Table, filename: str, is_csv: bool) -> str:
    """Build a Power Query M expression for a table that reads from the source file."""
    cols_list = ", ".join([f'"{c}"' for c in table.columns])

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
        sheet = table.source_sheet or "Sheet1"
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

        m_expr = _build_m_expression(t, data.filename, is_csv)

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

    pbip_json = json.dumps({
        "version": "1.0",
        "artifacts": [{"report": {"path": rpt_dir}}],
        "settings": {"enableAutoRecovery": True},
    }, indent=2)

    pbism_json = json.dumps({
        "version": "1.0",
    }, indent=2)

    pbir_json = json.dumps({
        "version": "4.0",
        "datasetReference": {"byPath": {"path": f"../{sm_dir}"}},
    }, indent=2)

    # Use PBIR enhanced format (definition/ folder) instead of legacy report.json
    # This avoids theme rendering crashes in Power BI Desktop
    use_pbir = True

    bim_json = json.dumps(model_bim, indent=2, ensure_ascii=False)

    tmp_dir = tempfile.gettempdir()
    zip_path = os.path.join(tmp_dir, f"{project_name}_{uuid.uuid4().hex[:8]}.zip")

    # Report definition files — PBIR enhanced format
    page_name = "ReportSection"

    # version.json — REQUIRED by PBI Desktop
    version_json = json.dumps({"version": "4.0"}, indent=2)

    # report.json — minimal valid structure
    report_config = json.dumps({
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/report/1.0.0/schema.json",
        "name": project_name,
    }, indent=2)

    # page.json
    page_json = json.dumps({
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/1.0.0/schema.json",
        "name": page_name,
        "displayName": "Page 1",
        "ordinal": 0,
        "displayOption": 1,
        "width": 1280,
        "height": 720,
        "visualContainers": [],
    }, indent=2)

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"{prefix}/{prefix}.pbip", pbip_json)
        zf.writestr(f"{prefix}/{sm_dir}/definition.pbism", pbism_json)
        zf.writestr(f"{prefix}/{sm_dir}/model.bim", bim_json)
        zf.writestr(f"{prefix}/{rpt_dir}/definition.pbir", pbir_json)
        zf.writestr(f"{prefix}/{rpt_dir}/definition/version.json", version_json)
        zf.writestr(f"{prefix}/{rpt_dir}/definition/report.json", report_config)
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
