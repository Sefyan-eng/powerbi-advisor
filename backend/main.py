from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
from pydantic import BaseModel
from typing import List, Optional
import pandas as pd
import anthropic
import json, io, os, re, tempfile, uuid
from datetime import datetime

app = FastAPI(title="Power BI Model Advisor")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

MAX_FILE_SIZE = 20 * 1024 * 1024

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
    try:
        if file.filename.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(content))
            schema = {"Sheet1": [{"name": c, "dtype": str(df[c].dtype), "nulls": int(df[c].isnull().sum()),
                "unique": int(df[c].nunique()), "sample": [str(v) for v in df[c].dropna().head(3).tolist()],
                "rows": len(df)} for c in df.columns]}
        else:
            schema = analyze_excel(content)
    except Exception as e:
        raise HTTPException(400, f"Erreur lecture : {str(e)}")
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

frontend_path = os.path.join(os.path.dirname(__file__), "..", "frontend")
if os.path.isdir(frontend_path):
    app.mount("/", StaticFiles(directory=frontend_path, html=True), name="frontend")
