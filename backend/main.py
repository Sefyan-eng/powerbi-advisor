from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import anthropic
import json
import io
import os

app = FastAPI(title="Power BI Model Advisor")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))


def analyze_excel(file_bytes: bytes, filename: str) -> dict:
    """Read Excel and extract schema info from all sheets."""
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


def build_prompt(schema: dict) -> str:
    schema_txt = json.dumps(schema, ensure_ascii=False, indent=2)
    return f"""Tu es un expert Power BI et modélisation de données.
Voici le schéma d'un fichier Excel avec ses feuilles et colonnes :

{schema_txt}

Analyse ce schéma et propose le modèle de données Power BI le plus adéquat.
Réponds UNIQUEMENT en JSON valide avec la structure suivante :

{{
  "model_type": "Star Schema | Snowflake Schema | Flat Table | Composite",
  "summary": "Explication concise du modèle recommandé",
  "tables": [
    {{
      "name": "NomTable",
      "type": "Fact | Dimension | Bridge",
      "source_sheet": "NomFeuille",
      "columns": ["col1", "col2"],
      "primary_key": "col_pk ou null",
      "description": "Rôle de cette table"
    }}
  ],
  "relationships": [
    {{
      "from_table": "Table1",
      "from_column": "col",
      "to_table": "Table2",
      "to_column": "col",
      "cardinality": "Many-to-One | One-to-One | Many-to-Many",
      "cross_filter": "Single | Both"
    }}
  ],
  "measures_suggested": [
    {{
      "name": "Nom Mesure",
      "dax": "DAX formula",
      "description": "Ce que mesure cette métrique"
    }}
  ],
  "warnings": ["Avertissement 1", "Avertissement 2"],
  "best_practices": ["Conseil 1", "Conseil 2"]
}}"""


@app.post("/analyze")
async def analyze(file: UploadFile = File(...)):
    if not file.filename.endswith((".xlsx", ".xls", ".csv")):
        raise HTTPException(400, "Fichier non supporté. Utilisez .xlsx, .xls ou .csv")
    
    content = await file.read()
    
    try:
        if file.filename.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(content))
            schema = {"Sheet1": [
                {"name": c, "dtype": str(df[c].dtype), "nulls": int(df[c].isnull().sum()),
                 "unique": int(df[c].nunique()), "sample": [str(v) for v in df[c].dropna().head(3).tolist()],
                 "rows": len(df)}
                for c in df.columns
            ]}
        else:
            schema = analyze_excel(content, file.filename)
    except Exception as e:
        raise HTTPException(400, f"Erreur lecture fichier: {str(e)}")

    prompt = build_prompt(schema)
    
    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=4096,
        messages=[{"role": "user", "content": prompt}]
    )
    
    raw = message.content[0].text.strip()
    # Strip markdown fences if present
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    raw = raw.strip()
    
    try:
        result = json.loads(raw)
    except json.JSONDecodeError:
        raise HTTPException(500, "Erreur parsing réponse Claude")
    
    result["schema"] = schema
    result["filename"] = file.filename
    return result


@app.get("/health")
def health():
    return {"status": "ok"}
