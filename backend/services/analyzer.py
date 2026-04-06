import io
import json
import re
from typing import Dict

import pandas as pd

# ── In-memory store for parsed Excel data (keyed by analysis session) ──
_session_data: Dict[str, Dict[str, pd.DataFrame]] = {}


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
