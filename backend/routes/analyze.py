import io
import re
import uuid
from typing import Dict

import pandas as pd
from fastapi import APIRouter, UploadFile, File, HTTPException

from config import MAX_FILE_SIZE, get_client
from services.analyzer import analyze_excel, parse_json_response, build_prompt, _session_data

router = APIRouter()


@router.post("/analyze")
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
