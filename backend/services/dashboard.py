import json
import logging
import re
from typing import List

from services.mcp_client import MCPClient

logger = logging.getLogger("powerbi-advisor")


async def get_dashboard_data(mcp: MCPClient) -> dict:
    """Query the connected Power BI model via MCP and return chart-ready data
    plus Python matplotlib scripts for PBI Desktop Python visuals.

    Steps:
    1. List tables and their column schemas
    2. List measures
    3. For each measure, execute DAX to get KPI scalar values
    4. For dimension columns x top 3 measures, execute DAX TOPN/SUMMARIZECOLUMNS
    5. Generate matplotlib Python scripts (dark theme, horizontal bar charts)

    Returns: {kpis: [...], charts: [...], python_scripts: [...]}
    """
    result = {"kpis": [], "charts": [], "python_scripts": [], "errors": []}

    # ── 1. Discover tables and columns ──
    tables_resp = await mcp.list_tables()
    tables_text = mcp._extract_text(tables_resp)

    table_schemas = {}  # {table_name: {col_name: col_datatype, ...}}
    dimension_columns = []  # [(table_name, col_name), ...]
    try:
        tables_data = json.loads(tables_text)
        table_names = [t.get("name", "") for t in tables_data.get("data", []) if t.get("name")]
    except (json.JSONDecodeError, TypeError):
        table_names = list(set(re.findall(r'"name"\s*:\s*"([^"]+)"', tables_text)))

    for tbl_name in table_names:
        try:
            schema_resp = await mcp.get_table_columns(tbl_name)
            schema_text = mcp._extract_text(schema_resp)
            schema_data = json.loads(schema_text)
            cols_info = {}
            for c in schema_data.get("data", {}).get("Columns", []):
                if c.get("isHidden"):
                    continue
                col_name = c.get("name", "")
                col_type = c.get("dataType", "String")
                cols_info[col_name] = col_type
                # Collect string/dimension columns for chart breakdowns
                if col_type in ("String", "string", "text") and not col_name.lower().endswith("id"):
                    dimension_columns.append((tbl_name, col_name))
            table_schemas[tbl_name] = cols_info
        except Exception as e:
            logger.warning(f"Dashboard: failed to get schema for {tbl_name}: {e}")
            table_schemas[tbl_name] = {}

    # ── 2. List measures ──
    measures_resp = await mcp.list_measures()
    measures_text = mcp._extract_text(measures_resp)

    measure_names = []
    try:
        measures_data = json.loads(measures_text)
        for m in measures_data.get("data", []):
            name = m.get("name", "")
            if name:
                measure_names.append(name)
    except (json.JSONDecodeError, TypeError):
        measure_names = list(set(re.findall(r'"name"\s*:\s*"([^"]+)"', measures_text)))

    if not measure_names:
        result["errors"].append("No measures found in the model.")
        return result

    # ── 3. KPI values: evaluate each measure as a scalar ──
    for measure_name in measure_names:
        try:
            dax_query = f'EVALUATE ROW("value", [{measure_name}])'
            dax_resp = await mcp.execute_dax(dax_query)
            dax_text = mcp._extract_text(dax_resp)
            dax_data = json.loads(dax_text)
            # MCP returns: {data: {rows: [{"[value]": 123}], columns: [...]}}
            value = None
            inner = dax_data.get("data", {})
            rows = inner.get("rows", []) if isinstance(inner, dict) else []
            if rows and isinstance(rows[0], dict):
                value = rows[0].get("[value]", rows[0].get("value", list(rows[0].values())[0]))
            result["kpis"].append({
                "name": measure_name,
                "value": value,
            })
        except Exception as e:
            logger.warning(f"Dashboard KPI: failed to evaluate [{measure_name}]: {e}")
            result["kpis"].append({
                "name": measure_name,
                "value": None,
            })

    # ── 4. Chart data: dimension columns x top 3 measures ──
    top_measures = measure_names[:3]

    for tbl_name, dim_col in dimension_columns:
        for measure_name in top_measures:
            chart_key = f"{tbl_name}[{dim_col}] by [{measure_name}]"
            try:
                dax_query = (
                    f'EVALUATE TOPN(15, '
                    f"SUMMARIZECOLUMNS('{tbl_name}'[{dim_col}], "
                    f'"value", [{measure_name}]), '
                    f'[value], DESC)'
                )
                dax_resp = await mcp.execute_dax(dax_query)
                dax_text = mcp._extract_text(dax_resp)
                dax_data = json.loads(dax_text)
                # MCP returns: {data: {rows: [{"'Table'[Col]": "val", "[value]": 123}]}}
                inner = dax_data.get("data", {})
                rows = inner.get("rows", []) if isinstance(inner, dict) else []
                if not dax_data.get("success", False):
                    continue

                labels = []
                values = []
                # Column name in result might be 'Table'[Col] or just [Col]
                dim_key_candidates = [
                    f"'{tbl_name}'[{dim_col}]", f"[{dim_col}]", dim_col,
                    f"{tbl_name}[{dim_col}]",
                ]
                for row in rows:
                    if isinstance(row, dict):
                        label = None
                        for k in dim_key_candidates:
                            if k in row:
                                label = row[k]
                                break
                        if label is None:
                            # Try first non-value key
                            for k, v in row.items():
                                if k not in ("[value]", "value"):
                                    label = v
                                    break
                        val = row.get("[value]", row.get("value", 0))
                        labels.append(str(label or ""))
                        try:
                            values.append(float(val))
                        except (ValueError, TypeError):
                            values.append(0)

                result["charts"].append({
                    "title": chart_key,
                    "dimension_table": tbl_name,
                    "dimension_column": dim_col,
                    "measure": measure_name,
                    "labels": labels,
                    "values": values,
                })
            except Exception as e:
                logger.warning(f"Dashboard chart: failed for {chart_key}: {e}")
                result["errors"].append(f"Chart {chart_key}: {str(e)}")

    # ── 5. Generate Python matplotlib scripts for PBI Desktop Python visuals ──
    for chart in result["charts"]:
        script = _build_matplotlib_script(
            title=chart["title"],
            labels=chart["labels"],
            values=chart["values"],
            dim_col=chart["dimension_column"],
            measure=chart["measure"],
        )
        result["python_scripts"].append({
            "title": chart["title"],
            "script": script,
        })

    return result


def _build_matplotlib_script(title: str, labels: List[str], values: List[float],
                              dim_col: str, measure: str) -> str:
    """Generate a matplotlib Python script for a Power BI Desktop Python visual.

    Uses dark theme and horizontal bar charts for readability.
    The script assumes `dataset` DataFrame is provided by PBI Desktop runtime.
    """
    # Sanitize strings for embedding in Python source
    safe_title = title.replace("'", "\\'").replace('"', '\\"')
    safe_dim = dim_col.replace("'", "\\'").replace('"', '\\"')
    safe_measure = measure.replace("'", "\\'").replace('"', '\\"')

    return f'''# Power BI Python Visual — {safe_title}
# Paste this into a Python visual in Power BI Desktop.
# Drag '{safe_dim}' and '{safe_measure}' into the Values well.

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

# ── Dark theme ──
plt.rcParams.update({{
    'figure.facecolor': '#0a0a0f',
    'axes.facecolor': '#111118',
    'axes.edgecolor': '#2a2a3a',
    'axes.labelcolor': '#e8e8f0',
    'text.color': '#e8e8f0',
    'xtick.color': '#888888',
    'ytick.color': '#888888',
    'grid.color': '#2a2a3a',
    'grid.alpha': 0.5,
    'font.size': 10,
}})

# ── Data from PBI Desktop ──
# The 'dataset' DataFrame is auto-provided by Power BI when columns are in Values.
try:
    labels = dataset["{safe_dim}"].astype(str).tolist()
    values = dataset["value"].tolist()
except KeyError:
    # Fallback: use embedded snapshot data
    labels = {labels!r}
    values = {values!r}

# ── Horizontal bar chart ──
fig, ax = plt.subplots(figsize=(8, max(4, len(labels) * 0.4)))

bars = ax.barh(range(len(labels)), values, color='#f7c52e', edgecolor='#f7c52e', alpha=0.85, height=0.6)

ax.set_yticks(range(len(labels)))
ax.set_yticklabels(labels, fontsize=9)
ax.invert_yaxis()
ax.set_xlabel("{safe_measure}", fontsize=11, fontweight='bold')
ax.set_title("{safe_title}", fontsize=13, fontweight='bold', color='#f7c52e', pad=15)
ax.grid(axis='x', linestyle='--')

# Value labels on bars
for bar, val in zip(bars, values):
    ax.text(bar.get_width() + max(values) * 0.01, bar.get_y() + bar.get_height() / 2,
            f'{{val:,.0f}}', va='center', fontsize=8, color='#aaaaaa')

plt.tight_layout()
plt.show()
'''
