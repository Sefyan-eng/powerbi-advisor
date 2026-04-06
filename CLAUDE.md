# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Power BI Model Advisor: upload an Excel/CSV file, Claude AI analyzes its structure, and returns an optimal Power BI data model with tables, relationships, DAX measures, and best practices. The user can then edit the model inline and generate an HTML report.

- **Language**: French (backend prompts/responses), English (frontend UI)
- **AI model used**: Claude Sonnet via Anthropic API

## Architecture

- **Backend** (`backend/main.py`): Single-file FastAPI app. Parses uploaded Excel/CSV with pandas, sends schema to Claude API for Power BI model recommendation, returns structured JSON. Generates HTML reports, `.bim` Tabular Model files, and pushes datasets to Power BI workspaces via REST API.
- **Frontend** (`frontend/index.html`): Single-file vanilla HTML/CSS/JS SPA (Data Studio aesthetic). Drag-and-drop upload, inline editing of tables/columns/relations/DAX measures, HTML report download, `.bim` export, and Power BI workspace push via modal.
- The backend serves the frontend as static files at `/` via `StaticFiles`.
- `_session_data` dict stores parsed DataFrames in memory (keyed by session_id) so the push-to-powerbi endpoint can access source data for row uploads.

## Commands

```bash
# Install dependencies
pip install -r backend/requirements.txt

# Run backend (requires ANTHROPIC_API_KEY env var)
cd backend && uvicorn main:app --host 0.0.0.0 --port 8000 --reload

# Or use the start script (checks API key, installs deps, starts server)
chmod +x start.sh && ./start.sh

# Frontend: open frontend/index.html with Live Server on port 5500
# Or access via backend at http://localhost:8000 (static mount)
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `POST /analyze` | Upload `.xlsx`/`.xls`/`.csv` (multipart) | Returns JSON with model_type, tables, relationships, measures, warnings, session_id |
| `POST /generate-report` | JSON body (ReportRequest) | Returns downloadable HTML report |
| `POST /generate-bim` | JSON body (ReportRequest) | Returns downloadable `.bim` Tabular Model file (for Tabular Editor) |
| `POST /push-to-powerbi` | JSON body (PushRequest with Azure AD creds) | Creates push dataset in Power BI workspace, optionally pushes data rows |
| `GET /health` | Health check | Returns status and API key presence |

## Power BI Integration

Two export paths:
1. **BIM file** (`/generate-bim`): Generates a Tabular Model JSON (compatibilityLevel 1550) with tables, columns (typed from pandas dtypes), relationships, and DAX measures. User opens in Tabular Editor and deploys to Power BI Service.
2. **Push to workspace** (`/push-to-powerbi`): Authenticates via Azure AD client credentials (tenant_id, client_id, client_secret), creates a push dataset via Power BI REST API, and optionally uploads data rows in 10K batches.

## Key Implementation Details

- Max upload size: 20 MB
- Claude prompt requests pure JSON response (no markdown), parsed via regex cleanup in `parse_json_response()`
- Pydantic models (`Table`, `Relationship`, `Measure`, `ReportRequest`, `PowerBIConfig`, `PushRequest`) define the data contract between frontend and backend
- HTML report generation is server-side string templating in `generate_html_report()`, saved to temp files
- Frontend `collectState()` scrapes the editable DOM to build the report/export payload
- CORS is fully open (`allow_origins=["*"]`)
- Pandas dtype to Power BI type mapping is in `PANDAS_TO_BIM_DTYPE` and `PANDAS_TO_REST_DTYPE`
