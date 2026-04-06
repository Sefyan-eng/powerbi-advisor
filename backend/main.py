import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

# Import config first to trigger .env loading
import config  # noqa: F401

from routes.analyze import router as analyze_router
from routes.export import router as export_router
from routes.deploy import router as deploy_router
from routes.dashboard import router as dashboard_router
from routes.push import router as push_router

app = FastAPI(title="Power BI Model Advisor")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── Include routers ──
app.include_router(analyze_router)
app.include_router(export_router)
app.include_router(deploy_router)
app.include_router(dashboard_router)
app.include_router(push_router)


@app.get("/health")
def health():
    return {"status": "ok", "api_key_configured": bool(os.environ.get("ANTHROPIC_API_KEY"))}


# ── Serve frontend static files ──
frontend_path = os.path.join(os.path.dirname(__file__), "..", "frontend")
if os.path.isdir(frontend_path):
    app.mount("/", StaticFiles(directory=frontend_path, html=True), name="frontend")
