import os
import logging
from pathlib import Path
from fastapi import HTTPException
import anthropic

# Auto-load .env from project root (works on Windows without bash)
_env_file = Path(__file__).resolve().parent.parent / ".env"
if _env_file.exists():
    for line in _env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

logger = logging.getLogger("powerbi-advisor")

MAX_FILE_SIZE = 20 * 1024 * 1024


def get_client():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise HTTPException(500, "ANTHROPIC_API_KEY non définie.")
    return anthropic.Anthropic(api_key=api_key)
