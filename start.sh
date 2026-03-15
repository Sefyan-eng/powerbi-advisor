#!/bin/bash
# start.sh — Lance le backend FastAPI

echo "🔍 Vérification de la clé API..."
if [ -z "$ANTHROPIC_API_KEY" ]; then
  echo "❌ ANTHROPIC_API_KEY non définie."
  echo "   Exporte-la d'abord : export ANTHROPIC_API_KEY=sk-ant-..."
  exit 1
fi

echo "📦 Installation des dépendances..."
pip install -r backend/requirements.txt -q

echo "🚀 Lancement du serveur FastAPI sur http://localhost:8000"
echo "   Ouvre frontend/index.html avec Live Server (port 5500)"
echo ""
cd backend && uvicorn main:app --host 0.0.0.0 --port 8000 --reload
