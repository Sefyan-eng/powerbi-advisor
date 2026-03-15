# 🏗️ Power BI Model Advisor

> Upload un fichier Excel → Claude analyse la structure → Reçois le modèle de données Power BI optimal avec les relations et mesures DAX.

## Stack

| Couche | Technologie |
|--------|-------------|
| Frontend | HTML/CSS/JS pur (pas de build) |
| Backend | FastAPI + Python |
| IA | Claude claude-sonnet-4-20250514 via API Anthropic |
| Parsing Excel | pandas + openpyxl |

## Lancement sur GitHub Codespaces

### 1. Ouvre le repo dans Codespaces
```
Code → Codespaces → Create codespace on main
```

### 2. Configure ta clé API Anthropic
```bash
export ANTHROPIC_API_KEY=sk-ant-xxxxxxxxxx
```
> 💡 Ou ajoute-la dans les **Codespace Secrets** : Settings → Secrets → `ANTHROPIC_API_KEY`

### 3. Lance le backend
```bash
chmod +x start.sh && ./start.sh
```

### 4. Ouvre le frontend
- Clique-droit sur `frontend/index.html`
- **"Open with Live Server"** (port 5500)

## Structure du projet

```
powerbi-advisor/
├── .devcontainer/
│   └── devcontainer.json     # Config Codespaces
├── backend/
│   ├── main.py               # API FastAPI
│   └── requirements.txt      # Dépendances Python
├── frontend/
│   └── index.html            # Interface utilisateur
├── start.sh                  # Script de lancement
└── README.md
```

## Ce que l'IA génère

- **Modèle recommandé** : Star Schema, Snowflake, Flat Table ou Composite
- **Tables** : Fact, Dimension ou Bridge avec leurs colonnes et clés primaires
- **Relations** : Cardinalité (Many-to-One, etc.) et filtrage croisé
- **Mesures DAX** : Formules prêtes à copier dans Power BI
- **Avertissements** : Problèmes potentiels détectés (nulls, doublons, etc.)
- **Bonnes pratiques** : Conseils de modélisation

## Formats supportés

- `.xlsx` (Excel 2007+)
- `.xls` (Excel ancien)
- `.csv`

## Variables d'environnement

| Variable | Description |
|----------|-------------|
| `ANTHROPIC_API_KEY` | Clé API Anthropic (obligatoire) |
