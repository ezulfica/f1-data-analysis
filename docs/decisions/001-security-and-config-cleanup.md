# ADR 001 — Sécurisation des credentials et centralisation de la config

**Date** : 2026-05-10
**Statut** : Accepté

---

## Contexte

En préparant l'évolution du projet vers une data platform complète, trois problèmes de fondation ont été identifiés :

1. **`.gitignore` ciblait le mauvais fichier** : la service account GCP était protégée par son statut "non tracké" (untracked), pas par le `.gitignore`. Le pattern `config/f1-dataanalysis-b14aabd9fd9e.json` ne correspondait pas au fichier réel `config/f1-dataanalysis-ba8211656bb8.json`. Un `git add .` accidentel aurait exposé la clé privée RSA dans l'historique git.

2. **La service account JSON était dans le répertoire du projet** : même non trackée, sa présence dans `config/` représentait un risque de commit accidentel et une mauvaise pratique (les secrets ne doivent pas cohabiter avec le code).

3. **Le chargement de la config était dupliqué** : `dotenv_values("config/.env")` était répété dans trois modules (`data_ingestion/main.py`, `data_prep/main.py`, `data_load_into_bigquery/main.py`), avec le chemin codé en dur à chaque fois.

---

## Décisions

### 1. Fix .gitignore avec un wildcard

**Avant**
```
config/f1-dataanalysis-b14aabd9fd9e.json
```

**Après**
```
config/f1-dataanalysis-*.json
config/*.json
```

Le wildcard protège tout fichier JSON de service account dans `config/`, quelle que soit la version ou le hash dans le nom.

### 2. Déplacer la service account JSON hors du repo

Le fichier est déplacé vers `~/.gcp/f1-dataanalysis-sa.json`, en dehors de tout répertoire versionné.

La variable `SERVICE_ACCOUNT_JSON_PATH` dans `.env` pointe désormais vers ce chemin absolu. Le `.env-example` documente la convention.

Cette approche suit le principe de séparation configuration/code : les secrets appartiennent au système, pas au projet.

### 3. Centraliser le config loading dans `utils/config.py`

Ajout d'une fonction `get_config()` qui charge `.env` via `dotenv_values` avec un chemin résolu dynamiquement depuis la racine du projet :

```python
_ENV_PATH = Path(__file__).parent.parent / "config" / ".env"

def get_config() -> dict:
    return dotenv_values(_ENV_PATH)
```

Les trois modules importent désormais `from utils.config import get_config` au lieu de dupliquer la logique de chargement.

---

## Conséquences

### Pour quelqu'un qui clone le repo

1. Copier `.env-example` → `.env` et remplir les valeurs
2. Placer la service account GCP dans `~/.gcp/f1-dataanalysis-sa.json` (ou tout autre chemin hors du repo)
3. Mettre à jour `SERVICE_ACCOUNT_JSON_PATH` dans `.env` avec le chemin exact

### Ce qui change en interne

- Le chemin du `.env` n'est plus une constante `"config/.env"` dupliquée — il est résolu une seule fois dans `utils/config.py`
- Toute modification future du nom ou de l'emplacement du `.env` se fait en un seul endroit

### Ce qui n'a pas changé

- La logique métier des pipelines est inchangée
- `dotenv_values` est toujours le mécanisme sous-jacent
- Les clés AWS/GCP référencées dans le code restent identiques
