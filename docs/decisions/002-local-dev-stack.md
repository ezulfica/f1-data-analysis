# ADR 002 — Stack local et architecture de déploiement

**Date** : 2026-05-11
**Statut** : Accepté

---

## Contexte

Le projet nécessite deux choses distinctes :

1. Un **environnement de développement local** pour tester le pipeline sans dépendre du cloud
2. Une **stratégie de déploiement** claire pour que l'infrastructure de prod (serveur Airflow centralisé) puisse exécuter la dernière version du pipeline

---

## Architecture cible

```
F1 repo (ce repo)
├── Pipeline code + dbt models
├── Dockerfile  →  image buildée par le CI
└── docker-compose.yml  →  stack local (LocalStack + Airflow)

        ↓ CI push sur merge main / dev

Registry (ECR ou Docker Hub)
    f1-pipeline:latest   ← branche main
    f1-pipeline:dev      ← branche dev

        ↓ Airflow tire l'image (repo dédié, étape future)

Serveur Airflow centralisé
└── DockerOperator / KubernetesPodOperator
        image: f1-pipeline:latest (prod) ou f1-pipeline:dev (dev)
```

Le serveur Airflow est dans un **repo séparé** — il orchestre plusieurs projets data, pas seulement F1. Le repo F1 expose une image Docker versionnée ; Airflow n'a pas accès au code source directement.

---

## Décisions

### 1. LocalStack pour l'émulation AWS (S3)

LocalStack (community, gratuit) émule S3, IAM, Lambda et EventBridge localement.

**Pourquoi pas MinIO ?** MinIO ne couvre que S3. LocalStack permettra aussi de tester les Lambdas de start/stop EC2 quand on arrivera à la couche Terraform.

`boto3` pointe vers LocalStack via `S3_ENDPOINT_URL=http://localstack:4566`. En prod, cette variable est absente → boto3 utilise le vrai AWS.

### 2. DuckDB pour l'émulation BigQuery (dbt)

Le profil dbt `local` (dans `local/dbt/profiles.yml`) pointe vers un fichier DuckDB au lieu de BigQuery.

**Pourquoi pas PostgreSQL ?** BigQuery et DuckDB ont une meilleure compatibilité SQL analytique. DuckDB ne nécessite pas de serveur séparé.

La variable `DBT_TARGET` contrôle le target utilisé par le DAG Airflow (défaut : `dev`).

### 3. docker-compose.yml comme stack de développement

`docker-compose.yml` orchestre LocalStack + Airflow en local. L'Airflow local utilise le même `Dockerfile` que la prod — même pattern, variables différentes.

C'est le seul `docker-compose.yml` du repo (pas de suffixe `.local` — c'est son usage naturel).

### 4. Politique de tags Docker

| Branche | Tag | Utilisé par |
|---------|-----|-------------|
| `main` | `f1-pipeline:latest` | Airflow prod |
| `dev` | `f1-pipeline:dev` | Airflow dev |
| PR / feature | `f1-pipeline:<git-sha>` | CI uniquement |

Le CI/CD (GitHub Actions, étape future) buildera et pushera automatiquement l'image sur merge vers `main` ou `dev`.

### 5. Makefile pour le développement sans Airflow

Un `Makefile` expose les étapes individuelles du pipeline (`make ingest`, `make dbt`, etc.) pour un feedback rapide sans démarrer le stack complet.

---

## Conséquences

### Pour lancer le stack local

```bash
cp config/.env.local.example config/.env.local
make up                    # démarre LocalStack + Airflow
make s3-status             # vérifie que LocalStack est prêt
make pipeline              # lance le pipeline complet sans Airflow
```

### Ce qui ne change pas

- Le code métier (ingestion, prep, load) est identique local/prod
- Les modèles dbt sont partagés — seul le profil change via `DBT_TARGET`

### Ce qui n'est pas couvert ici

- Build et push de l'image Docker (CI/CD — étape suivante)
- Repo Airflow centralisé (étape suivante)
- Terraform pour l'infrastructure cloud (étape `feat/terraform-infra`)
