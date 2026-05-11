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

### 2. DuckDB comme émulation BigQuery (dbt)

#### Stratégie des profils

Trois targets dbt couvrent les trois environnements :

| Target | Type | Profil | Quand |
|--------|------|--------|-------|
| `local` | DuckDB | `local/dbt/profiles.yml` | docker-compose, CI |
| `dev` | BigQuery | `dbt_projects/profiles.yml` | dataset `staging` sur GCP |
| `prod` | BigQuery | `dbt_projects/profiles.yml` | dataset prod sur GCP (à créer) |

La variable `DBT_TARGET` contrôle le target utilisé par le DAG Airflow (défaut : `dev`). Localement : `DBT_TARGET=local`.

DuckDB fonctionne en mode **embarqué** — pas de serveur séparé. Le fichier `.duckdb` est persisté dans un bind mount (`./local/duckdb/`) accessible depuis le host pour inspection directe (DBeaver, notebooks).

**Pourquoi pas PostgreSQL ?** DuckDB a une compatibilité SQL analytique bien supérieure avec BigQuery (window functions, `QUALIFY`, `STRUCT`, types analytiques). PostgreSQL nécessiterait de réécrire davantage de modèles.

#### Compatibilité SQL DuckDB ↔ BigQuery

DuckDB couvre l'essentiel du SQL analytique BigQuery, mais certaines constructions diffèrent :

| Construct | BigQuery | DuckDB | Stratégie |
|-----------|----------|--------|-----------|
| Types numériques | `FLOAT64`, `INT64` | `DOUBLE`, `BIGINT` | Géré par les adapters dbt |
| `DATE_DIFF` | `DATE_DIFF(d1,d2,DAY)` | `datediff('day',d1,d2)` | Macro cross-adapter |
| `FORMAT_DATE` | `FORMAT_DATE('%Y',d)` | `strftime(d,'%Y')` | Macro cross-adapter |
| `SAFE_DIVIDE` | natif | non natif | Macro cross-adapter |
| `GENERATE_DATE_ARRAY` | natif | non natif | Macro cross-adapter si besoin |
| `SELECT * EXCEPT(col)` | natif | non supporté | À éviter dans les modèles |
| Window functions | ✓ | ✓ | Aucun ajustement |
| `QUALIFY` | ✓ | ✓ | Aucun ajustement |

**Stratégie de compatibilité** : les fonctions BigQuery-spécifiques sont encapsulées dans des macros dbt (`macros/compat/`) qui dispatchent vers l'implémentation correcte selon `{{ target.type }}`. Les modèles utilisent `{{ safe_divide(a, b) }}` au lieu de la syntaxe native.

Un audit des 22 modèles existants est nécessaire avant de valider la compatibilité complète (étape à venir sur cette branche ou une branche dédiée).

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
