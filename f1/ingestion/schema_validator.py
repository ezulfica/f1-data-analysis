import logging
from pathlib import Path

import polars as pl
import yaml

logger = logging.getLogger(__name__)

CONTRACTS_DIR = Path("config/contracts")


class SchemaViolationError(Exception):
    def __init__(self, violations: list[dict]) -> None:
        self.violations = violations
        details = "; ".join(
            f"{v['table']}: missing {v['missing']}" for v in violations
        )
        super().__init__(f"Schema contract violated: {details}")

    def to_dict(self) -> dict:
        return {"violations": self.violations}


def load_contract(table_name: str) -> list[str] | None:
    path = CONTRACTS_DIR / f"{table_name}.yml"
    if not path.exists():
        return None
    return yaml.safe_load(path.read_text())["columns"]


def validate_dump_schema(csv_paths: dict[str, Path]) -> None:
    violations = []
    for raw_name, path in csv_paths.items():
        table = raw_name.removeprefix("formula_one_")
        contract_cols = load_contract(table)
        if contract_cols is None:
            continue
        actual_cols = set(pl.read_csv(path, n_rows=0, infer_schema_length=0).columns)
        missing = set(contract_cols) - actual_cols
        if missing:
            violations.append({"table": table, "missing": sorted(missing)})

    if violations:
        raise SchemaViolationError(violations)

    contracted = sum(
        1 for raw_name in csv_paths
        if load_contract(raw_name.removeprefix("formula_one_")) is not None
    )
    logger.info("Schema validation passed — %d/%d tables under contract", contracted, len(csv_paths))
