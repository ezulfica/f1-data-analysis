import io
import zipfile
from pathlib import Path

import polars as pl
import pytest
import yaml

from ingestion.schema_validator import (
    SchemaViolationError,
    load_contract,
    validate_dump_schema,
)


def _write_contract(tmp_path: Path, table: str, columns: list[str]) -> None:
    contract_dir = tmp_path / "contracts"
    contract_dir.mkdir(exist_ok=True)
    (contract_dir / f"{table}.yml").write_text(yaml.dump({"columns": columns}))


def _write_csv(tmp_path: Path, name: str, columns: list[str]) -> Path:
    path = tmp_path / f"{name}.csv"
    pl.DataFrame({col: [] for col in columns}).write_csv(path)
    return path


@pytest.fixture(autouse=True)
def _patch_contracts_dir(tmp_path, monkeypatch):
    import ingestion.schema_validator as sv
    monkeypatch.setattr(sv, "CONTRACTS_DIR", tmp_path / "contracts")
    (tmp_path / "contracts").mkdir()


def test_load_contract_none_si_absent(tmp_path):
    assert load_contract("inexistant") is None


def test_load_contract_retourne_colonnes(tmp_path):
    _write_contract(tmp_path, "driver", ["id", "name", "nationality"])
    assert load_contract("driver") == ["id", "name", "nationality"]


def test_validate_passe_si_colonnes_ok(tmp_path):
    _write_contract(tmp_path, "driver", ["id", "name"])
    csv_paths = {"formula_one_driver": _write_csv(tmp_path, "formula_one_driver", ["id", "name", "extra_col"])}
    validate_dump_schema(csv_paths)  # no exception


def test_validate_leve_si_colonne_manquante(tmp_path):
    _write_contract(tmp_path, "driver", ["id", "name", "nationality"])
    csv_paths = {"formula_one_driver": _write_csv(tmp_path, "formula_one_driver", ["id", "name"])}
    with pytest.raises(SchemaViolationError) as exc_info:
        validate_dump_schema(csv_paths)
    assert exc_info.value.violations[0]["table"] == "driver"
    assert "nationality" in exc_info.value.violations[0]["missing"]


def test_validate_skip_si_pas_de_contrat(tmp_path):
    csv_paths = {"formula_one_unknown_table": _write_csv(tmp_path, "formula_one_unknown_table", ["col1"])}
    validate_dump_schema(csv_paths)  # no contract → no exception


def test_schema_violation_error_message():
    err = SchemaViolationError([{"table": "driver", "missing": ["nationality"]}])
    assert "driver" in str(err)
    assert "nationality" in str(err)
