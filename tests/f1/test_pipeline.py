import io
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ingestion.pipeline import _extract_csvs, _fetch_metadata, build_pipeline
from ingestion.models import DumpMetadata


def _make_zip(files: dict[str, str]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for name, content in files.items():
            z.writestr(name, content)
    return buf.getvalue()


def test_fetch_metadata_parse_response():
    payload = {
        "delayed_dumps": {
            "csv": {
                "download_url": "https://example.com/dump.zip",
                "file_hash": "abc123",
                "file_size": 1024,
                "uploaded_at": "2025-01-01T00:00:00Z",
            }
        }
    }
    mock_response = MagicMock()
    mock_response.json.return_value = payload

    with patch("ingestion.pipeline.requests.get", return_value=mock_response) as mock_get:
        from ingestion.models import JolpicaSettings
        settings = JolpicaSettings(metadata_url="https://example.com/meta")
        result = _fetch_metadata(settings)

    mock_get.assert_called_once_with("https://example.com/meta", timeout=10)
    assert isinstance(result, DumpMetadata)
    assert result.file_hash == "abc123"
    assert result.file_size == 1024


def test_extract_csvs_retourne_dict(tmp_path):
    zip_bytes = _make_zip({
        "formula_one_driver.csv": "id,name\n1,Hamilton\n",
        "formula_one_circuit.csv": "id,name\n1,Monza\n",
    })
    result = _extract_csvs(zip_bytes, tmp_path)

    assert set(result.keys()) == {"formula_one_driver", "formula_one_circuit"}
    assert all(isinstance(v, Path) for v in result.values())
    assert result["formula_one_driver"].read_text().startswith("id,name")


def test_build_pipeline_destination_default(monkeypatch):
    monkeypatch.delenv("DLT_DESTINATION", raising=False)
    from ingestion.models import DltSettings
    settings = DltSettings(pipeline_name="test_pipe", dataset_name="test_ds")
    pipeline = build_pipeline(settings)
    assert pipeline.pipeline_name == "test_pipe"


def test_build_pipeline_destination_env(monkeypatch):
    monkeypatch.setenv("DLT_DESTINATION", "motherduck")
    from ingestion.models import DltSettings
    settings = DltSettings(pipeline_name="test_pipe", dataset_name="test_ds")
    pipeline = build_pipeline(settings)
    assert pipeline.pipeline_name == "test_pipe"
