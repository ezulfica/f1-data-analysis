import logging
import os
import tempfile
import zipfile
from pathlib import Path

import dlt
import polars as pl
import requests

from ingestion.models import DltSettings, DumpMetadata, JolpicaSettings, Settings
from ingestion.schema_validator import load_contract, validate_dump_schema

logger = logging.getLogger(__name__)

# Resource that persists the last-loaded hash; merge disposition prevents state reset.
_HASH_RESOURCE = "_dump_metadata"


def _fetch_metadata(settings: JolpicaSettings) -> DumpMetadata:
    response = requests.get(settings.metadata_url, timeout=settings.timeout_metadata)
    response.raise_for_status()
    return DumpMetadata.model_validate(response.json()["delayed_dumps"]["csv"])


def _download_zip(metadata: DumpMetadata, settings: JolpicaSettings) -> bytes:
    logger.info("Downloading dump (%.1f MB)", metadata.file_size / 1_000_000)
    response = requests.get(
        metadata.download_url,
        allow_redirects=True,
        timeout=settings.timeout_download,
    )
    response.raise_for_status()
    return response.content


def _extract_csvs(zip_bytes: bytes, tmp_dir: Path) -> dict[str, Path]:
    zip_path = tmp_dir / "dump.zip"
    zip_path.write_bytes(zip_bytes)
    extract_dir = tmp_dir / "extracted"
    extract_dir.mkdir()
    with zipfile.ZipFile(zip_path) as z:
        z.extractall(extract_dir)
    return {p.stem: p for p in extract_dir.glob("*.csv")}


@dlt.resource(name=_HASH_RESOURCE, write_disposition="merge", primary_key="id")
def _dump_hash_resource(file_hash: str):
    # dlt resets resource_state for write_disposition="replace" resources before
    # each extraction run. Using "merge" here prevents that reset so the hash
    # survives between pipeline runs.
    rs = dlt.current.resource_state()
    rs["hash"] = file_hash
    yield {"id": 1, "file_hash": file_hash}


@dlt.source(name="jolpica")
def jolpica_source(settings: Settings):
    # source_state() here is read-only (dlt discards any mutations in @dlt.source).
    # We read the hash stored by _dump_hash_resource on the previous successful run.
    state = dlt.current.source_state()
    metadata = _fetch_metadata(settings.jolpica)

    stored_hash = state.get("resources", {}).get(_HASH_RESOURCE, {}).get("hash")
    if stored_hash == metadata.file_hash:
        logger.info("No new dump available — nothing to do.")
        return

    zip_bytes = _download_zip(metadata, settings.jolpica)

    with tempfile.TemporaryDirectory() as tmp:
        csv_paths = _extract_csvs(zip_bytes, Path(tmp))
        validate_dump_schema(csv_paths)

        resources = []
        for raw_name, path in csv_paths.items():
            table_name = raw_name.removeprefix("formula_one_")
            contract_cols = load_contract(table_name)
            df = pl.read_csv(
                path,
                infer_schema_length=settings.dlt.schema_length,
                null_values=[""],
            )
            if contract_cols:
                df = df.select(contract_cols)
            resources.append(
                dlt.resource(
                    df.to_dicts(),
                    name=table_name,
                    write_disposition=settings.dlt.write_disposition,
                    primary_key=settings.dlt.primary_key,
                )
            )
    # temp dir cleaned up — all row data already in memory via df.to_dicts()

    yield _dump_hash_resource(metadata.file_hash)
    yield from resources


def build_pipeline(settings: DltSettings) -> dlt.Pipeline:
    return dlt.pipeline(
        pipeline_name=settings.pipeline_name,
        destination=os.getenv("DLT_DESTINATION", "duckdb"),
        dataset_name=settings.dataset_name,
    )
