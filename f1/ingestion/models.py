from __future__ import annotations

from pydantic import BaseModel


class JolpicaSettings(BaseModel):
    metadata_url: str
    timeout_metadata: int = 10
    timeout_download: int = 120


class DltSettings(BaseModel):
    pipeline_name: str
    dataset_name: str
    schema_length: int = 10000
    write_disposition: str = "merge"
    primary_key: str = "id"


class Settings(BaseModel):
    jolpica: JolpicaSettings
    dlt: DltSettings


class DumpMetadata(BaseModel):
    download_url: str
    file_hash: str
    file_size: int
    uploaded_at: str
