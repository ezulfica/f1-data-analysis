import logging
from pathlib import Path

import yaml

from ingestion.models import Settings
from ingestion.pipeline import build_pipeline, jolpica_source
from ingestion.schema_validator import SchemaViolationError

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    settings = Settings.model_validate(
        yaml.safe_load(Path("config/settings.yaml").read_text())
    )
    pipeline = build_pipeline(settings.dlt)
    try:
        info = pipeline.run(jolpica_source(settings))
        logger.info("Pipeline completed: %s", info)
    except SchemaViolationError as e:
        logger.error("Schema violation — pipeline aborted:\n%s", e)
        raise


if __name__ == "__main__":
    main()
