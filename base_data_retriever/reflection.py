import logging
from sqlalchemy import MetaData
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

cache = {}

def reflect_metadata(engine: Engine, schema: str) -> MetaData:
    try:
        meta = cache["database-reflection"]
    except KeyError:
        logger.info("Reflecting metadata from database")
        meta = MetaData(bind = engine,schema = schema)
        meta.reflect()
        cache["database-reflection"] = meta
    else:
        logger.debug("Returned metadata from cache")
    return meta
