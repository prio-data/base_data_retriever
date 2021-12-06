import logging
from typing import Optional
import psycopg2
import sqlalchemy
import views_query_planning
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)

class DatabaseLayer():
    """
    DatabaseLayer
    =============

    A class that allows managed access to a database, as well as interfacing
    nicely with FastAPIs Depends function.
    """
    _metadata = None

    def __init__(self,
            host: str,
            port: int,
            user: str,
            name: str,
            sslmode: str,
            schema: str = "public",
            password: Optional[str] = None,
            metadata: Optional[sqlalchemy.MetaData] = None,
            pool_size: int = 20,
            max_overflow: int = 0):

        self.host     = host
        self.port     = port
        self.user     = user
        self.dbname   = name
        self.sslmode  = sslmode
        self.password = password
        self.schema = schema


        self._engine  = sqlalchemy.create_engine(
                "postgresql+psycopg2://",
                creator = self._creator_factory(),
                pool_size = pool_size,
                max_overflow = max_overflow)

        self._metadata = metadata

        if self._metadata is not None:
            self._metadata.bind = self._engine

        self.Session  = sessionmaker(bind = self._engine)

    @property
    def metadata(self):
        if self._metadata is None:
            logger.debug(f"Reflecting metadata for database postgresql://{self.host}/{self.dbname}")
            self._metadata = sqlalchemy.MetaData(bind = self._engine, schema = self.schema)
            self._metadata.reflect()
        return self._metadata

    @property
    def connection_dependency(self):
        def dep():
            con = self.connect()
            try:
                yield con
            finally:
                con.close()
        return dep

    @property
    def session_dependency(self):
        def dep():
            session = self.Session()
            try:
                yield session
            finally:
                session.close()
        return dep

    def connect(self):
        return self._engine.connect()

    @property
    def connection_string(self):
        s = [f"{p}={getattr(self,p)}" for p in ("host","port","user","dbname","sslmode")]
        if self.password:
            s += f"password={self.password}"

        return " ".join(s)

    def _creator_factory(self):
        def creator():
            return psycopg2.connect(self.connection_string)

        return creator

class BaseDatabaseLayer(DatabaseLayer):
    """
    BaseDatabaseLayer
    =================

    DatabaseLayer that allows cached access to both reflected metadata and the
    subsequent computed join network through the properties .metadata and
    .join_network.

    """
    _join_network = None

    @property
    def join_network(self):
        if self._join_network is None:
            self._join_network = views_query_planning.join_network(self.metadata.tables)
        return self._join_network

    @property
    def tables(self):
        return self.metadata.tables
