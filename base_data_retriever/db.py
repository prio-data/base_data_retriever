from typing import Optional
import psycopg2
import sqlalchemy
import views_query_planning
from sqlalchemy.orm import sessionmaker

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
            password: Optional[str] = None,
            metadata: Optional[sqlalchemy.MetaData] = None):

        self.host     = host
        self.port     = port
        self.user     = user
        self.name     = name
        self.sslmode  = sslmode
        self.password = password


        self._engine  = sqlalchemy.create_engine("postgresql+psycopg2://", creator = self._creator_factory())

        self._metadata = metadata
        if self._metadata is not None:
            self._metadata.bind(self._engine)

        self.Session  = sessionmaker(bind = self._engine)

    @property
    def metadata(self):
        if self._metadata is None:
            self._metadata = sqlalchemy.MetaData(bind = self._engine)
            self._metadata.reflect()
        return self._metadata

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
    def _constring(self):
        s = [f"{p}={getattr(self,p)}" for p in ("host","port","user","dbname","sslmode")]
        if self.password:
            s += f"password={self.password}"

        return " ".join(s)

    def _creator_factory(self):
        def creator():
            return psycopg2.connect(self._constring)

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
