
import sqlalchemy as sa
from sqlalchemy.orm import declarative_base
from pydantic import BaseModel

Base = declarative_base()

class LevelOfAnalysis(Base):
    """
    LevelOfAnalysis
    ===============

    A level of analysis metadata description.

    name:        str
    description: str
    time_index:  str
    unit_index:  str

    """
    __tablename__ = "level_of_analysis"

    name        = sa.Column(sa.String, primary_key = True)
    description = sa.Column(sa.String, default = None, nullable = True)
    time_index  = sa.Column(sa.String)
    unit_index  = sa.Column(sa.String)

    class PydanticModel(BaseModel):
        name:        str
        description: str
        time_index:  str
        unit_index:  str

    def pydantic(self):
        return self.PydanticModel(
                name        = self.name,
                description = self.description,
                time_index  = self.time_index,
                unit_index  = self.unit_index)

