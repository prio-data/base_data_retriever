
import sqlalchemy as sa
from sqlalchemy.orm import declarative_base
import views_schema

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

    def pydantic(self)-> views_schema.LevelOfAnalysis:
        return views_schema.LevelOfAnalysis(
                name        = self.name,
                description = self.description,
                time_index  = self.time_index,
                unit_index  = self.unit_index)

    @classmethod
    def from_pydantic(cls, model: views_schema.LevelOfAnalysis):
        return cls(
                name        = model.name,
                description = model.description,
                time_index  = model.time_index,
                unit_index  = model.unit_index
            )
