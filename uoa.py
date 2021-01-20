"""
Resources for defining UOA Views in the database.

UOA Views are not constrained in terms of their definition. This is
because the SQL required to define useful UOA views is quite involved.

"""
from typing import List,Optional

from contextlib import closing

from fastapi import Request
from fastapi.responses import JSONResponse 

from pydantic import BaseModel

from sqlalchemy import MetaData,Table,inspect
from sqlalchemy.sql import text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import ProgrammingError
from sqlalchemy_views import CreateView,DropView

from db import engine,Session
from util import hyperlink
from env import env

meta = MetaData(schema=env("UOA_SCHEMA"))

class Uoa(BaseModel):
    name: str
    definition: Optional[str]
    link: Optional[str]

def uoa_post(r:Request,uoa:Uoa)->JSONResponse:
    view = CreateView(Table(uoa.name,meta),text(uoa.definition))

    with closing(Session()) as sess:
        try:
            sess.execute(view)
        except ProgrammingError as e:
            return JSONResponse({"error":str(e)},status_code=400)
    return JSONResponse({"a":hyperlink(r,"uoa",uoa.name)}) 

def uoa_detail(r:Request,name:str)->Uoa:
    definition = inspect(engine).get_view_definition(name,schema=env("UOA_SCHEMA"))
    return Uoa(
        name=name,
        definition=definition,
        link = hyperlink(r,"uoa",name)
    )

def uoa_list(r:Request)->List[Uoa]:
    defined_views = inspect(engine).get_view_names(schema=env("UOA_SCHEMA"))
    return [Uoa(name=v,link=hyperlink(r,"uoa",v)) for v in defined_views]
