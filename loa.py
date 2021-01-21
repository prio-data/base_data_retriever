import os
import re
import pickle

from contextlib import closing

from typing import Optional,Tuple,List
from datetime import date

import requests
import pydantic
from fastapi import Request,Response

import sqlalchemy as sa

import networkx as nx
from networkx.algorithms.simple_paths import shortest_simple_paths as ssp

from calls import cast_date_to_mid
from db import engine,Session
from env import env

import pandas as pd

staging_meta = sa.MetaData(schema="staging")
inspector = sa.inspect(engine)

def reflect_uoa_base_tables():
    """
    Retrieves reflected base tables
    Costly!
    """

    names = inspector.get_table_names(schema=env("STAGING_SCHEMA")) 
    tables = {} 
    for n in names:
        tables[n] = (sa.Table(n,staging_meta,autoload_with=engine))
    return tables

def join_network(tables:List[sa.Table])->nx.Graph:
    g = nx.DiGraph()
    
    def get_fk_tables(fk):
        return [tbl for tbl in tables if fk.references(tbl)]

    for table in tables:
        for fk in table.foreign_keys:
            try:
                ref_table = get_fk_tables(fk)[-1]
            except IndexError:
                continue 
            g.add_edge(
                    table,
                    ref_table,
                    reference=fk.parent,
                    referent=fk.column
                )
    return g

class Query(pydantic.BaseModel):
    columns: List[str]

def priogrid_month_query(r:Request,
        query:Query,
        start_date:Optional[date]=None,
        end_date:Optional[date]=None,
        bb_ul:Tuple[int,int]=None,
        bb_br:Tuple[int,int]=None):
    """
    Returns data @ the PGM level
    """

    tables = reflect_uoa_base_tables()
    network = join_network(list(tables.values())) 
    with open("/tmp/nw.pckl","wb") as f:
        pickle.dump(network,f)

    columns = set(query.columns).union({
        "priogrid_month.priogrid_gid",
        "year.year",
        "month.month",
        "month.id",
        })

    for c in columns:
        try:
            assert re.search(r"[a-zA-Z_0-9]+\.[a-zA-Z_0-9]+",c)
        except AssertionError:
            return Response("Columns must be in format {table}.{name}, not: "+c)

    column_references = []
    unique_columns = set()
    for c in columns: 
        table,column = c.split(".")
        if column in unique_columns:
            return Response("All column names must be unique",status_code=400)
        unique_columns = unique_columns.union({column})
        try:
            column_table = tables[table]
        except KeyError:
            return Response(f"Table {table} not found",status_code=404)
        try:
            column_ref = column_table.c[column]
        except KeyError:
            return Response(f"Column {column} not found in table {table}",status_code=404)
        
        column_references.append(column_ref)

    base = tables["priogrid_month"]
    #base = tables[loa]

    joinsto = {cr.table for cr in column_references if cr.table is not base}
    joinpaths = []

    for tbl in joinsto:
        joinpaths.append(next(ssp(network,base,tbl)))

    column_references = [cr.label(cr.name) for cr in column_references]

    with closing(Session()) as sess:
        q = sess.query(*column_references).select_from(base)

        hasjoined = set()
        for path in joinpaths:
            prev = base
            for tbl in path[1:]:
                if tbl not in hasjoined:
                    con = network[prev][tbl]
                    q = q.join(tbl,con["reference"]==con["referent"])
                    hasjoined = hasjoined.union({tbl})
                else:
                    pass
                prev = tbl

        start_date = date.fromisoformat("1989-01-01") 
        end_date = date.fromisoformat("1990-01-01")

        if start_date is not None:
            startmid = cast_date_to_mid(start_date)
            q = q.filter(tables["month"].c.id >= startmid)
        if end_date is not None:
            endmid = cast_date_to_mid(end_date)
            q = q.filter(tables["month"].c.id <= endmid)

        data = pd.DataFrame(q.all())
    return data.to_csv(index=False)

def variable_query(r:Request,loa:str,var:str):
    """
    Returns data for a single variable @ a LOA
    Nasty metaprogramming, effective results

    Query params are passed to each query function
    """
    try:
        fn = globals()[f"{loa}_query"] # Nicht gut
    except KeyError:
        return Response(status_code=404)
    return fn(r=r,query=Query(columns=[loa+"."+var]),**r.query_params)
