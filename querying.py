from contextlib import closing

from typing import Optional,Tuple,List
from datetime import date

import pydantic
from fastapi import Request,Response

import sqlalchemy as sa

import networkx as nx
from networkx.algorithms.simple_paths import shortest_simple_paths as ssp

import pandas as pd

from calls import cast_date_to_mid
from db import engine,Session
from env import env


staging_meta = sa.MetaData(schema="staging")
inspector = sa.inspect(engine)

LOAS = {
    "priogrid_month":{
        "required_columns":[
                "month.id",
                "priogrid.gid",
            ]
        },
    "country_month":{
        "required_columns":[
                "month.id",
                "country.id"
            ]
        }
}

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

def join_network(tables:List[sa.Table])->nx.DiGraph:
    """
    Creates a directed graph of the FK->Referent relationships present in a
    list of tables. This graph can then be traversed to figure out how to
    perform a join when retrieving data from this collection of tables.
    """
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

column_reference = pydantic.constr(regex=r"[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+")

class Coord(pydantic.BaseModel):
    x: float
    y: float

class Query(pydantic.BaseModel):
    """
    A query for a list of columns, referenced by [table].[column].
    These are then queried from the UOA_SCHEMA namespace. 
    """
    columns: List[column_reference]
    start_date: Optional[date]=None
    end_date: Optional[date]=None

    bbox_ul: Optional[Coord]=None
    bbox_br: Optional[Coord]=None

    countries: Optional[List[int]]=None

def json_query(loa:str,query:Query):
    """
    Returns data from a relational structure by way of a simple JSON query.
    This function is basically an extension of the QP to work with our
    relational structure, making it super easy to get the data you need,
    elevating our DB service to the REST level (e.g making it possible
    to query for data over HTTP).

    The function reads the table structure from a schema which is reserved
    for UOAs. Then it constructs a digraph using the FKs of this structure,
    which can be traversed to determine which disaggregations can be made.

    This makes it possible to retrieve, for instance, country-level data
    at the Priogrid-month level of analysis.

    Aggregation is not yet supported, because it requires an additional step;
    grouping and applying aggregation functions. Since which agg. function to
    use is a significant choice, the user must be able to specify which one
    they want. Then, I think it's just a matter of reversing the digraph to
    figure out which aggregations to make and then applying the proper grouping
    constructs.
    """

    tables = reflect_uoa_base_tables()
    network = join_network(list(tables.values())) 

    try:
        required_columns = LOAS[loa]["required_columns"]
        base_table = tables[loa]
    except KeyError:
        return Response(f"LOA {loa} not found",status_code=404)

    columns = set(query.columns).union(required_columns)

    column_references = []
    """
    unique_columns = set()
    """
    for c in columns: 
        table,column = c.split(".")

        """
        if column in unique_columns:
            continue
            #return Response("All column names must be unique",status_code=400)

        unique_columns = unique_columns.union({column})
        """

        try:
            column_table = tables[table]
        except KeyError:
            return Response(f"Table {table} not found",status_code=404)

        try:
            column_ref = column_table.c[column]
        except KeyError:
            return Response(f"Column {column} not found in table {table}",status_code=404)
        
        column_references.append(column_ref)

    joinsto = {cr.table for cr in column_references if cr.table is not base_table}

    joinpaths = []
    for tbl in joinsto:
        try:
            joinpaths.append(next(ssp(network,base_table,tbl)))
        except nx.exception.NetworkXNoPath:
            return Response(
                    f"No join path found between {base_table.name} and {tbl.name}. "
                    "Aggregation is not yet implemented. Did you try to aggregate?",
                    status_code=400)

    column_references = [cr.label("_".join((cr.table.name,cr.name))) for cr in column_references]

    with closing(Session()) as sess:
        q = sess.query(*column_references).select_from(base_table)

        hasjoined = set()
        for path in joinpaths:
            prev = base_table
            for tbl in path[1:]:
                if tbl not in hasjoined:
                    con = network[prev][tbl]
                    q = q.join(tbl,con["reference"]==con["referent"])
                    hasjoined = hasjoined.union({tbl})
                else:
                    pass
                prev = tbl

        if "month.id" in columns: #Filterable by date
            start_date = date.fromisoformat("1980-01-01") 
            end_date = date.fromisoformat("1981-01-01")

            if query.start_date is not None:
                startmid = cast_date_to_mid(start_date)
                q = q.filter(tables["month"].c.id >= startmid)
            if query.end_date is not None:
                endmid = cast_date_to_mid(end_date)
                q = q.filter(tables["month"].c.id <= endmid)

        if "country.id" in columns and query.countries is not None: #Filterable by country
            q = q.filter(tables["country"].c.id.in_(query.countries))

        data = pd.DataFrame(q.all())

    # serialization

    return Response(data.to_csv(index=False))

def variable_query(r:Request,loa:str,var:str):
    """
    Query for a single variable.
    Basically just a curried version of the above function.
    """

    try:
        query = Query(columns=[var])
    except pydantic.ValidationError:
        return Response("Specify the variable as {table}.{variable}",status_code=404)

    return json_query(r=r,loa=loa,query=query,**r.query_params)
