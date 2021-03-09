from contextlib import closing
import os
import pickle
import logging
from typing import Dict
import enum
import networkx as nx
from functools import partial
from itertools import chain
from networkx.exception import NetworkXNoPath
from networkx.algorithms.shortest_paths import has_path,shortest_path
from sqlalchemy.orm import Query
import sqlalchemy as sa
from mytools import class_partial

import pandas

import exceptions
import loa

logger = logging.getLogger(__name__)

class Direction(enum.Enum):
    forwards = 1
    backwards = 2


def join_network(tables:Dict[str,sa.Table])->nx.DiGraph:
    """
    Creates a directed graph of the FK->Referent relationships present in a
    list of tables. This graph can then be traversed to figure out how to
    perform a join when retrieving data from this collection of tables.
    """
    digraph = nx.DiGraph()
    
    def get_fk_tables(fk):
        return [tbl for tbl in tables.values() if fk.references(tbl)]

    for table in tables.values():

        for fk in table.foreign_keys:
            try:
                ref_table = get_fk_tables(fk)[-1]
            except IndexError:
                print("No table found for fk: %s",str(fk))
                continue 
            digraph.add_edge(
                    table,
                    ref_table,
                    reference=fk.parent,
                    referent=fk.column
                )
    return digraph 

def join_direction(digraph,origin,destination):
    """
    Which way can the graph be traversed?
    """
    try:
        has_path(digraph,origin,destination)
        return Direction.forwards 
    except NetworkXNoPath:
        shortest_path(digraph.reverse(),origin,destination)
        return Direction.backwards


def compose_join(network,loa_name,table_name,column_name,loa_index_columns,agg_fn="avg"):
    """
    compose_join

    Creates a list of operations that can be applied to a Query object, making
    it retrieve data at a certain LOA

    :param network: A networkx graph representing a database
    :param loa_name: The name of the LOA to retrieve data at
    :param table_name: The name of the table from which to retrieve the data 
    :param column_name: The name of the column to retrieve 
    :param agg_fn: If the retrieval op. needs to aggregate data, if will do so with this function
    :returns: An iterator containing functions to be applied to a Query object 
    :raises QueryError: If table_name.column_name does not exist in the DB 
    :raises ConfigError: If the requested loa is not configured  
    """

    lookup = {tbl.name:tbl for tbl in network}
    get_col_ref = lambda tbl,name: lookup[tbl].c[name]#.label(tbl+"_"+name)

    loa_table = lookup[loa_name]

    try:
        column_ref = get_col_ref(table_name,column_name)
    except KeyError as ke:
        raise exceptions.QueryError(f"{table_name}.{column_name} not found") from ke

    index_columns_ref = []
    for idx_table_name,idx_column_name in loa_index_columns:
        try:
            index_columns_ref.append(get_col_ref(idx_table_name,idx_column_name))
        except KeyError as ke:
            raise exceptions.ConfigError(f"Index column for {loa_table}"
                    f" {idx_table_name}.{idx_column_name} not found")

    to_join = set() 
    all_columns = list(set(index_columns_ref+[column_ref]))
    aggregates = False

    for idx,c in enumerate(all_columns):
        try:
            path = shortest_path(network, loa_table, c.table)
        except NetworkXNoPath:
            aggregates = True
            path = shortest_path(network,c.table, loa_table)
            path.reverse()
            all_columns[idx] = getattr(sa.func,agg_fn)(c)

        to_join = set(path).union(to_join)
        logger.debug("->".join([tbl.name for tbl in to_join]))

    to_join = to_join.difference({loa_table})

    select = [class_partial("add_columns",col) for col in all_columns]
    join = [class_partial("join",table) for table in to_join]

    if aggregates:
        group_by = [class_partial("group_by",c) for c in index_columns_ref]
    else:
        group_by = []

    logger.debug("%s selects",len(select))
    logger.debug("%s joins",len(join))
    logger.debug("%s groupbys",len(group_by))

    return chain(select,[lambda query: query.select_from(loa_table)],join,group_by)

def query_with_ops(query,op_composer,*args,**kwargs):
    for op in op_composer(*args,**kwargs):
        query = op(query)
    return query

if __name__ == "__main__":
    """
    Proof of concept
    """

    from db import engine,Session
    from sqlalchemy import MetaData

    ma_cache = "/tmp/ma.pckl"
    if os.path.exists(ma_cache):
        with open(ma_cache,"rb") as f:
            ma = pickle.load(f)
    else:
        ma = MetaData(bind=engine,schema="prod")
        ma.reflect()
        with open(ma_cache,"wb") as f:
            pickle.dump(ma,f)

    nw = join_network(ma.tables)
    cmonth = [tbl for tbl in nw.nodes() if tbl.name=="country_month"][0]
    #ops = compose_join(join_network(ma.tables),"country_month","priogrid_month","ged_best_ns")

    with closing(Session()) as sess:
        q = query_with_ops(sess.query(),compose_join,join_network(ma.tables),"country_month","priogrid_month","ged_best_ns",loa.index_columns("country_month"))
        q = q.filter(cmonth.c.month_id>=400)
        q = q.filter(cmonth.c.month_id<=450)
        print(q.all())
