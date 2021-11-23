import logging
from typing import Deque, Tuple
from collections import deque
from sqlalchemy import Table, Column
from sqlalchemy import sql
from sqlalchemy.sql.selectable import Select
from toolz.functoolz import curry
from networkx import NetworkXNoPath
from networkx.algorithms.shortest_paths import shortest_path
from pymonad.either import Left, Right, Either
from pymonad.maybe import Maybe, Just, Nothing
from . import models

logger = logging.getLogger(__name__)

def path(forwards, network, a, b)-> Maybe[Deque[Table]]:
    if forwards:
        x,y = a,b
        post = lambda x:x
    else:
        x,y = b,a
        post = reversed

    try:
        return Just(deque(post(shortest_path(network, x, y))))
    except NetworkXNoPath:
        logger.debug(f"No path between {a} and {b} in {network}! ({'forwards' if forwards else 'backwards'})")
        return Nothing

def aggregate(aggregation_function: str, aggregate_to: Table, column: Column, select: Select)-> Select:
    aggregation_function = getattr(sql.func, aggregation_function)
    return select.add_columns(aggregation_function(column)).group_by(*set(aggregate_to.primary_key))

class QueryComposer():
    def __init__(self, network, loa):
        self.network = network
        self._tables = {tbl.name: tbl for tbl in self.network}
        self.loa: models.LevelOfAnalysis = loa

    def joins(self, join_path: Deque[Table]):
        a = join_path.popleft()
        expression = a
        for b in join_path:
            try:
                edge = self.network[a][b]
                condition = (edge["reference"] == edge["referent"])
            except KeyError:
                edge = self.network[b][a]
                condition = (edge["referent"] == edge["reference"])

            expression = sql.join(expression, b, condition)
            a = b
        return expression


    def expression(self, table: str, column: str, aggregation_function: str = "sum")-> Either[Exception, str]:
        to_select = (self._column(table,column)
                .maybe(Left(f"Column {table}.{column} doesn't exist"), Right)
                .then(lambda c: c.label(c.name)))

        index_columns = (self.index_columns
                .maybe(Left("Index columns not found."), Right)
                .then(lambda columns: (c.label(c.name) for c in columns)))

        tables = [self.loa_table, self._table(table)]

        forwards_path = Maybe.apply(curry(path, True)).to_arguments(Just(self.network), *tables).join()

        join_path, aggregates = forwards_path.maybe(
                (Maybe.apply(curry(path, False)).to_arguments(Just(self.network), *tables).join(), True),
                lambda x: (Just(x), False))

        if aggregates:
            selection_function = curry(aggregate, aggregation_function, self.loa_table.value)
        else:
            selection_function = lambda to_select, select_from: select_from.add_columns(to_select)

        joins = (join_path.maybe(Left("Failed to find path"), Right)
            .then(self.joins))

        select_from = (Either.apply(curry(lambda idx_col, joins: sql.select(*idx_col).select_from(joins)))
            .to_arguments(index_columns, joins))

        return Either.apply(curry(selection_function)).to_arguments(to_select, select_from).then(str)

    def _table(self, name)-> Maybe[Table]:
        try:
            return Just(self._tables[name])
        except KeyError:
            return Nothing

    def _column(self, table, column)-> Maybe[Column]:
        def get_key(lookup, key):
            try:
                return Just(lookup[key])
            except KeyError:
                return Nothing

        return self._table(table).then(lambda tbl: get_key(tbl.c, column))

    @property
    def loa_table(self) -> Maybe[Table]:
        return self._table(self.loa.name)

    @property
    def index_columns(self) -> Maybe[Tuple[Column, Column]]:
        time,unit = (self._column(self.loa.name, col) for col in (self.loa.time_index, self.loa.unit_index))
        if time.is_just and unit.is_just:
            return Just((time.value, unit.value))
        else:
            return Nothing
