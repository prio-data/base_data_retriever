import logging
from collections import namedtuple,defaultdict

from itertools import chain
from functools import reduce
from operator import add
import unittest
from sqlalchemy import MetaData,Table,Column,ForeignKey,Integer
from sqlalchemy.sql.functions import Function
from query_planning import compose_join,join_network,join_direction,query_with_ops
from networkx.algorithms.shortest_paths import shortest_path
from networkx.exception import NetworkXNoPath

Statement = namedtuple("sql_statement",("type","args","kwargs"))

class MockQuery:
    def __init__(self):
        self.statements = []

    def select_from(self,*args,**kwargs):
        self.statements.append(Statement("from",args,kwargs))
        return self

    def add_columns(self,*args,**kwargs):
        self.statements.append(Statement("select",args,kwargs))
        return self

    def join(self,*args,**kwargs):
        self.statements.append(Statement("join",args,kwargs))
        return self

    def group_by(self,*args,**kwargs):
        self.statements.append(Statement("group_by",args,kwargs))
        return self

    def __str__(self):
        return (f"Query with the following ops:\n"
                f"{self.statements}"
            )

class QueryPlannerTest(unittest.TestCase):
    def test_join_network(self):
        """
        Tests various joins by path analysis
        """
        md = MetaData()
        table_one = Table("one",md,Column("pk",Integer,primary_key=True))
        table_two = Table("two",md,
                Column("pk",Integer,primary_key=True),
                Column("one_fk",Integer,ForeignKey("one.pk")),
            )
        
        network = join_network(md.tables)

        self.assertListEqual(
                [tbl.name for tbl in shortest_path(network,table_two,table_one)],
                ["two","one"]
            )

        def fails():
            shortest_path(network,table_one,table_two)

        self.assertRaises(NetworkXNoPath,fails)

        table_three = Table("three",md,
                Column("pk",Integer,primary_key=True),
                Column("two_fk",Integer,ForeignKey("two.pk"))
                )
        table_four = Table("four",md,
                Column("pk",Integer,primary_key=True),
                Column("three_fk",Integer,ForeignKey("three.pk"))
                )

        self.assertListEqual(
                [tbl.name for tbl in shortest_path(join_network(md.tables),table_four,table_one)],
                ["four","three","two","one"]
            )

        table_five = Table("five",md,
                Column("pk",Integer,primary_key=True),
                Column("one_fk",Integer,ForeignKey("one.pk"))
                )
        table_six = Table("six",md,
                Column("pk",Integer,primary_key=True),
                Column("five_fk",Integer,ForeignKey("five.pk"))
                )
        
        self.assertListEqual(
                [tbl.name for tbl in shortest_path(join_network(md.tables),table_six,table_one)],
                ["six","five","one"]
            )

    def test_query_comp(self):
        """
        Performs a query composition and checks which operations were performed on a mock object.
        """
        md = MetaData()

        table_one = Table("one",md,
                Column("pk",Integer,primary_key=True),
                Column("value",Integer)
            )

        table_two = Table("two",md,
                Column("pk",Integer,primary_key=True),
                Column("one_fk",Integer,ForeignKey("one.pk"))
            )

        mock_query = query_with_ops(MockQuery(),
                compose_join,join_network(md.tables),"two","one","value",[("two","pk")])

        statement_names = [stmt.type for stmt in mock_query.statements]
        counts = defaultdict(int)
        for name in statement_names:
            counts[name] += 1
        self.assertDictEqual(dict(counts),{"select":2,"from":1,"join":1})

        selects = [stmt for stmt in mock_query.statements if stmt.type == "select"]
        selected = {stmt.args[0].name for stmt in selects}
        self.assertEqual(selected,{"two_pk","one_value"})

        self.assertEqual(
                [stmt for stmt in mock_query.statements if stmt.type == "join"][0].args[0].name,
                "one"
            )

        self.assertEqual(
                [stmt for stmt in mock_query.statements if stmt.type == "from"][0].args[0].name,
                "two"
            )

        # Aggregations get a groupby and function statement
        mock_query = query_with_ops(MockQuery(),
                compose_join,join_network(md.tables),"one","two","pk",[("one","pk")])
        self.assertIn("group_by",[stmt.type for stmt in mock_query.statements])
        selects = [stmt for stmt in mock_query.statements if stmt.type == "select"]
        select_argtypes = [type(a) for a in reduce(add,[stmt.args for stmt in selects])]
        self.assertIn(Function,select_argtypes)
