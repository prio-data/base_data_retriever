
import logging
from unittest import TestCase
from sqlalchemy import Table, Column, MetaData, ForeignKey, Integer
from views_query_planning import join_network
from base_data_retriever.querying import QueryComposer
from base_data_retriever.models import LevelOfAnalysis

class TestQueryComposer(TestCase):
    def setUp(self):
        metadata = MetaData()

        Table("a",metadata,
                Column("id", Integer, primary_key = True),
                Column("t", Integer),
                Column("u", Integer),
                Column("val", Integer))

        Table("b",metadata,
                Column("id", Integer, primary_key = True),
                Column("val", Integer),
                Column("fk_a", Integer, ForeignKey("a.id")),
                Column("fk_b", Integer, ForeignKey("c.id")))

        Table("c",metadata,
                Column("id", Integer, primary_key = True),
                Column("val", Integer))

        Table("d",metadata,
                Column("id", Integer, primary_key = True),
                Column("fk", Integer, ForeignKey("b.id")),
                Column("val", Integer))

        Table("e",metadata,
                Column("id", Integer, primary_key = True),
                Column("t", Integer),
                Column("u", Integer),
                Column("fk", Integer, ForeignKey("d.id")))

        self.network = join_network(metadata.tables)

    def test_forwards_expression(self):
        loa = LevelOfAnalysis(name = "e", time_index = "t", unit_index = "u")

        compose = QueryComposer(self.network, loa)

        for t in ["a","b","c","d"]:
            expr = compose.expression(t, "val")
            self.assertTrue(expr.is_right())


    def test_backwards_expression(self):
        loa = LevelOfAnalysis(name = "a", time_index = "t", unit_index = "u")

        compose = QueryComposer(self.network, loa)

        self.assertTrue(compose.expression("b","val").is_right())
        self.assertTrue(compose.expression("c","val").is_left())
        self.assertTrue(compose.expression("d","val").is_right())

    def test_fails_with_wrong_column_name(self):
        loa = LevelOfAnalysis(name = "a", time_index = "t", unit_index = "u")
        compose = QueryComposer(self.network, loa)
        self.assertTrue(compose.expression("b","val").is_right())
        self.assertTrue(compose.expression("b","foo").is_left())

    def test_aggregates(self):
        loa = LevelOfAnalysis(name = "a", time_index = "t", unit_index = "u")
        compose = QueryComposer(self.network, loa)
        expression = compose.expression("b","val")
        self.assertIn("sum", expression.value)

    def test_does_not_aggregate(self):
        loa = LevelOfAnalysis(name = "e", time_index = "t", unit_index = "u")
        compose = QueryComposer(self.network, loa)
        expression = compose.expression("d","val")
        self.assertNotIn("sum", expression.value)

    def test_custom_agg_function(self):
        loa = LevelOfAnalysis(name = "a", time_index = "t", unit_index = "u")
        compose = QueryComposer(self.network, loa)
        for fn_name in ("avg", "sum", "min", "max"):
            expression = compose.expression("b","val",aggregation_function=fn_name)
            self.assertIn(fn_name, expression.value)
    
    def test_bad_loa_def(self):
        loa = LevelOfAnalysis(name = "a", time_index = "foo", unit_index = "bar")
        compose = QueryComposer(self.network, loa)
        e = compose.expression("b","val")
        self.assertTrue(e.is_left)

    def test_bad_aggregation_function(self):
        loa = LevelOfAnalysis(name = "a", time_index = "t", unit_index = "u")
        compose = QueryComposer(self.network, loa)
        a = compose.expression("b","val",aggregation_function="foobar")
        b = compose.expression("b","val",aggregation_function="avg")
        print(a)
        print(b)
        self.assertTrue(a.is_left)
        self.assertTrue(a.is_right)

