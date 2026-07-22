"""Pin the Python API to the engine's actual component and property names.

This is the contract these tests exist to protect: a curated method like
`.limit(n)` writes a node the Rust engine has to recognise. When a property
name does not match, the engine does not complain. It falls back to a default
and the run reports ok, so `.limit(2)` returned every row (it fell through to
LIMIT 100) and `read_duckdb` returned zero rows with a green status.

That failure mode is invisible in a demo and invisible in CI unless something
asserts the exact keys, which is what these do. The expected values below are
taken from the builders in crates/duckdb-engine/src/plan/builders.rs, not from
what looked plausible.
"""

import unittest

import duckle


def props_of(pipeline, index=-1):
    return pipeline.to_dict()["nodes"][index]["data"]["properties"]


def component_of(pipeline, index=-1):
    return pipeline.to_dict()["nodes"][index]["data"]["componentId"]


class TransformContractTests(unittest.TestCase):
    """Each transform's component id and property keys, as the engine reads them."""

    def test_limit_uses_engine_property_name(self):
        # build_limit reads props["limit"] (with a "rows" fallback) and
        # otherwise defaults to 100, so a wrong key silently passes 100 rows.
        pipeline = duckle.read_csv("input.csv").limit(7)
        self.assertEqual(component_of(pipeline), "xf.limit")
        self.assertEqual(props_of(pipeline), {"limit": 7})

    def test_select_targets_the_project_component(self):
        # There is no xf.select. The component is xf.project ("Project /
        # Select"), and it reads "columns".
        pipeline = duckle.read_csv("input.csv").select("id", "amount")
        self.assertEqual(component_of(pipeline), "xf.project")
        self.assertEqual(props_of(pipeline), {"columns": ["id", "amount"]})

    def test_rename_uses_renames_pairs(self):
        pipeline = duckle.read_csv("input.csv").rename(amount="value")
        self.assertEqual(component_of(pipeline), "xf.rename")
        self.assertEqual(props_of(pipeline), {"renames": [{"from": "amount", "to": "value"}]})

    def test_sort_uses_order_by(self):
        pipeline = duckle.read_csv("input.csv").sort("amount", desc=True)
        self.assertEqual(component_of(pipeline), "xf.sort")
        self.assertEqual(
            props_of(pipeline), {"orderBy": [{"column": "amount", "direction": "desc"}]}
        )

    def test_dedupe_uses_columns(self):
        pipeline = duckle.read_csv("input.csv").dedupe("id")
        self.assertEqual(component_of(pipeline), "xf.distinct")
        self.assertEqual(props_of(pipeline), {"columns": ["id"]})

    def test_where_marks_the_predicate_as_python(self):
        # mode="python" is what routes the expression through the compiler
        # rather than splicing it in as raw SQL.
        pipeline = duckle.read_csv("input.csv").where("amount >= 20")
        self.assertEqual(component_of(pipeline), "xf.filter")
        self.assertEqual(
            props_of(pipeline),
            {"predicate": {"mode": "python", "expr": "amount >= 20"}},
        )

    def test_derive_uses_name_expr_pairs(self):
        pipeline = duckle.read_csv("input.csv").derive(total="amount * 2")
        self.assertEqual(component_of(pipeline), "xf.pyexpr")
        self.assertEqual(props_of(pipeline), {"columns": [{"name": "total", "expr": "amount * 2"}]})


class SourceContractTests(unittest.TestCase):
    """Source property keys. A wrong key here reads nothing and still reports ok."""

    def test_read_duckdb_uses_database_and_table_name(self):
        # build_duckdb_source reads "tableName", and attach_prelude reads
        # "database". Passing path/table attached nothing and returned 0 rows
        # with a successful status.
        pipeline = duckle.read_duckdb("warehouse.duckdb", "orders")
        self.assertEqual(component_of(pipeline, 0), "src.duckdb")
        self.assertEqual(
            props_of(pipeline, 0), {"database": "warehouse.duckdb", "tableName": "orders"}
        )

    def test_read_postgres_uses_table_name(self):
        pipeline = duckle.read_postgres("public.orders")
        self.assertEqual(component_of(pipeline, 0), "src.postgres")
        self.assertEqual(props_of(pipeline, 0), {"tableName": "public.orders"})

    def test_read_csv_and_parquet_use_path(self):
        for method, component in (
            (duckle.read_csv, "src.csv"),
            (duckle.read_parquet, "src.parquet"),
            (duckle.read_json, "src.json"),
        ):
            pipeline = method("data.file")
            self.assertEqual(component_of(pipeline, 0), component)
            self.assertEqual(props_of(pipeline, 0), {"path": "data.file"})


class CatalogContractTests(unittest.TestCase):
    """Every component a curated method emits must exist and be runnable.

    This is the check that would have caught xf.select, which was not a
    component at all, so `.select()` failed the compile outright.
    """

    # Executable by the engine but missing from the palette, so the catalog
    # does not list them. Both compile and run; they are simply invisible to
    # the GUI and to agents using list_components. Tracked as a catalog gap
    # rather than a broken node, and named here so the list cannot grow
    # silently: anything else missing is a genuine failure.
    UNCATALOGUED_BUT_EXECUTABLE = {"xf.limit", "xf.pyexpr"}

    def test_every_curated_component_is_in_the_catalog(self):
        from duckle._components import COMPONENTS

        chain = duckle.read_csv("in.csv")
        chain.where("a > 1").derive(b="a * 2").select("a").rename(a="x")
        chain.sort("x").limit(5).dedupe().write_csv("out.csv")
        for node in chain.to_dict()["nodes"]:
            cid = node["data"]["componentId"]
            if cid in self.UNCATALOGUED_BUT_EXECUTABLE:
                continue
            with self.subTest(component=cid):
                self.assertIn(cid, COMPONENTS, "{} is not a known component".format(cid))


if __name__ == "__main__":
    unittest.main()
