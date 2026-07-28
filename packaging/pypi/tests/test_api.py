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

import os
import tempfile
import unittest
from unittest import mock

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


class WorkspaceTests(unittest.TestCase):
    """The workspace the engine is told to use, which is where state lives.

    The pipeline JSON is written to a temp dir before the runner is invoked. The
    runner resolves an unset --workspace to the pipeline file's parent, so
    without an explicit flag every run pointed its watermarks, connections and
    ${workspace} at that temp dir. Incremental loads reported ok and silently
    reloaded everything, because the watermark they had written was gone.
    """

    def test_invoke_passes_the_workspace(self):
        captured = {}

        def fake_run(argv, **kwargs):
            captured["argv"] = argv

            class Result:
                returncode = 0
                stdout = ""
                stderr = ""

            return Result()

        p = duckle.read_csv("in.csv").write_csv("out.csv")
        p.workspace = os.path.join("some", "workspace")
        with mock.patch("subprocess.run", fake_run), \
             mock.patch("duckle.__main__._binary_path", lambda: "duckle-runner"), \
             mock.patch("duckle.__main__._engine_env", dict):
            p.run(quiet=True)

        argv = captured["argv"]
        self.assertIn("--workspace", argv)
        self.assertEqual(argv[argv.index("--workspace") + 1], p.workspace)

    def test_workspace_defaults_to_the_working_directory(self):
        # Not the temp dir the pipeline JSON is written to.
        self.assertEqual(duckle.Pipeline().workspace, os.getcwd())

    def test_from_json_adopts_the_owning_workspace(self):
        # A studio workspace keeps pipelines one level down, so resolving from
        # the file's own parent would land on <workspace>/pipelines.
        root = tempfile.mkdtemp(prefix="duckle-ws-")
        os.makedirs(os.path.join(root, "pipelines"))
        with open(os.path.join(root, "duckle.json"), "w") as fh:
            fh.write("{}")
        path = os.path.join(root, "pipelines", "p.json")
        duckle.read_csv("in.csv").write_csv("out.csv").save(path)

        self.assertEqual(
            os.path.realpath(duckle.from_json(path).workspace), os.path.realpath(root)
        )

    def test_explicit_workspace_wins(self):
        self.assertEqual(duckle.Pipeline(workspace="/tmp/ws").workspace, "/tmp/ws")


if __name__ == "__main__":
    unittest.main()
