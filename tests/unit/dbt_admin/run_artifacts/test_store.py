"""Unit tests for ArtifactStore (in-memory DuckDB artifact store).

Two levels of coverage:
- Direct: _insert_rows with hand-crafted tuples for focused behavior tests.
- Integration: minimal raw artifact dicts through the full load_artifact pipeline.
  The parser's _to_dict fallback returns raw dicts unchanged when strict validation
  fails, so extractors process them via .get() with safe defaults — no mocking needed.
"""

from collections.abc import Iterator
from unittest.mock import MagicMock, patch

import pytest

from dbt_mcp.dbt_admin.run_artifacts.artifacts.parsers import ArtifactType
from dbt_mcp.dbt_admin.run_artifacts.store import MAX_RESULT_ROWS, ArtifactStore
from dbt_mcp.dbt_admin.run_artifacts.tables import (
    INVOCATIONS,
    NODE_COLUMNS,
    RUN_RESULTS,
)
from dbt_mcp.errors.artifact_search import (
    ArtifactNotLoadedError,
    ArtifactQueryError,
    ArtifactValidationError,
)

# ── Row-tuple helpers ──────────────────────────────────────────────────────
# _insert_rows expects (id, col1, ...) — run_id is injected as col 2 by the store.


def _invocation(idx: int = 0) -> tuple:
    # id, invocation_id, command, selector, dbt_version, generated_at, elapsed_time, args, node_count
    return (
        idx,
        f"inv-{idx}",
        "run",
        None,
        "1.9.0",
        "2024-01-01T00:00:00",
        float(idx),
        None,
        0,
    )


def _run_result(idx: int, message: str = "ok") -> tuple:
    # id, unique_id, invocation_id, status, execution_time, thread_id, message, relation_name, adapter_response, timing
    return (
        idx,
        f"model.pkg.node_{idx}",
        "inv-0",
        "pass",
        1.0,
        "thread-1",
        message,
        None,
        None,
        None,
    )


def _node_column(idx: int, unique_id: str = "model.pkg.x", col: str = "col_a") -> tuple:
    # id, unique_id, column_name, column_index, declared_type, catalog_type,
    # data_type, description, tags, meta, tests, catalog_comment
    return (idx, unique_id, col, None, None, None, None, None, None, None, None, None)


# ── Fixtures ───────────────────────────────────────────────────────────────


@pytest.fixture
def store() -> Iterator[ArtifactStore]:
    s = ArtifactStore()
    yield s
    s.close()


@pytest.fixture
def loaded_store(store: ArtifactStore) -> ArtifactStore:
    """Store with one invocations row pre-loaded (run_id=1)."""
    store._ensure_tables_created()
    store._insert_rows(INVOCATIONS, [_invocation(0)], run_id=1)
    store._loaded_tables.add("invocations")
    return store


# ── Reset ──────────────────────────────────────────────────────────────────


class TestReset:
    def test_clears_loaded_state_and_returns_row_counts(
        self, loaded_store: ArtifactStore
    ) -> None:
        dropped = loaded_store.reset()
        assert not loaded_store.is_loaded
        assert not loaded_store._tables_created
        assert dropped["invocations"] == 1

    def test_tables_can_be_reloaded_after_reset(
        self, loaded_store: ArtifactStore
    ) -> None:
        loaded_store.reset()
        loaded_store._ensure_tables_created()
        loaded_store._insert_rows(INVOCATIONS, [_invocation(0)], run_id=2)
        loaded_store._loaded_tables.add("invocations")
        rows = loaded_store.query("SELECT run_id FROM invocations")
        assert rows == [{"run_id": 2}]


# ── Table introspection ────────────────────────────────────────────────────


class TestTableIntrospection:
    def test_list_tables_distinguishes_loaded_from_not_loaded(
        self, loaded_store: ArtifactStore
    ) -> None:
        by_name = {t["table_name"]: t for t in loaded_store.list_tables()}
        assert by_name["invocations"]["status"] == "loaded"
        assert by_name["nodes"]["status"] == "not_loaded"

    def test_describe_unknown_table_raises(self, loaded_store: ArtifactStore) -> None:
        with pytest.raises(ArtifactNotLoadedError, match="ghost_table"):
            loaded_store.describe_table("ghost_table")


# ── Query ──────────────────────────────────────────────────────────────────


class TestQuery:
    def test_mutating_keywords_are_blocked(self, store: ArtifactStore) -> None:
        for keyword in ("INSERT", "UPDATE", "DELETE", "DROP", "CREATE"):
            with pytest.raises(ArtifactQueryError, match="Blocked keyword"):
                store.query(f"{keyword} something")

    def test_multi_statement_query_blocked(self, store: ArtifactStore) -> None:
        with pytest.raises(ArtifactQueryError, match="Multi-statement"):
            store.query("SELECT 1; DROP TABLE nodes")

    def test_comment_bypass_blocked(self, store: ArtifactStore) -> None:
        with pytest.raises(ArtifactQueryError, match="Multi-statement"):
            store.query("SELECT 1;/**/DROP TABLE nodes")

    def test_trailing_semicolon_is_allowed(self, store: ArtifactStore) -> None:
        store._ensure_tables_created()
        result = store.query("SELECT 1 AS n;")
        assert result == [{"n": 1}]

    def test_invalid_sql_raises(self, store: ArtifactStore) -> None:
        with pytest.raises(ArtifactQueryError, match="Query failed"):
            store.query("SELECT * FROM table_that_does_not_exist_xyz")

    def test_row_cap_at_max_result_rows(self, store: ArtifactStore) -> None:
        store._ensure_tables_created()
        store._insert_rows(
            INVOCATIONS,
            [_invocation(i) for i in range(MAX_RESULT_ROWS + 100)],
            run_id=1,
        )
        assert len(store.query("SELECT id FROM invocations")) == MAX_RESULT_ROWS


# ── Search ─────────────────────────────────────────────────────────────────


class TestSearch:
    def test_unknown_table_raises(self, loaded_store: ArtifactStore) -> None:
        with pytest.raises(ArtifactNotLoadedError):
            loaded_store.search(table_name="ghost_table", query_text="foo")

    def test_table_without_fts_columns_raises(self, store: ArtifactStore) -> None:
        store._ensure_tables_created()
        store._loaded_tables.add("edges")  # edges has no fts_columns
        with pytest.raises(
            ArtifactQueryError, match="does not support full-text search"
        ):
            store.search(table_name="edges", query_text="anything")

    def test_bm25_returns_matching_rows_with_score(self, store: ArtifactStore) -> None:
        store._ensure_tables_created()
        store._insert_rows(
            RUN_RESULTS, [_run_result(0, message="compilation failure")], run_id=1
        )
        store._loaded_tables.add("run_results")
        store._build_indexes(RUN_RESULTS)

        results = store.search(table_name="run_results", query_text="compilation")
        assert len(results) == 1
        assert results[0]["message"] == "compilation failure"
        assert "fts_score" in results[0]


# ── Load artifact ──────────────────────────────────────────────────────────


class TestLoadArtifact:
    def test_parse_failure_raises_validation_error(self, store: ArtifactStore) -> None:
        with patch(
            "dbt_mcp.dbt_admin.run_artifacts.store.ARTIFACT_PARSERS"
        ) as mock_parsers:
            mock_parsers.__getitem__.return_value = MagicMock(
                side_effect=ValueError("bad schema")
            )
            with pytest.raises(ArtifactValidationError, match="Parsing failed"):
                store.load_artifact(
                    run_id=1, artifact_type=ArtifactType.RUN_RESULTS, raw_data={}
                )

    def test_deferred_indexing_and_build_all(self, store: ArtifactStore) -> None:
        fake_rows = [_invocation(0)]
        with (
            patch(
                "dbt_mcp.dbt_admin.run_artifacts.store.ARTIFACT_PARSERS"
            ) as mock_parsers,
            patch(
                "dbt_mcp.dbt_admin.run_artifacts.store.ARTIFACT_EXTRACTORS"
            ) as mock_extractors,
        ):
            mock_parsers.__getitem__.return_value = MagicMock(return_value={})
            mock_extractors.__getitem__.return_value = MagicMock(
                return_value={"invocations": fake_rows}
            )
            store.load_artifact(
                run_id=1,
                artifact_type=ArtifactType.RUN_RESULTS,
                raw_data={},
                reindex=False,
            )

        assert "invocations" in store._pending_index_tables
        store.build_all_indexes()
        assert not store._pending_index_tables


# ── Merge node columns ─────────────────────────────────────────────────────


class TestMergeNodeColumns:
    def _seed(self, store: ArtifactStore, run_id: int = 1) -> None:
        store._ensure_tables_created()
        store._insert_rows(NODE_COLUMNS, [_node_column(0, col="amount")], run_id=run_id)

    def test_updates_existing_row_with_catalog_data(self, store: ArtifactStore) -> None:
        self._seed(store)
        store._merge_node_columns(
            [("model.pkg.x", "amount", 0, "INTEGER", "dollar amount")], run_id=1
        )

        row = store.conn.execute(
            "SELECT catalog_type, catalog_comment FROM node_columns WHERE column_name = 'amount'"
        ).fetchone()
        assert row == ("INTEGER", "dollar amount")

    def test_inserts_catalog_only_column(self, store: ArtifactStore) -> None:
        self._seed(store)
        store._merge_node_columns(
            [("model.pkg.x", "revenue", 1, "FLOAT", None)], run_id=1
        )

        row = store.conn.execute(
            "SELECT catalog_type FROM node_columns WHERE column_name = 'revenue'"
        ).fetchone()
        assert row == ("FLOAT",)

    def test_scoped_to_run_id(self, store: ArtifactStore) -> None:
        self._seed(store, run_id=1)
        # Merge against run_id=2 — run_id=1 row must stay untouched
        store._merge_node_columns(
            [("model.pkg.x", "amount", 0, "TEXT", "overwritten")], run_id=2
        )

        row = store.conn.execute(
            "SELECT catalog_type FROM node_columns WHERE column_name = 'amount' AND run_id = 1"
        ).fetchone()
        assert row is not None
        assert row[0] is None


# ── Full pipeline integration (no mocking) ────────────────────────────────
#
# Minimal raw dicts trigger the parser's fallback path (strict validation fails,
# raw dict returned as-is) so extractors run against real — just sparse — data.

_RUN_RESULTS_RAW: dict = {
    "metadata": {
        "invocation_id": "inv-rr-001",
        "dbt_version": "1.9.0",
        "generated_at": "2024-01-01T00:00:00Z",
    },
    "args": {"which": "run", "select": "my_model"},
    "elapsed_time": 3.5,
    "results": [
        {
            "unique_id": "model.pkg.my_model",
            "status": "success",
            "execution_time": 1.2,
            "thread_id": "thread-1",
            "message": "1 of 1 OK",
        }
    ],
}

_MANIFEST_RAW: dict = {
    "metadata": {
        "invocation_id": "inv-manifest-001",
        "dbt_version": "1.9.0",
        "generated_at": "2024-01-01T00:00:00Z",
    },
    "nodes": {
        "model.pkg.my_model": {
            "unique_id": "model.pkg.my_model",
            "name": "my_model",
            "resource_type": "model",
            "package_name": "pkg",
            "description": "A test model for artifact loading",
            "columns": {
                "id": {
                    "name": "id",
                    "description": "Primary key",
                    "data_type": "INTEGER",
                },
                "name": {"name": "name", "description": "User name"},
            },
            "depends_on": {"nodes": [], "macros": []},
        }
    },
    "sources": {},
    "exposures": {},
    "metrics": {},
    "groups": {},
    "macros": {},
}

_CATALOG_RAW: dict = {
    "metadata": {"generated_at": "2024-01-01T00:00:00Z"},
    "nodes": {
        "model.pkg.my_model": {
            "metadata": {
                "type": "table",
                "database": "my_db",
                "schema": "public",
                "name": "my_model",
            },
            "stats": {},
            "columns": {
                "id": {"name": "id", "index": 1, "type": "INTEGER", "comment": "PK"},
                "name": {"name": "name", "index": 2, "type": "VARCHAR"},
            },
        }
    },
    "sources": {},
}

_SOURCES_RAW: dict = {
    "metadata": {
        "invocation_id": "inv-src-001",
        "dbt_version": "1.9.0",
        "generated_at": "2024-01-01T00:00:00Z",
    },
    "elapsed_time": 1.0,
    "results": [
        {
            "unique_id": "source.pkg.raw.orders",
            "status": "pass",
            "execution_time": 0.5,
            "thread_id": "thread-1",
            "max_loaded_at": "2024-01-01T00:00:00Z",
            "snapshotted_at": "2024-01-01T01:00:00Z",
            "criteria": {
                "warn_after": {"count": 24, "period": "hour"},
                "error_after": {},
            },
        }
    ],
}


class TestLoadArtifactIntegration:
    def test_run_results_loads_invocation_and_results(
        self, store: ArtifactStore
    ) -> None:
        store.load_artifact(
            run_id=1, artifact_type=ArtifactType.RUN_RESULTS, raw_data=_RUN_RESULTS_RAW
        )

        invocations = store.query("SELECT invocation_id, command FROM invocations")
        assert invocations == [{"invocation_id": "inv-rr-001", "command": "run"}]

        results = store.query("SELECT unique_id, status FROM run_results")
        assert results == [{"unique_id": "model.pkg.my_model", "status": "success"}]

    def test_manifest_loads_nodes_and_columns(self, store: ArtifactStore) -> None:
        store.load_artifact(
            run_id=1, artifact_type=ArtifactType.MANIFEST, raw_data=_MANIFEST_RAW
        )

        nodes = store.query("SELECT unique_id, description FROM nodes")
        assert nodes[0]["description"] == "A test model for artifact loading"

        columns = store.query(
            "SELECT column_name FROM node_columns ORDER BY column_name"
        )
        assert [r["column_name"] for r in columns] == ["id", "name"]

    def test_sources_loads_freshness_rows(self, store: ArtifactStore) -> None:
        store.load_artifact(
            run_id=1, artifact_type=ArtifactType.SOURCES, raw_data=_SOURCES_RAW
        )

        rows = store.query(
            "SELECT unique_id, status, warn_after_count FROM source_freshness"
        )
        assert rows == [
            {
                "unique_id": "source.pkg.raw.orders",
                "status": "pass",
                "warn_after_count": 24,
            }
        ]

    def test_catalog_after_manifest_merges_catalog_types(
        self, store: ArtifactStore
    ) -> None:
        store.load_artifact(
            run_id=1, artifact_type=ArtifactType.MANIFEST, raw_data=_MANIFEST_RAW
        )
        assert (
            store.query(
                "SELECT catalog_type FROM node_columns WHERE column_name = 'id'"
            )[0]["catalog_type"]
            is None
        )

        store.load_artifact(
            run_id=1, artifact_type=ArtifactType.CATALOG, raw_data=_CATALOG_RAW
        )
        assert (
            store.query(
                "SELECT catalog_type FROM node_columns WHERE column_name = 'id'"
            )[0]["catalog_type"]
            == "INTEGER"
        )

    def test_fts_search_works_after_load(self, store: ArtifactStore) -> None:
        store.load_artifact(
            run_id=1, artifact_type=ArtifactType.MANIFEST, raw_data=_MANIFEST_RAW
        )
        results = store.search(table_name="nodes", query_text="test model")
        assert len(results) >= 1
        assert results[0]["unique_id"] == "model.pkg.my_model"

    def test_multiple_artifact_types_coexist(self, store: ArtifactStore) -> None:
        store.load_artifact(
            run_id=1, artifact_type=ArtifactType.RUN_RESULTS, raw_data=_RUN_RESULTS_RAW
        )
        store.load_artifact(
            run_id=1, artifact_type=ArtifactType.MANIFEST, raw_data=_MANIFEST_RAW
        )
        store.load_artifact(
            run_id=1, artifact_type=ArtifactType.SOURCES, raw_data=_SOURCES_RAW
        )

        loaded = {
            t["table_name"] for t in store.list_tables() if t["status"] == "loaded"
        }
        assert {
            "invocations",
            "run_results",
            "nodes",
            "node_columns",
            "source_freshness",
        }.issubset(loaded)
