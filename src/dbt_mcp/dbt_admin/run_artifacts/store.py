"""In-memory DuckDB artifact store.

Artifacts are loaded via `load_artifact()` from parsed dicts
fetched by the Admin API client.
"""

import logging
import time
from typing import Any

import duckdb

from dbt_mcp.dbt_admin.run_artifacts.artifacts.parsers import (
    ArtifactType,
    ARTIFACT_PARSERS,
)
from dbt_mcp.dbt_admin.run_artifacts.extractors import ARTIFACT_EXTRACTORS
from dbt_mcp.dbt_admin.run_artifacts.tables import TABLES, TableConfig
from dbt_mcp.errors.artifact_search import (
    ArtifactNotLoadedError,
    ArtifactQueryError,
    ArtifactValidationError,
)

logger = logging.getLogger(__name__)

READONLY_BLOCKED = frozenset(
    {
        "INSERT",
        "UPDATE",
        "DELETE",
        "DROP",
        "CREATE",
        "ALTER",
        "TRUNCATE",
        "REPLACE",
        "MERGE",
        "UPSERT",
        "COPY",
        "LOAD",
        "INSTALL",
        "ATTACH",
        "DETACH",
        "EXPORT",
        "IMPORT",
        "VACUUM",
        "PRAGMA",
    }
)

MAX_RESULT_ROWS = 500


class ArtifactStore:
    """Manages an in-memory DuckDB database loaded with dbt artifacts."""

    def __init__(self) -> None:
        self.conn = duckdb.connect()
        self.conn.execute("INSTALL fts;")
        self.conn.execute("LOAD fts;")
        self._loaded_tables: set[str] = set()
        self._tables_created: bool = False
        self._pending_index_tables: set[str] = set()

    # Methods for loading

    def load_artifact(
        self,
        run_id: int,
        artifact_type: ArtifactType,
        raw_data: dict[str, Any],
        *,
        reindex: bool = True,
    ) -> dict[str, Any]:
        """Validate, extract, and load a single artifact into DuckDB.

        Assumes the store has already been cleared by the caller (load_artifacts
        calls reset() before the first artifact). Returns
        ``{"tables": {table: row_count}, "timing": {phase: ms}}``.
        """
        t_wall = time.perf_counter()
        self._ensure_tables_created()

        # Phase 1: parsing (dbt-artifacts-parser handles schema version dispatch)
        t = time.perf_counter()
        try:
            parsed = ARTIFACT_PARSERS[artifact_type](raw_data)
        except Exception as e:
            raise ArtifactValidationError(
                f"Parsing failed for {artifact_type.value}: {e}"
            ) from e
        validate_ms = round((time.perf_counter() - t) * 1000)

        # Phase 2: row extraction
        extractor = ARTIFACT_EXTRACTORS[artifact_type]
        t = time.perf_counter()
        tables_data = extractor(parsed)
        extract_ms = round((time.perf_counter() - t) * 1000)

        # Phase 3: DuckDB inserts
        counts: dict[str, int] = {}
        affected_tables: set[str] = set()
        t = time.perf_counter()

        for table_name, rows in tables_data.items():
            if not rows:
                continue

            # Special: catalog column merge into node_columns
            if table_name == "_node_columns_update":
                self._merge_node_columns(rows, run_id)
                affected_tables.add("node_columns")
                row = self.conn.execute(
                    "SELECT COUNT(*) FROM node_columns WHERE run_id = ?", [run_id]
                ).fetchone()
                counts["node_columns"] = row[0] if row else 0
                continue

            config = TABLES[table_name]
            self._insert_rows(config, rows, run_id)
            counts[table_name] = len(rows)
            self._loaded_tables.add(table_name)
            affected_tables.add(table_name)
            logger.info(f"Loaded {len(rows)} rows into {table_name}")

        insert_ms = round((time.perf_counter() - t) * 1000)

        # Phase 4: index building (deferred or immediate)
        t = time.perf_counter()
        if reindex:
            for table_name in affected_tables:
                if table_name in TABLES:
                    self._build_indexes(TABLES[table_name])
        else:
            self._pending_index_tables |= affected_tables
        index_ms = round((time.perf_counter() - t) * 1000)

        total_ms = round((time.perf_counter() - t_wall) * 1000)

        return {
            "tables": counts,
            "timing": {
                "validate_ms": validate_ms,
                "extract_ms": extract_ms,
                "insert_ms": insert_ms,
                "index_ms": index_ms,
                "total_ms": total_ms,
            },
        }

    def build_all_indexes(self) -> list[str]:
        """Build indexes for all tables pending index construction.

        Call this once after loading multiple artifacts with ``reindex=False``.
        Returns the list of table names that were indexed.
        """
        to_index = set(self._pending_index_tables)
        indexed = []
        for table_name in to_index:
            if table_name in TABLES:
                self._build_indexes(TABLES[table_name])
                indexed.append(table_name)
        self._pending_index_tables.clear()
        return indexed

    def reset(self) -> dict[str, int]:
        """Drop all tables and reset the store to empty state.

        Returns ``{table_name: rows_dropped}`` for the caller's log.
        The FTS extension stays loaded. Call ``load_artifact`` to repopulate.
        """
        dropped: dict[str, int] = {}
        if self._tables_created:
            for table_name in TABLES:
                row = self.conn.execute(
                    f'SELECT COUNT(*) FROM "{table_name}"'
                ).fetchone()
                dropped[table_name] = row[0] if row else 0
                self.conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        self._loaded_tables.clear()
        self._pending_index_tables.clear()
        self._tables_created = False
        return dropped

    def _ensure_tables_created(self) -> None:
        """Lazily create all tables on first load."""
        if self._tables_created:
            return
        for config in TABLES.values():
            self.conn.execute(f'DROP TABLE IF EXISTS "{config.table_name}";')
            self.conn.execute(config.table_ddl)
        self._tables_created = True

    def _insert_rows(self, config: TableConfig, rows: list[tuple], run_id: int) -> None:
        """Bulk insert rows into a table, injecting run_id as the second column."""
        if not rows:
            return
        # rows are (id, col1, col2, ...) — insert as (id, run_id, col1, col2, ...)
        tagged = [(row[0], run_id, *row[1:]) for row in rows]
        placeholders = ", ".join(["?"] * len(tagged[0]))
        self.conn.executemany(
            f"INSERT INTO {config.table_name} VALUES ({placeholders})", tagged
        )

    def _merge_node_columns(self, rows: list[tuple], run_id: int) -> None:
        """Merge catalog column data into node_columns via batch update.

        Each row is (unique_id, column_name, column_index, catalog_type, catalog_comment).
        Uses a temp table + batch UPDATE/INSERT for performance.
        Only touches rows with the given run_id.
        """
        if not rows:
            return

        self.conn.execute("""
            CREATE OR REPLACE TEMP TABLE _catalog_cols (
                unique_id VARCHAR, column_name VARCHAR,
                column_index INTEGER, catalog_type VARCHAR,
                catalog_comment TEXT
            )
        """)
        try:
            placeholders = ", ".join(["?"] * 5)
            self.conn.executemany(
                f"INSERT INTO _catalog_cols VALUES ({placeholders})", rows
            )

            self.conn.execute(
                """
                UPDATE node_columns SET
                    column_index = COALESCE(cc.column_index, node_columns.column_index),
                    catalog_type = COALESCE(cc.catalog_type, node_columns.catalog_type),
                    data_type = COALESCE(cc.catalog_type, node_columns.data_type),
                    catalog_comment = COALESCE(cc.catalog_comment, node_columns.catalog_comment)
                FROM _catalog_cols cc
                WHERE node_columns.unique_id = cc.unique_id
                  AND node_columns.column_name = cc.column_name
                  AND node_columns.run_id = ?
            """,
                [run_id],
            )

            row = self.conn.execute(
                "SELECT COALESCE(MAX(id), -1) + 1 FROM node_columns"
            ).fetchone()
            next_id = row[0] if row else 0
            self.conn.execute(
                """
                INSERT INTO node_columns (id, run_id, unique_id, column_name, column_index,
                    catalog_type, data_type, catalog_comment)
                SELECT ? + row_number() OVER () - 1,
                       ?,
                       cc.unique_id, cc.column_name, cc.column_index,
                       cc.catalog_type, cc.catalog_type, cc.catalog_comment
                FROM _catalog_cols cc
                WHERE NOT EXISTS (
                    SELECT 1 FROM node_columns nc
                    WHERE nc.unique_id = cc.unique_id
                      AND nc.column_name = cc.column_name
                      AND nc.run_id = ?
                )
            """,
                [next_id, run_id, run_id],
            )
        finally:
            self.conn.execute("DROP TABLE IF EXISTS _catalog_cols")
        self._loaded_tables.add("node_columns")
        logger.info(f"Merged {len(rows)} catalog columns into node_columns")

    def _build_indexes(self, config: TableConfig) -> None:
        """Build FTS and B-tree indexes for a table."""
        if config.fts_columns:
            fts_cols = ", ".join(f"'{c}'" for c in config.fts_columns)
            self.conn.execute(
                f"PRAGMA create_fts_index('{config.table_name}', 'id', "
                f"{fts_cols}, overwrite=1);"
            )

        for col in config.index_columns:
            idx_name = f"idx_{config.table_name[:4]}_{col}"
            self.conn.execute(
                f"CREATE INDEX IF NOT EXISTS {idx_name} "
                f'ON {config.table_name}("{col}");'
            )

    # Methods to query

    @property
    def is_loaded(self) -> bool:
        """Whether any artifact tables have been loaded."""
        return bool(self._loaded_tables)

    def list_tables(self) -> list[dict[str, Any]]:
        """List all loaded artifact tables with row counts."""
        if not self._tables_created:
            return []

        result = self.conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main'"
        ).fetchall()

        tables = []
        for (table_name,) in result:
            if table_name.startswith("fts_"):
                continue
            count_row = self.conn.execute(
                f'SELECT COUNT(*) FROM "{table_name}"'
            ).fetchone()
            row_count = count_row[0] if count_row else 0
            tables.append(
                {
                    "table_name": table_name,
                    "row_count": row_count,
                    "status": "loaded"
                    if table_name in self._loaded_tables
                    else "not_loaded",
                }
            )
        return tables

    def describe_table(self, table_name: str) -> list[dict[str, str]]:
        """Describe the schema of a loaded table."""
        self._validate_table_name(table_name)
        rows = self.conn.execute(f'DESCRIBE "{table_name}"').fetchall()
        return [{"column_name": row[0], "column_type": row[1]} for row in rows]

    def query(self, sql: str) -> list[dict[str, Any]]:
        """Execute a read-only SQL query. Results capped at 500 rows."""
        tokens = sql.strip().upper().split()
        for token in tokens:
            if token in READONLY_BLOCKED:
                raise ArtifactQueryError(
                    f"Only read-only queries are allowed. Blocked keyword: {token}"
                )

        try:
            result = self.conn.execute(sql)
            columns = [desc[0] for desc in result.description]
            rows = result.fetchmany(MAX_RESULT_ROWS)
            return [
                {col: _serialize(val) for col, val in zip(columns, row)} for row in rows
            ]
        except duckdb.Error as e:
            raise ArtifactQueryError(f"Query failed: {e}") from e

    def search(
        self, *, table_name: str, query_text: str, limit: int = 20
    ) -> list[dict[str, Any]]:
        """Full-text BM25 search on a loaded table."""
        self._validate_table_name(table_name)
        config = TABLES.get(table_name)
        if not config or not config.fts_columns:
            raise ArtifactQueryError(
                f"Table '{table_name}' does not support full-text search"
            )
        limit = min(limit, MAX_RESULT_ROWS)

        fts_table = f"fts_main_{table_name}"
        sql = f"""
            SELECT t.*, {fts_table}.match_bm25(t.id, ?) AS fts_score
            FROM "{table_name}" t
            WHERE {fts_table}.match_bm25(t.id, ?) IS NOT NULL
            ORDER BY fts_score DESC
            LIMIT ?
        """
        try:
            result = self.conn.execute(sql, [query_text, query_text, limit])
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()
            return [
                {col: _serialize(val) for col, val in zip(columns, row)} for row in rows
            ]
        except duckdb.Error as e:
            raise ArtifactQueryError(f"Search failed: {e}") from e

    def close(self) -> None:
        """Close the DuckDB connection."""
        if self.conn:
            self.conn.close()
            logger.info("In-memory database closed")

    # Helper

    def _validate_table_name(self, table_name: str) -> None:
        """Check table_name is a known loaded table (also prevents SQL injection)."""
        if table_name not in self._loaded_tables:
            available = ", ".join(sorted(self._loaded_tables)) or "(none)"
            raise ArtifactNotLoadedError(
                f"Unknown table '{table_name}'. Available: {available}"
            )


def _serialize(val: Any) -> Any:
    """Ensure values are JSON-serializable."""
    if val is None or isinstance(val, (str, int, float, bool)):
        return val
    return str(val)
