"""DuckDB table definitions for dbt artifacts."""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class TableConfig:
    """Schema definition for a DuckDB table."""

    table_name: str
    table_ddl: str
    fts_columns: list[str] = field(default_factory=list)
    index_columns: list[str] = field(default_factory=list)


NODES = TableConfig(
    table_name="nodes",
    table_ddl="""
        CREATE TABLE nodes (
            id INTEGER PRIMARY KEY,
            run_id INTEGER NOT NULL,
            unique_id VARCHAR NOT NULL,
            name VARCHAR,
            resource_type VARCHAR,
            package_name VARCHAR,
            file_path VARCHAR,
            original_file_path VARCHAR,
            fqn TEXT,
            alias VARCHAR,
            checksum VARCHAR,
            description TEXT,
            node_language VARCHAR,
            raw_code TEXT,
            database_name VARCHAR,
            schema_name VARCHAR,
            relation_name VARCHAR,
            identifier VARCHAR,
            enabled BOOLEAN,
            materialized VARCHAR,
            incremental_strategy VARCHAR,
            on_schema_change VARCHAR,
            unique_key TEXT,
            full_refresh BOOLEAN,
            config TEXT,
            access_level VARCHAR,
            group_name VARCHAR,
            contract_enforced BOOLEAN,
            version VARCHAR,
            latest_version VARCHAR,
            deprecation_date VARCHAR,
            constraints TEXT,
            tags TEXT,
            meta TEXT,
            source_name VARCHAR,
            source_description TEXT,
            loader VARCHAR,
            loaded_at_field VARCHAR,
            freshness TEXT,
            compiled_code TEXT,
            compiled_path VARCHAR,
            extra_ctes TEXT,
            patch_path VARCHAR,
            docs_show BOOLEAN,
            quoting TEXT,
            depends_on_nodes TEXT,
            depends_on_macros TEXT
        )
    """,
    fts_columns=["name", "description", "raw_code", "compiled_code"],
    index_columns=["unique_id", "name", "resource_type", "package_name"],
)

NODE_COLUMNS = TableConfig(
    table_name="node_columns",
    table_ddl="""
        CREATE TABLE node_columns (
            id INTEGER PRIMARY KEY,
            run_id INTEGER NOT NULL,
            unique_id VARCHAR NOT NULL,
            column_name VARCHAR NOT NULL,
            column_index INTEGER,
            declared_type VARCHAR,
            catalog_type VARCHAR,
            data_type VARCHAR,
            description TEXT,
            tags TEXT,
            meta TEXT,
            tests TEXT,
            catalog_comment TEXT,
            UNIQUE (run_id, unique_id, column_name)
        )
    """,
    fts_columns=[],
    index_columns=["unique_id"],
)

EDGES = TableConfig(
    table_name="edges",
    table_ddl="""
        CREATE TABLE edges (
            id INTEGER PRIMARY KEY,
            run_id INTEGER NOT NULL,
            parent_unique_id VARCHAR NOT NULL,
            child_unique_id VARCHAR NOT NULL,
            edge_type VARCHAR
        )
    """,
    index_columns=["parent_unique_id", "child_unique_id"],
)

TEST_METADATA = TableConfig(
    table_name="test_metadata",
    table_ddl="""
        CREATE TABLE test_metadata (
            id INTEGER PRIMARY KEY,
            run_id INTEGER NOT NULL,
            unique_id VARCHAR NOT NULL,
            test_name VARCHAR,
            test_namespace VARCHAR,
            kwargs TEXT,
            column_name VARCHAR,
            attached_node VARCHAR,
            severity VARCHAR,
            warn_if VARCHAR,
            error_if VARCHAR,
            fail_calc VARCHAR,
            store_failures BOOLEAN,
            store_failures_as VARCHAR
        )
    """,
    index_columns=["unique_id", "test_name", "attached_node"],
)

EXPOSURES = TableConfig(
    table_name="exposures",
    table_ddl="""
        CREATE TABLE exposures (
            id INTEGER PRIMARY KEY,
            run_id INTEGER NOT NULL,
            unique_id VARCHAR NOT NULL,
            name VARCHAR,
            exposure_type VARCHAR,
            label VARCHAR,
            owner_name VARCHAR,
            owner_email VARCHAR,
            url VARCHAR,
            maturity VARCHAR,
            description TEXT,
            package_name VARCHAR,
            file_path VARCHAR,
            original_file_path VARCHAR,
            fqn TEXT,
            depends_on_nodes TEXT,
            depends_on_macros TEXT,
            tags TEXT,
            meta TEXT,
            config TEXT
        )
    """,
    fts_columns=["name", "description"],
    index_columns=["unique_id", "name"],
)

METRICS = TableConfig(
    table_name="metrics",
    table_ddl="""
        CREATE TABLE metrics (
            id INTEGER PRIMARY KEY,
            run_id INTEGER NOT NULL,
            unique_id VARCHAR NOT NULL,
            name VARCHAR,
            label VARCHAR,
            metric_type VARCHAR,
            description TEXT,
            package_name VARCHAR,
            file_path VARCHAR,
            original_file_path VARCHAR,
            fqn TEXT,
            type_params TEXT,
            time_granularity VARCHAR,
            semantic_model_name VARCHAR,
            depends_on_nodes TEXT,
            depends_on_macros TEXT,
            group_name VARCHAR,
            tags TEXT,
            meta TEXT,
            config TEXT
        )
    """,
    fts_columns=["name", "label", "description"],
    index_columns=["unique_id", "name"],
)

GROUPS = TableConfig(
    table_name="groups",
    table_ddl="""
        CREATE TABLE groups (
            id INTEGER PRIMARY KEY,
            run_id INTEGER NOT NULL,
            unique_id VARCHAR NOT NULL,
            name VARCHAR,
            description TEXT,
            package_name VARCHAR,
            file_path VARCHAR,
            original_file_path VARCHAR,
            owner_name VARCHAR,
            owner_email VARCHAR
        )
    """,
    index_columns=["unique_id", "name"],
)

MACROS = TableConfig(
    table_name="macros",
    table_ddl="""
        CREATE TABLE macros (
            id INTEGER PRIMARY KEY,
            run_id INTEGER NOT NULL,
            unique_id VARCHAR NOT NULL,
            name VARCHAR,
            package_name VARCHAR,
            file_path VARCHAR,
            original_file_path VARCHAR,
            macro_sql TEXT,
            description TEXT,
            depends_on_macros TEXT,
            arguments TEXT,
            meta TEXT
        )
    """,
    fts_columns=["name", "description", "macro_sql"],
    index_columns=["unique_id", "name", "package_name"],
)

CATALOG_TABLES = TableConfig(
    table_name="catalog_tables",
    table_ddl="""
        CREATE TABLE catalog_tables (
            id INTEGER PRIMARY KEY,
            run_id INTEGER NOT NULL,
            unique_id VARCHAR NOT NULL,
            table_type VARCHAR,
            database_name VARCHAR,
            schema_name VARCHAR,
            table_name VARCHAR,
            table_owner VARCHAR,
            table_comment TEXT
        )
    """,
    fts_columns=["table_name", "table_comment"],
    index_columns=["unique_id", "table_name"],
)

CATALOG_STATS = TableConfig(
    table_name="catalog_stats",
    table_ddl="""
        CREATE TABLE catalog_stats (
            id INTEGER PRIMARY KEY,
            run_id INTEGER NOT NULL,
            unique_id VARCHAR NOT NULL,
            stat_id VARCHAR,
            stat_label VARCHAR,
            stat_value VARCHAR,
            description TEXT,
            include_in_stats BOOLEAN
        )
    """,
    index_columns=["unique_id"],
)

INVOCATIONS = TableConfig(
    table_name="invocations",
    table_ddl="""
        CREATE TABLE invocations (
            id INTEGER PRIMARY KEY,
            run_id INTEGER NOT NULL,
            invocation_id VARCHAR NOT NULL,
            command VARCHAR,
            selector VARCHAR,
            dbt_version VARCHAR,
            generated_at VARCHAR,
            elapsed_time FLOAT,
            args TEXT,
            node_count INTEGER
        )
    """,
    index_columns=["invocation_id"],
)

RUN_RESULTS = TableConfig(
    table_name="run_results",
    table_ddl="""
        CREATE TABLE run_results (
            id INTEGER PRIMARY KEY,
            run_id INTEGER NOT NULL,
            unique_id VARCHAR NOT NULL,
            invocation_id VARCHAR,
            status VARCHAR,
            execution_time FLOAT,
            thread_id VARCHAR,
            message TEXT,
            relation_name VARCHAR,
            adapter_response TEXT,
            timing TEXT
        )
    """,
    fts_columns=["status", "message"],
    index_columns=["unique_id", "invocation_id", "status"],
)

SOURCE_FRESHNESS = TableConfig(
    table_name="source_freshness",
    table_ddl="""
        CREATE TABLE source_freshness (
            id INTEGER PRIMARY KEY,
            run_id INTEGER NOT NULL,
            unique_id VARCHAR NOT NULL,
            source_name VARCHAR,
            table_name VARCHAR,
            invocation_id VARCHAR,
            status VARCHAR,
            max_loaded_at VARCHAR,
            snapshotted_at VARCHAR,
            max_loaded_at_time_ago FLOAT,
            execution_time FLOAT,
            thread_id VARCHAR,
            error TEXT,
            warn_after_count INTEGER,
            warn_after_period VARCHAR,
            error_after_count INTEGER,
            error_after_period VARCHAR,
            adapter_response TEXT,
            timing TEXT
        )
    """,
    fts_columns=["source_name", "table_name", "status"],
    index_columns=["unique_id", "source_name", "status"],
)

# All table configs, keyed by table name
TABLES: dict[str, TableConfig] = {
    c.table_name: c
    for c in [
        NODES,
        NODE_COLUMNS,
        EDGES,
        TEST_METADATA,
        EXPOSURES,
        METRICS,
        GROUPS,
        MACROS,
        CATALOG_TABLES,
        CATALOG_STATS,
        INVOCATIONS,
        RUN_RESULTS,
        SOURCE_FRESHNESS,
    ]
}
